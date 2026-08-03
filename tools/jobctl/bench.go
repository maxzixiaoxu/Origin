package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
)

// Subcommands used by the chaos scripts.
//
// They deliberately track submitted job ids in a file rather than inferring
// success from queue depth. Depth reaching zero only proves the queue is empty,
// which is also what happens if jobs are silently dropped -- precisely the
// failure the crash test is meant to detect. Checking each id individually is
// the difference between "the queue drained" and "no work was lost".

const defaultIDFile = "/tmp/jobq-bench-ids.txt"

// --- bench: submit synthetic load -----------------------------------------

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	addr, _, _, queue := addFlags(fs)
	count := fs.Int("n", 100, "number of jobs")
	sleepMS := fs.Int("sleep-ms", 0, "handler sleep duration; 0 uses bench.noop")
	async := fs.Bool("async", true, "return immediately instead of waiting")
	idFile := fs.String("id-file", defaultIDFile, "where to record submitted job ids")
	concurrency := fs.Int("c", 16, "concurrent submitters")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	jobType := "bench.noop"
	var payload []byte
	if *sleepMS > 0 {
		jobType = "bench.sleep"
		if payload, err = json.Marshal(map[string]int{"ms": *sleepMS}); err != nil {
			return err
		}
	}

	start := time.Now()

	var (
		mu   sync.Mutex
		ids  = make([]string, 0, *count)
		errs int
		wg   sync.WaitGroup
	)

	work := make(chan int, *count)
	for i := 0; i < *count; i++ {
		work <- i
	}
	close(work)

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				res, err := rpc.Enqueue(ctx, &queuev1.EnqueueRequest{
					Queue:   *queue,
					Type:    jobType,
					Payload: payload,
				})
				mu.Lock()
				if err != nil {
					errs++
				} else {
					ids = append(ids, res.GetId())
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("submitted %d %s jobs in %s (%.0f/sec), %d errors\n",
		len(ids), jobType, elapsed.Round(time.Millisecond),
		float64(len(ids))/elapsed.Seconds(), errs)

	if err := writeIDs(*idFile, ids); err != nil {
		return err
	}
	fmt.Printf("recorded ids in %s\n", *idFile)

	if *async {
		return nil
	}
	return waitForIDs(ctx, rpc, ids, 10*time.Minute)
}

func writeIDs(path string, ids []string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	for _, id := range ids {
		if _, err := fmt.Fprintln(w, id); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readIDs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var ids []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			ids = append(ids, line)
		}
	}
	return ids, sc.Err()
}

// --- drain: wait for a queue to empty --------------------------------------

func cmdDrain(args []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	addr, _, _, queue := addFlags(fs)
	timeout := fs.Duration("timeout", 3*time.Minute, "give up after this long")
	quiet := fs.Bool("quiet", false, "suppress progress output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+time.Minute)
	defer cancel()

	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	deadline := time.Now().Add(*timeout)
	start := time.Now()
	lastPrint := time.Time{}

	for time.Now().Before(deadline) {
		res, err := rpc.QueueDepth(ctx, &queuev1.QueueDepthRequest{
			Queues: []string{*queue},
		})
		if err != nil {
			return err
		}

		var ready, scheduled, running int64
		for _, d := range res.GetDepths() {
			ready += d.GetReady()
			scheduled += d.GetScheduled()
			running += d.GetRunning()
		}
		total := ready + scheduled + running

		if total == 0 {
			if !*quiet {
				fmt.Printf("queue %s drained in %s\n",
					*queue, time.Since(start).Round(time.Millisecond))
			}
			return nil
		}

		if !*quiet && time.Since(lastPrint) > time.Second {
			fmt.Printf("  ready=%-6d scheduled=%-6d running=%-4d  (%s elapsed)\n",
				ready, scheduled, running, time.Since(start).Round(time.Second))
			lastPrint = time.Now()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}

	return fmt.Errorf("queue %s did not drain within %s", *queue, *timeout)
}

// --- verify: assert every submitted job reached a terminal state -----------

// cmdVerify is the assertion behind the "zero job loss" claim.
//
// It checks each submitted id individually rather than trusting a count,
// because the failure being tested for -- a job vanishing when its worker died
// -- looks exactly like success from an aggregate view: the queue is empty
// either way.
func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	addr, _, _, _ := addFlags(fs)
	idFile := fs.String("id-file", defaultIDFile, "file of submitted job ids")
	expect := fs.Int("expect", 0, "expected job count; 0 uses the file length")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	ids, err := readIDs(*idFile)
	if err != nil {
		return err
	}
	if *expect > 0 && len(ids) != *expect {
		fmt.Printf("WARNING: expected %d ids but the file holds %d\n", *expect, len(ids))
	}

	counts := map[string]int{}
	var (
		missing    []string
		unfinished []string
		retried    int
	)

	for _, id := range ids {
		res, err := rpc.GetJob(ctx, &queuev1.GetJobRequest{
			JobId: id, IncludeAttempts: true,
		})
		if err != nil {
			missing = append(missing, id)
			continue
		}

		job := res.GetJob()
		status := strings.TrimPrefix(job.GetStatus().String(), "JOB_STATUS_")
		counts[status]++

		if !isTerminal(job.GetStatus()) {
			unfinished = append(unfinished, id)
		}
		// More than one attempt means this job survived a worker death and was
		// picked up again -- the recovery the test is looking for.
		if len(res.GetAttempts()) > 1 {
			retried++
		}
	}

	fmt.Printf("checked %d jobs\n", len(ids))
	for _, k := range sortedKeys(counts) {
		fmt.Printf("  %-12s %d\n", strings.ToLower(k), counts[k])
	}
	if retried > 0 {
		fmt.Printf("  %-12s %d  (recovered after a worker died)\n", "reattempted", retried)
	}

	if len(missing) > 0 {
		fmt.Printf("\nFAIL: %d jobs are unknown to the queue -- work was LOST\n", len(missing))
		return fmt.Errorf("%d jobs lost", len(missing))
	}
	if len(unfinished) > 0 {
		fmt.Printf("\nFAIL: %d jobs never reached a terminal state\n", len(unfinished))
		return fmt.Errorf("%d jobs unfinished", len(unfinished))
	}

	fmt.Printf("\nPASS: every submitted job reached a terminal state. Zero loss.\n")
	return nil
}

func isTerminal(s queuev1.JobStatus) bool {
	switch s {
	case queuev1.JobStatus_JOB_STATUS_SUCCEEDED,
		queuev1.JobStatus_JOB_STATUS_DEAD,
		queuev1.JobStatus_JOB_STATUS_CANCELLED:
		return true
	default:
		return false
	}
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// waitForIDs blocks until every id reaches a terminal state.
func waitForIDs(
	ctx context.Context,
	rpc queuev1.QueueServiceClient,
	ids []string,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	pending := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		pending[id] = struct{}{}
	}

	start := time.Now()
	for len(pending) > 0 && time.Now().Before(deadline) {
		for id := range pending {
			res, err := rpc.GetJob(ctx, &queuev1.GetJobRequest{JobId: id})
			if err != nil {
				continue
			}
			if isTerminal(res.GetJob().GetStatus()) {
				delete(pending, id)
			}
		}
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	if len(pending) > 0 {
		return fmt.Errorf("%d jobs did not finish within %s", len(pending), timeout)
	}
	fmt.Printf("all %d jobs finished in %s\n", len(ids), time.Since(start).Round(time.Millisecond))
	return nil
}
