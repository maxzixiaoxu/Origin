// Command jobctl drives the queue from a terminal.
//
// It exists for two reasons: to verify the end-to-end path without a browser,
// and to script the demo and chaos scenarios in the README. The Rails admin is
// the presentation surface; this is the one that can run in CI.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/jobspec"
	"github.com/maxzixiaoxu/Origin/pkg/objectstore"
)

// derivativeSizes is the shared fan-out table; see pkg/jobspec.
var derivativeSizes = jobspec.StandardSizes

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "gen-image":
		err = cmdGenImage(args)
	case "submit":
		err = cmdSubmit(args)
	case "watch":
		err = cmdWatch(args)
	case "queues":
		err = cmdQueues(args)
	case "bench":
		err = cmdBench(args)
	case "drain":
		err = cmdDrain(args)
	case "verify":
		err = cmdVerify(args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `jobctl -- drive the distributed job queue

  gen-image  -out FILE [-w N] [-h N]     write a test JPEG
  submit     -file FILE [flags]          upload an image and fan out derive jobs
  watch      -id JOB_ID [flags]          poll a job until it finishes
  queues                                 list queues with live depth
  bench      -n N [-sleep-ms MS]          submit synthetic load, recording ids
  drain      -queue Q [-timeout D]        wait until a queue empties
  verify     [-expect N]                  assert every recorded id finished

Common flags:
  -addr   queue service gRPC address (default localhost:59090)
  -s3     S3 endpoint (default http://localhost:59000)
  -bucket S3 bucket (default jobq)
`)
}

// --- shared setup ---------------------------------------------------------

func addFlags(fs *flag.FlagSet) (addr, s3, bucket, queue *string) {
	addr = fs.String("addr", envOr("QUEUE_ADDR", "localhost:59090"), "queue service gRPC address")
	s3 = fs.String("s3", envOr("S3_ENDPOINT", "http://localhost:59000"), "S3 endpoint")
	bucket = fs.String("bucket", envOr("S3_BUCKET", "jobq"), "S3 bucket")
	queue = fs.String("queue", "images", "queue name")
	return addr, s3, bucket, queue
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func dial(addr string) (queuev1.QueueServiceClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return queuev1.NewQueueServiceClient(conn), func() { _ = conn.Close() }, nil
}

func openStore(ctx context.Context, endpoint, bucket string) (*objectstore.Store, error) {
	return objectstore.New(ctx, objectstore.Config{
		Endpoint:  endpoint,
		Region:    envOr("S3_REGION", "us-east-1"),
		Bucket:    bucket,
		AccessKey: envOr("S3_ACCESS_KEY", "jobq"),
		SecretKey: envOr("S3_SECRET_KEY", "jobq-secret"),
		PathStyle: true,
	})
}

// --- gen-image ------------------------------------------------------------

// cmdGenImage writes a synthetic JPEG.
//
// Generated rather than committed so the repository carries no binary fixtures,
// and so the demo can produce an image large enough for the resize cost to be
// visible -- a 4000x3000 gradient compresses small but decodes to 48MB of
// pixels, which is what makes the handler genuinely CPU-bound.
func cmdGenImage(args []string) error {
	fs := flag.NewFlagSet("gen-image", flag.ExitOnError)
	out := fs.String("out", "sample.jpg", "output path")
	width := fs.Int("w", 4000, "width")
	height := fs.Int("h", 3000, "height")
	quality := fs.Int("q", 90, "jpeg quality")
	if err := fs.Parse(args); err != nil {
		return err
	}

	img := image.NewRGBA(image.Rect(0, 0, *width, *height))

	// Concentric colour bands: cheap to generate, and visibly wrong if a resize
	// ever produces a corrupt or mis-sized output, which a flat fill would hide.
	cx, cy := float64(*width)/2, float64(*height)/2
	maxDist := math.Hypot(cx, cy)

	for y := 0; y < *height; y++ {
		for x := 0; x < *width; x++ {
			d := math.Hypot(float64(x)-cx, float64(y)-cy) / maxDist
			img.Set(x, y, color.RGBA{
				R: uint8(255 * d),
				G: uint8(255 * (1 - d)),
				B: uint8(128 + 127*math.Sin(d*12)),
				A: 255,
			})
		}
	}

	f, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("create %s: %w", *out, err)
	}
	defer func() { _ = f.Close() }()

	if err := jpeg.Encode(f, img, &jpeg.Options{Quality: *quality}); err != nil {
		return fmt.Errorf("encode jpeg: %w", err)
	}

	info, _ := f.Stat()
	fmt.Printf("wrote %s  %dx%d  %.1f KB\n", *out, *width, *height, float64(info.Size())/1024)
	return nil
}

// --- submit ---------------------------------------------------------------

func cmdSubmit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	addr, s3, bucket, queue := addFlags(fs)
	file := fs.String("file", "", "image file to upload (required)")
	wait := fs.Bool("wait", true, "poll until all derivatives finish")
	corrupt := fs.Bool("corrupt", false, "upload non-image bytes to demonstrate permanent failure")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" && !*corrupt {
		return fmt.Errorf("-file is required (or use -corrupt)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	store, err := openStore(ctx, *s3, *bucket)
	if err != nil {
		return err
	}

	// --- upload the original ---
	var (
		data       []byte
		sourceName string
	)
	if *corrupt {
		// The permanent-failure demo: a text file with an image extension.
		// Decoding fails, the handler classifies it as permanent, and it lands
		// in the dead-letter queue on the first attempt without burning retries.
		data = []byte("this is definitely not an image\n")
		sourceName = "corrupt.jpg"
	} else {
		if data, err = os.ReadFile(*file); err != nil {
			return fmt.Errorf("read %s: %w", *file, err)
		}
		sourceName = filepath.Base(*file)
	}

	stamp := time.Now().UTC().Format("20060102T150405.000")
	sourceKey := fmt.Sprintf("originals/%s-%s", stamp, sourceName)

	if err := store.Put(ctx, sourceKey, bytes.NewReader(data), "image/jpeg"); err != nil {
		return fmt.Errorf("upload original: %w", err)
	}
	fmt.Printf("uploaded  %s  (%.1f KB)\n\n", sourceKey, float64(len(data))/1024)

	// --- fan out one job per size ---
	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	type submitted struct {
		id    string
		label string
	}
	var jobs []submitted

	for _, size := range derivativeSizes {
		payload, err := json.Marshal(jobspec.DerivePayload{
			SourceKey: sourceKey,
			TargetKey: fmt.Sprintf("derivatives/%s-%s-%s.jpg", stamp,
				strings.TrimSuffix(sourceName, filepath.Ext(sourceName)), size.Label),
			Width:  size.Width,
			Label:  size.Label,
			Format: "jpeg",
		})
		if err != nil {
			return err
		}

		priority := int32(size.Priority)
		res, err := rpc.Enqueue(ctx, &queuev1.EnqueueRequest{
			Queue:    *queue,
			Type:     "image.derive",
			Payload:  payload,
			Priority: &priority,
		})
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", size.Label, err)
		}

		fmt.Printf("enqueued  %-7s width=%-5d priority=%d  %s\n",
			size.Label, size.Width, size.Priority, res.GetId())
		jobs = append(jobs, submitted{id: res.GetId(), label: size.Label})
	}

	if !*wait {
		return nil
	}

	fmt.Println()
	start := time.Now()
	for _, j := range jobs {
		if err := pollJob(ctx, rpc, j.id, 2*time.Minute, start); err != nil {
			return err
		}
	}
	return nil
}

// --- watch ----------------------------------------------------------------

func cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	addr, _, _, _ := addFlags(fs)
	id := fs.String("id", "", "job id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	return pollJob(ctx, rpc, *id, 2*time.Minute, time.Now())
}

// pollJob waits for a job to reach a terminal state and prints the outcome.
func pollJob(
	ctx context.Context,
	rpc queuev1.QueueServiceClient,
	jobID string,
	timeout time.Duration,
	start time.Time,
) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		res, err := rpc.GetJob(ctx, &queuev1.GetJobRequest{
			JobId:           jobID,
			IncludeAttempts: true,
		})
		if err != nil {
			return fmt.Errorf("get job %s: %w", jobID, err)
		}

		job := res.GetJob()
		switch job.GetStatus() {
		case queuev1.JobStatus_JOB_STATUS_SUCCEEDED:
			printSuccess(job, res.GetAttempts(), start)
			return nil

		case queuev1.JobStatus_JOB_STATUS_DEAD:
			fmt.Printf("DEAD      %s\n          %s\n", jobID, job.GetLastError())
			printAttempts(res.GetAttempts())
			return nil

		case queuev1.JobStatus_JOB_STATUS_CANCELLED:
			fmt.Printf("CANCELLED %s\n", jobID)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}

	return fmt.Errorf("job %s did not finish within %s", jobID, timeout)
}

func printSuccess(job *queuev1.Job, attempts []*queuev1.Attempt, start time.Time) {
	var r jobspec.DeriveResult
	if err := json.Unmarshal(job.GetResult(), &r); err != nil {
		fmt.Printf("OK        %s  (unreadable result)\n", job.GetId())
		return
	}

	elapsed := time.Since(start).Milliseconds()
	if fin := job.GetFinishedAt(); fin != nil {
		elapsed = fin.AsTime().Sub(start).Milliseconds()
	}

	fmt.Printf("OK        %-7s %dx%d -> %dx%d  %5.1f KB  handler=%dms  end-to-end=%dms\n",
		r.Label, r.SourceW, r.SourceH, r.Width, r.Height,
		float64(r.Bytes)/1024, r.TotalMS, elapsed)
	fmt.Printf("          decode=%dms resize=%dms encode=%dms upload=%dms  worker=%s\n",
		r.DecodeMS, r.ResizeMS, r.EncodeMS, r.UploadMS, r.ProcessedBy)

	if len(attempts) > 1 {
		printAttempts(attempts)
	}
}

func printAttempts(attempts []*queuev1.Attempt) {
	for _, a := range attempts {
		fmt.Printf("          attempt %d  %-14s %-24s %s\n",
			a.GetAttempt(),
			strings.TrimPrefix(a.GetOutcome().String(), "OUTCOME_"),
			a.GetWorkerId(),
			truncate(a.GetError(), 60))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// --- queues ---------------------------------------------------------------

func cmdQueues(args []string) error {
	fs := flag.NewFlagSet("queues", flag.ExitOnError)
	addr, _, _, _ := addFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rpc, closeConn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer closeConn()

	cfgs, err := rpc.ListQueues(ctx, &queuev1.ListQueuesRequest{})
	if err != nil {
		return err
	}
	depths, err := rpc.QueueDepth(ctx, &queuev1.QueueDepthRequest{})
	if err != nil {
		return err
	}

	byName := make(map[string]*queuev1.Depth, len(depths.GetDepths()))
	for _, d := range depths.GetDepths() {
		byName[d.GetQueue()] = d
	}

	fmt.Printf("%-14s %7s %9s %8s %6s %6s %8s\n",
		"QUEUE", "READY", "SCHEDULED", "RUNNING", "CONC", "RETRY", "PAUSED")
	for _, q := range cfgs.GetQueues() {
		d := byName[q.GetName()]
		fmt.Printf("%-14s %7d %9d %8d %6d %6d %8v\n",
			q.GetName(), d.GetReady(), d.GetScheduled(), d.GetRunning(),
			q.GetMaxConcurrency(), q.GetMaxAttempts(), q.GetPaused())
	}
	return nil
}
