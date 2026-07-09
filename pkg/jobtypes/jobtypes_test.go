package jobtypes

import (
	"errors"
	"math"
	"sort"
	"testing"
	"time"
)

// --- priority scoring -----------------------------------------------------
//
// The ready set is a Redis sorted set and its scores are float64. Every claim
// the schema comments make about ordering rests on the packing scheme being
// exact at the magnitudes we actually produce, so these tests check the
// arithmetic rather than trusting the comment.

func TestPriorityBeatsAgeAcrossBands(t *testing.T) {
	// A high-priority job enqueued much later must still sort ahead of a
	// low-priority job enqueued long ago. If the timestamp component could ever
	// bleed into the priority component, this is what would break.
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)

	urgent := PriorityScore(PriorityHighest, recent)
	bulk := PriorityScore(PriorityLowest, old)

	if urgent >= bulk {
		t.Errorf("priority 0 enqueued late (%.0f) did not beat priority 9 enqueued early (%.0f)",
			urgent, bulk)
	}
}

func TestFIFOWithinSamePriority(t *testing.T) {
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	first := PriorityScore(PriorityDefault, base)
	second := PriorityScore(PriorityDefault, base.Add(time.Millisecond))

	if first >= second {
		t.Errorf("earlier job scored %.0f, later scored %.0f; want strictly increasing",
			first, second)
	}
}

// The precision claim in migrations/000001_init.up.sql and in the package doc:
// two jobs enqueued one millisecond apart must produce distinct scores. If
// float64 rounding collapsed them, ordering within a band would silently become
// arbitrary -- a bug that would never surface as an error, only as unfair
// scheduling under load.
func TestOneMillisecondApartStaysDistinct(t *testing.T) {
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	for _, priority := range []int{PriorityHighest, PriorityDefault, PriorityLowest} {
		a := PriorityScore(priority, base)
		b := PriorityScore(priority, base.Add(time.Millisecond))
		if a == b {
			t.Errorf("priority %d: scores collapsed at 1ms resolution (both %.0f)",
				priority, a)
		}
		if d := b - a; d != 1 {
			t.Errorf("priority %d: 1ms apart produced a delta of %v, want exactly 1",
				priority, d)
		}
	}
}

// Guards the headroom argument directly: the largest score the system can
// generate must stay inside float64's exact-integer range (2^53). Beyond that,
// integer arithmetic on scores stops being exact and the previous test's
// guarantee quietly dies.
func TestMaxScoreStaysWithinExactIntegerRange(t *testing.T) {
	// Far past any plausible operating date.
	far := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	max := PriorityScore(PriorityLowest, far)

	const exactIntegerLimit = 1 << 53 // 9007199254740992
	if max >= exactIntegerLimit {
		t.Fatalf("max score %.0f reaches float64's exact-integer limit %d", max, exactIntegerLimit)
	}

	// Confirm exactness at that magnitude rather than merely being under the
	// bound: adding 1 must still be observable.
	if max+1 == max {
		t.Errorf("score %.0f is no longer exact; +1 was absorbed by rounding", max)
	}

	t.Logf("max score %.0f uses %.4f%% of the exact-integer range",
		max, 100*max/float64(exactIntegerLimit))
}

// The timestamp component must never reach the multiplier that separates
// priority bands, or a very old job in one band would leak into the next.
func TestTimestampNeverReachesBandWidth(t *testing.T) {
	far := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	if ms := float64(far.UnixMilli()); ms >= priorityMultiplier {
		t.Fatalf("unix millis %.0f in year 2200 reaches band width %.0f; bands would overlap",
			ms, priorityMultiplier)
	}
}

func TestScoreOrderingMatchesDispatchOrder(t *testing.T) {
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	type job struct {
		name     string
		priority int
		at       time.Time
	}
	// Deliberately shuffled relative to expected dispatch order.
	jobs := []job{
		{"bulk-old", PriorityLowest, base},
		{"normal-new", PriorityDefault, base.Add(time.Hour)},
		{"urgent-new", PriorityHighest, base.Add(2 * time.Hour)},
		{"normal-old", PriorityDefault, base},
		{"urgent-old", PriorityHighest, base},
	}

	sort.Slice(jobs, func(i, j int) bool {
		return PriorityScore(jobs[i].priority, jobs[i].at) <
			PriorityScore(jobs[j].priority, jobs[j].at)
	})

	want := []string{"urgent-old", "urgent-new", "normal-old", "normal-new", "bulk-old"}
	for i, w := range want {
		if jobs[i].name != w {
			t.Errorf("dispatch position %d = %q, want %q", i, jobs[i].name, w)
		}
	}
}

func TestClampPriorityCoercesRatherThanRejects(t *testing.T) {
	// Clamping rather than erroring means a client bug degrades scheduling
	// instead of dropping the job entirely.
	cases := map[int]int{
		-100:          PriorityHighest,
		-1:            PriorityHighest,
		0:             0,
		5:             5,
		9:             9,
		10:            PriorityLowest,
		math.MaxInt32: PriorityLowest,
	}
	for in, want := range cases {
		if got := ClampPriority(in); got != want {
			t.Errorf("ClampPriority(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestPriorityScoreClampsOutOfRangeInput(t *testing.T) {
	at := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	if got, want := PriorityScore(999, at), PriorityScore(PriorityLowest, at); got != want {
		t.Errorf("out-of-range priority scored %.0f, want clamped %.0f", got, want)
	}
}

// --- error classification -------------------------------------------------

func TestIsPermanentDefaultsToRetryable(t *testing.T) {
	// An unclassified error is more likely transient than permanent, and a
	// wrongly-retried job is cheaper than a wrongly-dropped one.
	if IsPermanent(errors.New("some unclassified failure")) {
		t.Error("plain error was treated as permanent; default must be retryable")
	}
	if IsPermanent(nil) {
		t.Error("nil error was treated as permanent")
	}
}

func TestIsPermanentDetectsWrappedPermanent(t *testing.T) {
	base := Permanent(errors.New("payload is malformed"))
	if !IsPermanent(base) {
		t.Fatal("Permanent error not detected")
	}
	// Must survive the wrapping that happens as an error travels up a handler.
	wrapped := errors.Join(errors.New("context"), base)
	if !IsPermanent(wrapped) {
		t.Error("Permanent error not detected through errors.Join")
	}
}

// A handler may know better than the library it is wrapping: a dependency can
// return something it labels permanent that is actually worth retrying. An
// explicit Retryable must therefore win.
func TestExplicitRetryableOverridesPermanent(t *testing.T) {
	err := Retryable(Permanent(errors.New("upstream said fatal, we disagree")))
	if IsPermanent(err) {
		t.Error("explicit Retryable did not override the wrapped Permanent")
	}
}

func TestErrorsUnwrapToOriginal(t *testing.T) {
	sentinel := errors.New("sentinel")

	if !errors.Is(Permanent(sentinel), sentinel) {
		t.Error("Permanent broke the errors.Is chain")
	}
	if !errors.Is(Retryable(sentinel), sentinel) {
		t.Error("Retryable broke the errors.Is chain")
	}
}

func TestPermanentfFormats(t *testing.T) {
	err := Permanentf("bad field %q at index %d", "email", 3)
	if !IsPermanent(err) {
		t.Error("Permanentf did not produce a permanent error")
	}
	if got, want := err.Error(), `permanent: bad field "email" at index 3`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestNilWrappedErrorsDoNotPanic(t *testing.T) {
	if got := (&PermanentError{}).Error(); got != "permanent error" {
		t.Errorf("empty PermanentError.Error() = %q", got)
	}
	if got := (&RetryableError{}).Error(); got != "retryable error" {
		t.Errorf("empty RetryableError.Error() = %q", got)
	}
}

// --- status ---------------------------------------------------------------

func TestTerminalStatuses(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusDead, StatusCancelled}
	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("%s should be terminal", s)
		}
	}

	// 'failed' is deliberately NOT terminal: it is the transient state between
	// an attempt failing and the retry being scheduled. Treating it as terminal
	// would make the dashboard show retryable jobs as permanently broken.
	transient := []Status{StatusPending, StatusScheduled, StatusRunning, StatusFailed}
	for _, s := range transient {
		if s.Terminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestStatusValidRejectsUnknown(t *testing.T) {
	for _, s := range []Status{
		StatusPending, StatusScheduled, StatusRunning,
		StatusSucceeded, StatusFailed, StatusDead, StatusCancelled,
	} {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	for _, s := range []Status{"", "PENDING", "done", "unknown"} {
		if s.Valid() {
			t.Errorf("%q should be rejected at the API boundary", s)
		}
	}
}

// --- envelope -------------------------------------------------------------

func TestAttemptsRemaining(t *testing.T) {
	cases := []struct {
		attempt, max, want int
		final              bool
	}{
		{attempt: 0, max: 5, want: 5, final: false},
		{attempt: 4, max: 5, want: 1, final: false},
		{attempt: 5, max: 5, want: 0, final: true},
		// Defensive: an attempt counter past max must clamp at zero rather than
		// going negative, or "attempts remaining" arithmetic elsewhere inverts.
		{attempt: 9, max: 5, want: 0, final: true},
	}
	for _, c := range cases {
		e := &Envelope{Attempt: c.attempt, MaxAttempts: c.max}
		if got := e.AttemptsRemaining(); got != c.want {
			t.Errorf("attempt=%d max=%d: remaining = %d, want %d",
				c.attempt, c.max, got, c.want)
		}
		if got := e.IsFinalAttempt(); got != c.final {
			t.Errorf("attempt=%d max=%d: final = %v, want %v",
				c.attempt, c.max, got, c.final)
		}
	}
}
