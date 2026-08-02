// Package jobspec defines the payload and result shapes for each job type.
//
// These are the queue's public contract. A job payload is written by whoever
// submits the work -- Rails, jobctl, the load generator -- and read by the
// worker that runs it, so the shape cannot live inside either side's internal
// packages. Putting it here makes the contract explicit and gives the Go
// submitters compile-time checking instead of hand-built JSON.
//
// Rails constructs the same JSON by hand, so the struct tags below are the
// authoritative spelling and any change here is a breaking change for the
// admin app too.
package jobspec

// TypeImageDerive is the job type for generating an image derivative.
const TypeImageDerive = "image.derive"

// DerivePayload is the input to an image.derive job.
type DerivePayload struct {
	// SourceKey is the object key of the original image.
	SourceKey string `json:"source_key"`

	// TargetKey is where the derivative is written.
	//
	// Chosen by the submitter rather than derived by the worker, deliberately:
	// it makes the job idempotent in the practical sense. A retry overwrites
	// the same object instead of accumulating orphaned derivatives from each
	// attempt, which is what a worker-generated random key would produce.
	TargetKey string `json:"target_key"`

	// Width is the target width in pixels. Height preserves aspect ratio.
	Width int `json:"width"`

	// Label names the size for display, e.g. "thumb".
	Label string `json:"label"`

	// Format is "jpeg" or "png". Empty keeps the source format.
	Format string `json:"format,omitempty"`

	// Quality is JPEG quality 1-100. Zero uses the default.
	Quality int `json:"quality,omitempty"`
}

// DeriveResult is stored on the job and rendered in the dashboard.
//
// The per-phase timings are not decoration. They are what makes it possible to
// answer why a queue is slow: decode dominating means the inputs are large,
// upload dominating means object storage is the bottleneck, and resize
// dominating means the workers are genuinely CPU-bound and adding replicas will
// help. An aggregate duration cannot distinguish those.
type DeriveResult struct {
	Label     string `json:"label"`
	TargetKey string `json:"target_key"`

	Width  int `json:"width"`
	Height int `json:"height"`
	Bytes  int `json:"bytes"`

	Format string `json:"format"`

	SourceW int `json:"source_width"`
	SourceH int `json:"source_height"`

	DecodeMS int64 `json:"decode_ms"`
	ResizeMS int64 `json:"resize_ms"`
	EncodeMS int64 `json:"encode_ms"`
	UploadMS int64 `json:"upload_ms"`
	TotalMS  int64 `json:"total_ms"`

	// ProcessedBy identifies the worker. Shown in the dashboard so the
	// crash-recovery demo is legible: the timeline reads "lease_expired on
	// worker-2, succeeded on worker-3" rather than just "it worked eventually".
	ProcessedBy string `json:"processed_by"`
}

// DerivativeSize describes one output of the standard fan-out.
type DerivativeSize struct {
	Label string
	Width int
	// Priority is 0 (highest) to 9 (lowest).
	Priority int
}

// StandardSizes is the fan-out applied to one uploaded image.
//
// The priorities carry the demonstration. All three jobs are submitted in the
// same instant, but a user is waiting on the thumbnail and nobody is waiting on
// the archive size -- so the thumb reliably completes first, visibly, which is
// what makes priority scheduling something you can watch rather than something
// described in a README.
var StandardSizes = []DerivativeSize{
	{Label: "thumb", Width: 150, Priority: 0},
	{Label: "medium", Width: 640, Priority: 5},
	{Label: "large", Width: 1600, Priority: 9},
}
