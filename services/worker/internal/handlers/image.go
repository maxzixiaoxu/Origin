package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"log/slog"
	"time"

	"golang.org/x/image/draw"

	"github.com/maxzixiaoxu/Origin/pkg/jobspec"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/pkg/objectstore"
)

// image.derive is the queue's real workload: read an original from object
// storage, resample it to a target width, and write the derivative back.
//
// Chosen over simulated work because "it sleeps for a random duration" is both
// a bad answer to "what does this system do?" and a false basis for the
// concurrency claims -- CPU saturation cannot be demonstrated by a handler that
// never uses the CPU. Decoding and resampling a multi-megapixel JPEG is
// genuinely compute-bound, so worker parallelism has a visible, measurable
// effect.
//
// It also produces the error taxonomy for free rather than through a contrived
// flag: a file that is not an image can never become one (permanent), while a
// storage timeout is worth another try (retryable).

const (
	// MaxSourceBytes bounds how large an original may be.
	//
	// Decoding is the memory hazard, not the download: a JPEG compresses
	// enormously, and a 40MB file can expand past 1GB as an RGBA bitmap. The
	// limit is applied to the encoded bytes because that is the cheap place to
	// check, with the pixel guard below covering the rest.
	MaxSourceBytes int64 = 32 << 20 // 32 MiB

	// MaxSourcePixels bounds decoded dimensions.
	//
	// This is the guard that actually prevents a decompression bomb. A ~4KB
	// crafted PNG can declare 50000x50000 dimensions, which Go will faithfully
	// try to allocate as 10GB. Checking the header before decoding pixels turns
	// that from a worker OOM -- taking down every other job on the box, all of
	// which then have to be reaped and retried -- into one failed job.
	MaxSourcePixels = 80_000_000 // ~80MP, comfortably above any real photo

	defaultJPEGQuality = 85
)

// ImageDeriver generates image derivatives.
type ImageDeriver struct {
	store    *objectstore.Store
	workerID string
	log      *slog.Logger
}

// NewImageDeriver builds the handler.
func NewImageDeriver(store *objectstore.Store, workerID string, log *slog.Logger) *ImageDeriver {
	if log == nil {
		log = slog.Default()
	}
	return &ImageDeriver{store: store, workerID: workerID, log: log}
}

// Handle generates one derivative.
func (h *ImageDeriver) Handle(ctx context.Context, job jobtypes.Envelope) ([]byte, error) {
	start := time.Now()

	var p jobspec.DerivePayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return nil, jobtypes.Permanentf("unreadable image.derive payload: %v", err)
	}
	if err := validatePayload(&p); err != nil {
		return nil, jobtypes.Permanent(err)
	}

	// --- fetch ---
	//
	// Storage failures are retryable: MinIO may be restarting, the network may
	// have blipped. A missing object is not -- the key will not appear later,
	// and retrying five times just delays the dead-letter signal.
	raw, err := h.store.GetBytes(ctx, p.SourceKey, MaxSourceBytes)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, jobtypes.Permanentf("source object %q does not exist", p.SourceKey)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, jobtypes.Retryable(fmt.Errorf("fetch source %q: %w", p.SourceKey, err))
	}

	// --- guard before decoding ---
	cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil {
		// The demo's permanent-failure case: a .txt renamed .jpg lands here and
		// goes straight to the dead-letter queue.
		return nil, jobtypes.Permanentf(
			"source %q is not a decodable image: %v", p.SourceKey, err)
	}
	if pixels := cfg.Width * cfg.Height; pixels > MaxSourcePixels {
		return nil, jobtypes.Permanentf(
			"source %q declares %dx%d (%d pixels), above the %d limit",
			p.SourceKey, cfg.Width, cfg.Height, pixels, MaxSourcePixels)
	}

	// Cancellation is checked between phases as well as inside them. Decoding
	// and resampling are uninterruptible once started, so these checkpoints are
	// what keep a cancelled or lease-lost job from running to completion.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// --- decode ---
	decodeStart := time.Now()
	src, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, jobtypes.Permanentf("decode %q: %v", p.SourceKey, err)
	}
	decodeMS := time.Since(decodeStart).Milliseconds()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// --- resample ---
	resizeStart := time.Now()
	dst := resize(src, p.Width)
	resizeMS := time.Since(resizeStart).Milliseconds()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// --- encode ---
	outFormat := p.Format
	if outFormat == "" {
		outFormat = format
	}

	encodeStart := time.Now()
	encoded, contentType, err := encode(dst, outFormat, p.Quality)
	if err != nil {
		return nil, jobtypes.Permanent(err)
	}
	encodeMS := time.Since(encodeStart).Milliseconds()

	// --- upload ---
	uploadStart := time.Now()
	if err := h.store.Put(ctx, p.TargetKey, bytes.NewReader(encoded), contentType); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, jobtypes.Retryable(fmt.Errorf("upload %q: %w", p.TargetKey, err))
	}
	uploadMS := time.Since(uploadStart).Milliseconds()

	bounds := dst.Bounds()
	result := jobspec.DeriveResult{
		Label:       p.Label,
		TargetKey:   p.TargetKey,
		Width:       bounds.Dx(),
		Height:      bounds.Dy(),
		Bytes:       len(encoded),
		Format:      outFormat,
		SourceW:     cfg.Width,
		SourceH:     cfg.Height,
		DecodeMS:    decodeMS,
		ResizeMS:    resizeMS,
		EncodeMS:    encodeMS,
		UploadMS:    uploadMS,
		TotalMS:     time.Since(start).Milliseconds(),
		ProcessedBy: h.workerID,
	}

	h.log.InfoContext(ctx, "generated image derivative",
		"label", p.Label,
		"source", fmt.Sprintf("%dx%d", cfg.Width, cfg.Height),
		"target", fmt.Sprintf("%dx%d", result.Width, result.Height),
		"bytes", result.Bytes,
		"total_ms", result.TotalMS)

	return json.Marshal(result)
}

func validatePayload(p *jobspec.DerivePayload) error {
	if p.SourceKey == "" {
		return errors.New("source_key is required")
	}
	if p.TargetKey == "" {
		return errors.New("target_key is required")
	}
	if p.Width < 1 {
		return fmt.Errorf("width must be positive, got %d", p.Width)
	}
	if p.Width > 20000 {
		return fmt.Errorf("width %d is unreasonably large", p.Width)
	}
	if p.Quality < 0 || p.Quality > 100 {
		return fmt.Errorf("quality must be 0-100, got %d", p.Quality)
	}
	switch p.Format {
	case "", "jpeg", "jpg", "png":
	default:
		return fmt.Errorf("unsupported format %q", p.Format)
	}
	return nil
}

// resize scales an image to the target width, preserving aspect ratio.
//
// Uses CatmullRom, a bicubic kernel. Nearest-neighbour and bilinear are faster
// but produce visibly worse downscales -- aliasing and mush respectively --
// which would undercut a demo whose whole point is showing the output. This is
// also what makes the handler genuinely CPU-bound: the resampling cost is real,
// not simulated.
//
// Upscaling is refused by returning the source unchanged: enlarging a thumbnail
// wastes CPU and storage to produce a blurrier image than the input.
func resize(src image.Image, targetWidth int) image.Image {
	bounds := src.Bounds()
	srcW, srcH := bounds.Dx(), bounds.Dy()

	if srcW <= 0 || srcH <= 0 || targetWidth >= srcW {
		return src
	}

	targetHeight := int(float64(srcH) * float64(targetWidth) / float64(srcW))
	if targetHeight < 1 {
		targetHeight = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, bounds, draw.Over, nil)
	return dst
}

// encode serialises an image, returning the bytes and their content type.
func encode(img image.Image, format string, quality int) ([]byte, string, error) {
	var buf bytes.Buffer

	switch format {
	case "png":
		if err := png.Encode(&buf, img); err != nil {
			return nil, "", fmt.Errorf("encode png: %w", err)
		}
		return buf.Bytes(), "image/png", nil

	case "jpeg", "jpg", "":
		q := quality
		if q <= 0 {
			q = defaultJPEGQuality
		}
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: q}); err != nil {
			return nil, "", fmt.Errorf("encode jpeg: %w", err)
		}
		return buf.Bytes(), "image/jpeg", nil

	default:
		// Reached only for a format that passed validation but has no encoder,
		// which would be a code bug rather than bad input.
		return nil, "", fmt.Errorf("no encoder for format %q", format)
	}
}
