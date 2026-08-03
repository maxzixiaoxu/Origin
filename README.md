# Origin Distributed Derivative Image Processor

A job queue using**Go** for the queue service and workers,
**Rails** for the admin dashboard, **Redis** for the hot path, **PostgreSQL**
for durability.

---

## What it does

Upload an image. It fans out into three jobs, submitted in the same instant
at different priorities:

| Size | Width | Priority | Why |
|---|---|---|---|
| thumb | 150px | 0 | a user is waiting on it |
| medium | 640px | 5 | needed soon |
| large | 1600px | 9 | archive |

Each job is picked up by a Go worker, which fetches the original from object
storage, decodes it, resamples it, re-encodes it, and writes the derivative
back. The dashboard shows the derivatives appearing.

---

## Architecture

```
                    ┌──────────────────────────────┐
   browser ───────► │  Rails admin (Puma)          │
                    │  upload UI + dashboard       │
                    └───────┬──────────────┬───────┘
                    writes  │              │  reads
                     (REST) │              │  (read-only ActiveRecord)
                            ▼              ▼
                    ┌───────────────┐  ┌──────────────┐
                    │ queued (Go)   │─►│  PostgreSQL  │  
                    │ gRPC + REST   │  └──────────────┘
                    │ ├ broker      │          ▲
                    │ ├ promoter    │          │
                    │ ├ reaper      │─►┌──────────────┐
                    │ ├ reconciler  │  │    Redis     │  
                    │ └ rollup      │  └──────────────┘
                    └───────┬───────┘          ▲
                     gRPC   │                  │
                            ▼                  │
                    ┌───────────────┐          │
                    │ workerd × 3   │──────────┘
                    │ pool + beat   │
                    └───────┬───────┘      ┌──────────┐
                            └─────────────►│  MinIO   │  images
                                           └──────────┘
```



