# Temporal Workflow in Backend (Detailed Guide)

This document explains exactly how the Temporal-based workflow works in this backend.

---

## 1) High-Level Architecture

The backend uses Temporal to orchestrate a k6 test run from request intake to DB persistence.

### Components

- **HTTP API**: accepts `/run-test` requests and starts a Temporal workflow.
- **Temporal Client**: used by API layer to start workflows.
- **Temporal Worker**: runs workflow and activities from the task queue.
- **Workflow**: durable orchestration logic (`LoadTestWorkflow`).
- **Activities**: concrete side-effect operations (file IO, runner call, parsing, DB writes).
- **MongoDB**: stores run status, parsed metrics array, and raw log file content.
- **Runner Service**: executes k6 and streams output as SSE-like `data:` lines.

---

## 2) Entry Point and Server Startup

### File
- [main.go](main.go)

### What happens on startup

1. `initDB()` initializes MongoDB collection handle.
2. `InitTemporalWorker()` creates Temporal client and worker.
3. `StartTemporalWorker()` starts polling the Temporal task queue.
4. HTTP handlers are registered:
   - `/run-test` → `handleRunTestWithWorkflow`
   - `/health` → basic health endpoint
5. Process waits for termination signal, then calls `StopTemporalWorker()`.

---

## 3) Temporal Worker Setup

### File
- [worker.go](worker.go)

### Important constants

- `TemporalTaskQueue = "k6-loadtest-queue"`
- `TemporalNamespace = "default"`

### Registered workflow and activities

- Workflow:
  - `LoadTestWorkflow`
- Activities:
  - `ActivityCreateRun`
  - `ActivityCreateLogFile`
  - `ActivityCallRunner`
  - `ActivityProcessStream`
  - `ActivityWriteToLogFile`
  - `ActivityExtractMetrics`
  - `ActivitySaveMetricsToDb`
  - `ActivitySaveRunLogFile`
  - `ActivityUpdateRunStatus`

---

## 4) HTTP Request Flow (`/run-test`)

### File
- [sse.go](sse.go)

### Handler
- `handleRunTestWithWorkflow(w, r)`

### Flow

1. Validates method (`POST`) and request body.
2. Validates required fields: `script`, `vus`.
3. Ensures `run_id` (auto-generates if omitted).
4. Starts workflow by calling `StartLoadTestWorkflow(req)`.
5. Responds as event stream (`text/event-stream`) and periodically polls Temporal for workflow status.
6. Emits status events to client until workflow leaves `RUNNING`.
7. Emits final `BACKEND COMPLETED` event.

> Note: status polling is API UX only. Actual business execution is in Temporal workflow + activities.

---

## 5) Workflow Orchestration Logic

### File
- [workflow.go](workflow.go)

### Workflow
- `LoadTestWorkflow(ctx, req)`

### Activity timeout

- `StartToCloseTimeout = 30 minutes` per activity.

### Step-by-step execution

1. **Create run record**
   - Activity: `ActivityCreateRun`
   - Creates/upserts run document in Mongo with base fields.

2. **Set status to initializing**
   - Activity: `ActivityUpdateRunStatus(runID, "initializing")`

3. **Create local log file**
   - Activity: `ActivityCreateLogFile`
   - Creates `results/<sanitized_run_id>.txt`.

4. **Resolve runner URL**
   - Activity: `ActivityCallRunner`
   - Resolves URL from request or env (`RUNNER_URL`) with fallback.

5. **Process runner stream**
   - Activity: `ActivityProcessStream`
   - Sends request to runner once.
   - Reads stream lines and collects chunks (`RunID`, `Message`, `Stream`).

6. **Write stream chunks to local file**
   - Activity: `ActivityWriteToLogFile`

7. **Extract metrics from local log file**
   - Activity: `ActivityExtractMetrics`
   - Parses both k6 summary text lines and threshold lines.

8. **Save metrics array to MongoDB**
   - Activity: `ActivitySaveMetricsToDb`
   - Pushes parsed `Metric` objects into run document `metrics` array.

9. **Save raw log file content to MongoDB + delete local file**
   - Activity: `ActivitySaveRunLogFile`
   - Uses DB helper `saveRunLogFile(runID)` to:
     - read `results/<run_id>.txt`
     - set `log_file_content`
     - remove local file from disk

10. **Set final status completed**
    - Activity: `ActivityUpdateRunStatus(runID, "completed")`

If any step fails, workflow updates status to a failure-specific state like:
- `failed_log_creation`
- `failed_runner_connection`
- `failed_stream_processing`
- `failed_log_write`
- `failed_metric_extraction`
- `failed_metrics_save`
- `failed_log_save`

---

## 6) Activity Details

### File
- [activities.go](activities.go)

### `ActivityCreateRun`
- Calls `createRun(runID)` in DB layer.
- Ensures a run document exists before status/log updates.

### `ActivityCreateLogFile`
- Creates `results/` directory if needed.
- Creates empty run log file.

### `ActivityCallRunner`
- Only resolves and returns runner URL.
- Does **not** execute test request itself.

### `ActivityProcessStream`
- Sends `vus` + `script` to runner.
- Reads line-by-line from response.
- Extracts `data:` payloads.
- Classifies stream as `stdout` or `stderr`.
- Returns all collected chunks.

### `ActivityWriteToLogFile`
- Appends each chunk as:
  - `[run_id] [stream] <message>`

### `ActivityExtractMetrics`
Parses these categories:

1. **Summary metric line**
   - Pattern like `http_req_duration.....: avg=...`
2. **Threshold header + rule lines**
   - Header context: `http_req_duration`
   - Rule pattern like `✓ 'p(95)<500' p(95)=123ms`
3. **JSON metric line**
   - If message is JSON with `type == metric`

Produces `[]Metric` with:
- `RunID`, `Name`, `Value`, `Stream`, `Raw`, `CreatedAt`

### `ActivitySaveMetricsToDb`
- Iterates parsed metrics.
- Calls `saveMetric(metric)` for each metric.

### `ActivitySaveRunLogFile`
- Calls `saveRunLogFile(runID)` from DB layer.
- Persists `log_file_content` and deletes local file.

### `ActivityUpdateRunStatus`
- Calls `updateRunStatus(runID, status)`.

---

## 7) Database Write Model

### File
- [db.go](db.go)

### Collection
- Database: `loadtest`
- Collection: `metrics`

### Core helpers

- `createRun(runID)`
  - Creates/initializes run document and `metrics: []`
- `saveMetric(metric)`
  - Upserts by `run_id`
  - `$push` into `metrics` array
- `updateRunStatus(runID, status)`
  - Sets run status and timestamps
- `saveRunLogFile(runID)`
  - Reads local file
  - sets `log_file_content`
  - deletes local file

### Expected document shape (simplified)

```json
{
  "run_id": "run_...",
  "status": "completed",
  "created_at": "...",
  "start_time": "...",
  "end_time": "...",
  "updated_at": "...",
  "metrics": [
    {
      "run_id": "run_...",
      "name": "http_req_duration",
      "value": "avg=...",
      "stream": "stdout",
      "raw": "http_req_duration.....: avg=...",
      "created_at": "..."
    }
  ],
  "log_file_content": "<binary>"
}
```

---

## 8) File Lifecycle for Logs

1. Created in Step 3 (`ActivityCreateLogFile`).
2. Appended during Step 6 (`ActivityWriteToLogFile`).
3. Read + persisted in Step 9 (`ActivitySaveRunLogFile`).
4. Deleted from local disk in `saveRunLogFile` after successful DB update.

Path pattern:
- `results/<sanitizeRunID(runID)>.txt`

Sanitization helper:
- [writer.go](writer.go)
- `sanitizeRunID(runID)`

---

## 9) Environment Variables

- `BACKEND_PORT` (default `8081`)
- `MONGO_URI` (default `mongodb://localhost:27017`)
- `RUNNER_URL` (default `http://localhost:8080/run-test`)

Temporal server default connection is used by SDK client (localhost:7233 in typical dev setup).

---

## 10) How to Observe and Debug

### API-level checks

- Health:
  - `GET /health`
- Trigger run:
  - `POST /run-test`

### Temporal-level checks

- Temporal UI (usually): `http://localhost:8233`
- Check workflow execution status and activity failures.

### DB-level checks

- Verify `metrics` array is non-empty for completed runs.
- Verify `log_file_content` exists.
- Verify local log file is removed from `results/` after completion.

---

## 11) Common Failure Points

1. **Runner unreachable**
   - `failed_runner_connection`
2. **Runner stream read issue**
   - `failed_stream_processing`
3. **File write permissions**
   - `failed_log_write`
4. **Parsing no metrics**
   - file empty or unexpected output format
5. **Mongo write issue**
   - `failed_metrics_save` or `failed_log_save`

---

## 12) End-to-End Sequence (Compact)

1. Client `POST /run-test`
2. Backend starts Temporal workflow
3. Workflow creates run + updates status
4. Workflow creates local log file
5. Workflow calls runner and collects stream chunks
6. Workflow writes chunks to local log
7. Workflow parses metrics from log
8. Workflow stores metrics into Mongo `metrics[]`
9. Workflow stores raw log to Mongo and deletes local file
10. Workflow marks run `completed`
11. API stream shows completion status

---

## 13) Source Map

- Server bootstrap: [main.go](main.go)
- HTTP handler: [sse.go](sse.go)
- Temporal client/worker: [worker.go](worker.go)
- Workflow orchestration: [workflow.go](workflow.go)
- Activity implementation: [activities.go](activities.go)
- DB operations: [db.go](db.go)
- Run ID sanitization: [writer.go](writer.go)

---

## 14) Notes on Current Design

- Workflow polling from HTTP handler is synchronous to client connection; if you need async UX later, return `workflow_id` immediately and provide separate status endpoint.
- Metrics parser currently supports common k6 summary/threshold patterns and JSON metric format; extend regex if runner output format changes.
- Local log deletion happens only after DB save succeeds.

---

## 15) Quick Verification Checklist

- [ ] Temporal server running
- [ ] MongoDB running
- [ ] Runner running
- [ ] Backend started successfully
- [ ] `/run-test` returns running/completed status events
- [ ] Mongo document has non-empty `metrics` array
- [ ] Mongo document has `log_file_content`
- [ ] corresponding file removed from `results/`

---

If you want, I can also generate a sequence diagram (Mermaid) in a second markdown file for architecture reviews.