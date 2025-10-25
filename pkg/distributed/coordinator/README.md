# Coordinator REST API

The coordinator provides a REST API for managing distributed MapReduce jobs.

## Running the Server

```bash
# Build the coordinator server
go build -o coordinator ./cmd/coordinator

# Run with default address (:8080)
./coordinator

# Or specify a custom address
COORDINATOR_ADDR=:9000 ./coordinator
```

The server includes built-in middleware for:
- **Request logging**: Logs all HTTP requests with method, path, status code, response size, and duration
- **Panic recovery**: Catches panics in handlers and returns 500 errors gracefully

Example log output:
```
2025/10/25 16:30:00 Starting coordinator API server on :8080
2025/10/25 16:30:05 [POST] /api/jobs 127.0.0.1:54321 - 201 (342 bytes) in 5.234ms
2025/10/25 16:30:10 [GET] /api/jobs/job-a1b2c3d4 127.0.0.1:54322 - 200 (1024 bytes) in 1.123ms
```

## API Endpoints

### 1. Create Job

Submit a new MapReduce job.

**Endpoint:** `POST /api/jobs`

**Request Body:**
```json
{
  "name": "word-count-20250925",
  "input": {
    "type": "s3",
    "paths": ["s3://my-bucket/input/*.txt"],
    "format": "text"
  },
  "executors": {
    "map": {
      "type": "docker",
      "uri": "myregistry/wordcount-map:latest"
    },
    "reduce": {
      "type": "docker",
      "uri": "myregistry/wordcount-reduce:latest"
    },
    "combiner": {
      "type": "docker",
      "uri": "myregistry/wordcount-combiner:latest"
    }
  },
  "config": {
    "numMappers": 10,
    "numReducers": 10,
    "maxMapAttempts": 3,
    "maxReduceAttempts": 3,
    "mapTimeoutSeconds": 600,
    "reduceTimeoutSeconds": 1800,
    "runtime": {
      "defaultType": "docker",
      "resources": {
        "cpu": "1",
        "memory": "512Mi"
      },
      "pullPolicy": "IfNotPresent"
    }
  },
  "metadata": {
    "user": "alice",
    "team": "analytics"
  }
}
```

**Response:** `201 Created`
```json
{
  "job_id": "job-a1b2c3d4",
  "status": "SUBMITTED",
  "submitted_at": "2025-09-25T10:30:00Z",
  "estimated_map_tasks": 10,
  "estimated_reduce_tasks": 10,
  "links": {
    "self": "/api/jobs/job-a1b2c3d4"
  }
}
```

### 2. Get Job Details

Retrieve detailed information about a specific job.

**Endpoint:** `GET /api/jobs/{id}`

**Response:** `200 OK`
```json
{
  "job_id": "job-a1b2c3d4",
  "name": "word-count-20250925",
  "status": "MAPPING",
  "progress": {
    "map_tasks": {
      "total": 45,
      "completed": 32,
      "running": 8,
      "failed": 2,
      "pending": 3
    },
    "reduce_tasks": {
      "total": 10,
      "completed": 0,
      "running": 0,
      "failed": 0,
      "pending": 10
    }
  },
  "timestamps": {
    "submitted": "2025-09-25T10:30:00Z",
    "started": "2025-09-25T10:30:05Z",
    "mapping_started": "2025-09-25T10:30:05Z",
    "mapping_completed": null,
    "reducing_started": null,
    "completed": null
  },
  "output": {
    "location": "s3://my-bucket/jobs/job-a1b2c3d4/out/",
    "available": false
  },
  "errors": [
    {
      "task_id": "map-007",
      "attempt": 2,
      "error": "worker timeout",
      "timestamp": "2025-09-25T10:35:12Z"
    }
  ]
}
```

### 3. List Jobs

Retrieve a list of jobs with optional filtering and pagination.

**Endpoint:** `GET /api/jobs`

**Query Parameters:**
- `status` (optional): Filter by job status (e.g., "RUNNING", "SUBMITTED", "SUCCEEDED")
- `limit` (optional): Maximum number of jobs to return (default: 10)
- `offset` (optional): Offset for pagination (default: 0)

**Examples:**
```bash
# Get all jobs
GET /api/jobs

# Get running jobs
GET /api/jobs?status=RUNNING

# Pagination
GET /api/jobs?limit=20&offset=0
GET /api/jobs?limit=20&offset=20
```

**Response:** `200 OK`
```json
{
  "jobs": [
    {
      "job_id": "job-a1b2c3d4",
      "name": "word-count-20250925",
      "status": "RUNNING",
      "submitted_at": "2025-09-25T10:30:00Z",
      "completed_at": null
    }
  ],
  "total": 42,
  "limit": 10,
  "offset": 0,
  "next_offset": 10
}
```

### 4. Get Job Tasks

Retrieve all tasks for a specific job.

**Endpoint:** `GET /api/jobs/{id}/tasks`

**Response:** `200 OK`
```json
{
  "tasks": [
    {
      "task_id": "map-001",
      "type": "MAP",
      "status": "COMPLETED",
      "worker_id": "worker-0",
      "attempts": 1,
      "start_time": "2025-09-25T10:30:10Z",
      "end_time": "2025-09-25T10:32:45Z"
    },
    {
      "task_id": "reduce-001",
      "type": "REDUCE",
      "status": "RUNNING",
      "worker_id": "worker-1",
      "attempts": 1,
      "start_time": "2025-09-25T10:35:00Z",
      "end_time": null
    }
  ]
}
```

## Error Responses

All endpoints may return error responses in the following format:

**Response:** `4xx` or `5xx`
```json
{
  "error": "validation failed",
  "message": "job name is required",
  "code": 400
}
```

## Usage Examples

```bash
# Create a job
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "wordcount-example",
    "input": {
      "type": "s3",
      "paths": ["s3://bucket/input/*.txt"],
      "format": "text"
    },
    "executors": {
      "map": {"type": "docker", "uri": "wordcount-map:latest"},
      "reduce": {"type": "docker", "uri": "wordcount-reduce:latest"}
    },
    "config": {
      "numMappers": 4,
      "numReducers": 2
    }
  }'

# Get job details
curl http://localhost:8080/api/jobs/job-a1b2c3d4

# List all jobs
curl http://localhost:8080/api/jobs

# List running jobs
curl "http://localhost:8080/api/jobs?status=RUNNING"

# Get job tasks
curl http://localhost:8080/api/jobs/job-a1b2c3d4/tasks
```

## Architecture Notes

The current implementation provides a REST API layer with in-memory storage. It includes:

- Request validation
- Job ID generation
- Task progress tracking
- Pagination and filtering

### Future Enhancements

1. **Persistent Storage**: Replace in-memory maps with a database (PostgreSQL, etcd, etc.)
2. **Job Scheduler**: Implement actual job scheduling and task distribution
3. **Worker Management**: Track worker nodes and assign tasks
8. **Job Cancellation**: Add DELETE /api/jobs/{id} endpoint
9. **Log Streaming**: Stream task logs via SSE

## Testing

Run the test suite:

```bash
# All tests
go test ./pkg/distributed/coordinator/

# With coverage
go test -cover ./pkg/distributed/coordinator/

# Verbose output
go test -v ./pkg/distributed/coordinator/
```

## Development

The API is structured as follows:

- [types.go](types.go): Request/response type definitions
- [api.go](api.go): HTTP handlers and routing
- [middleware.go](middleware.go): Logging and recovery middleware
- [api_test.go](api_test.go): Comprehensive test suite for API handlers
- [middleware_test.go](middleware_test.go): Comprehensive test suite for middleware
- [coordinator](../../../cmd/coordinator/main.go): Server entry point
