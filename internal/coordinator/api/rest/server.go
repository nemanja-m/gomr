package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type JobInfo struct {
	Request   CreateJobRequest
	Response  GetJobResponse
	CreatedAt time.Time
	UpdatedAt time.Time
}

type API struct {
	mu    sync.RWMutex
	jobs  map[string]*JobInfo
	tasks map[string][]TaskInfo // jobID -> tasks
}

func NewAPI() *API {
	return &API{
		jobs:  make(map[string]*JobInfo),
		tasks: make(map[string][]TaskInfo),
	}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/jobs", a.createJob)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("GET /api/jobs/{id}/tasks", a.getJobTasks)
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if err := a.validateCreateJobRequest(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "validation failed", err.Error())
		return
	}

	jobID := uuid.New().String()
	now := time.Now().UTC()

	// Estimate tasks (simplified - in real implementation, would scan input)
	estimatedMapTasks := req.Config.NumMappers
	estimatedReduceTasks := req.Config.NumReducers

	createResp := CreateJobResponse{
		JobID:                jobID,
		Status:               "SUBMITTED",
		SubmittedAt:          now,
		EstimatedMapTasks:    estimatedMapTasks,
		EstimatedReduceTasks: estimatedReduceTasks,
		Links: Links{
			Self: fmt.Sprintf("/api/jobs/%s", jobID),
		},
	}

	jobInfo := &JobInfo{
		Request:   req,
		CreatedAt: now,
		UpdatedAt: now,
		Response: GetJobResponse{
			JobID:  jobID,
			Name:   req.Name,
			Status: "SUBMITTED",
			Progress: ProgressInfo{
				MapTasks: TaskProgress{
					Total:     estimatedMapTasks,
					Completed: 0,
					Running:   0,
					Failed:    0,
					Pending:   estimatedMapTasks,
				},
				ReduceTasks: TaskProgress{
					Total:     estimatedReduceTasks,
					Completed: 0,
					Running:   0,
					Failed:    0,
					Pending:   estimatedReduceTasks,
				},
			},
			Timestamps: TimestampsInfo{
				Submitted: now,
			},
			Output: OutputInfo{
				Location:  fmt.Sprintf("s3://output-bucket/jobs/%s/out/", jobID),
				Available: false,
			},
			Errors: []ErrorInfo{},
		},
	}

	a.mu.Lock()
	a.jobs[jobID] = jobInfo
	a.tasks[jobID] = []TaskInfo{}
	a.mu.Unlock()

	a.respondJSON(w, http.StatusCreated, createResp)
}

// getJob handles GET /api/jobs/{id}
func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		a.respondError(w, http.StatusBadRequest, "job ID required", "")
		return
	}

	a.mu.RLock()
	jobInfo, exists := a.jobs[jobID]
	a.mu.RUnlock()

	if !exists {
		a.respondError(w, http.StatusNotFound, "job not found", "")
		return
	}

	a.respondJSON(w, http.StatusOK, jobInfo.Response)
}

// listJobs handles GET /api/jobs with filters and pagination
func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// Parse filters
	statusFilter := query.Get("status")

	// Parse pagination
	limit := 10
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	offset := 0
	if offsetStr := query.Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	allJobs := make([]JobSummary, 0)
	for jobID, jobInfo := range a.jobs {
		// Apply status filter if provided
		if statusFilter != "" && jobInfo.Response.Status != statusFilter {
			continue
		}

		summary := JobSummary{
			JobID:       jobID,
			Name:        jobInfo.Request.Name,
			Status:      jobInfo.Response.Status,
			SubmittedAt: jobInfo.Response.Timestamps.Submitted,
			CompletedAt: jobInfo.Response.Timestamps.Completed,
		}
		allJobs = append(allJobs, summary)
	}

	total := len(allJobs)

	start := min(offset, total)
	end := min(start+limit, total)

	pagedJobs := allJobs[start:end]

	// Calculate next offset
	var nextOffset *int
	if end < total {
		next := end
		nextOffset = &next
	}

	resp := ListJobsResponse{
		Jobs:       pagedJobs,
		Total:      total,
		Limit:      limit,
		Offset:     offset,
		NextOffset: nextOffset,
	}

	a.respondJSON(w, http.StatusOK, resp)
}

// getJobTasks handles GET /api/jobs/{id}/tasks
func (a *API) getJobTasks(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		a.respondError(w, http.StatusBadRequest, "job ID required", "")
		return
	}

	a.mu.RLock()
	tasks, exists := a.tasks[jobID]
	a.mu.RUnlock()

	if !exists {
		a.respondError(w, http.StatusNotFound, "job not found", "")
		return
	}

	resp := GetTasksResponse{
		Tasks: tasks,
	}

	a.respondJSON(w, http.StatusOK, resp)
}

// validateCreateJobRequest validates the create job request
func (a *API) validateCreateJobRequest(req *CreateJobRequest) error {
	if req.Name == "" {
		return fmt.Errorf("job name is required")
	}

	if req.Input.Type == "" {
		return fmt.Errorf("input type is required")
	}

	if len(req.Input.Paths) == 0 {
		return fmt.Errorf("at least one input path is required")
	}

	if req.Executors.Map.Type == "" || req.Executors.Map.URI == "" {
		return fmt.Errorf("map executor type and URI are required")
	}

	if req.Executors.Reduce.Type == "" || req.Executors.Reduce.URI == "" {
		return fmt.Errorf("reduce executor type and URI are required")
	}

	if req.Config.NumMappers <= 0 {
		return fmt.Errorf("numMappers must be greater than 0")
	}

	if req.Config.NumReducers <= 0 {
		return fmt.Errorf("numReducers must be greater than 0")
	}

	return nil
}

func (a *API) respondJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (a *API) respondError(w http.ResponseWriter, statusCode int, error string, message string) {
	resp := ErrorResponse{
		Error:   error,
		Message: message,
		Code:    statusCode,
	}
	a.respondJSON(w, statusCode, resp)
}

const (
	readTimeout  = 15 * time.Second
	writeTimeout = 15 * time.Second
	idleTimeout  = 60 * time.Second
)

func NewServer(addr string) *http.Server {
	api := NewAPI()
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	handler := ChainMiddleware(
		mux,
		RecoveryMiddleware,
		LoggingMiddleware,
	)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}
}
