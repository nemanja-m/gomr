package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

type API struct {
	jobStore  core.JobStore
	taskStore core.TaskStore
	logger    logging.Logger
}

func NewAPI(jobStore core.JobStore, taskStore core.TaskStore, logger logging.Logger) *API {
	return &API{
		jobStore:  jobStore,
		taskStore: taskStore,
		logger:    logger,
	}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/jobs", a.submitJob)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("GET /api/jobs/{id}/tasks", a.getJobTasks)
}

// submitJob handles POST /api/jobs
func (a *API) submitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if err := a.validateCreateJobRequest(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "validation failed", err.Error())
		return
	}

	job := req.ToJob()
	if err := a.jobStore.SaveJob(job); err != nil {
		a.respondError(w, http.StatusInternalServerError, "failed to save job", err.Error())
		return
	}

	// Estimate tasks (simplified - in real implementation, would scan input)
	estimatedMapTasks := req.Config.NumMappers
	estimatedReduceTasks := req.Config.NumReducers

	createResp := SubmitJobResponse{
		JobID:                job.ID.String(),
		Status:               string(job.Status),
		SubmittedAt:          job.SubmittedAt,
		EstimatedMapTasks:    estimatedMapTasks,
		EstimatedReduceTasks: estimatedReduceTasks,
		Links: Links{
			Self: fmt.Sprintf("/api/jobs/%s", job.ID.String()),
		},
	}

	a.respondJSON(w, http.StatusCreated, createResp)
}

// getJob handles GET /api/jobs/{id}
func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	jobIDStr := r.PathValue("id")
	if jobIDStr == "" {
		a.respondError(w, http.StatusBadRequest, "job ID required", "")
		return
	}

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid job ID format", err.Error())
		return
	}

	job, err := a.jobStore.GetJobByID(jobID)
	if err != nil {
		a.respondError(w, http.StatusInternalServerError, "failed to get job", err.Error())
		return
	}

	if job == nil {
		a.respondError(w, http.StatusNotFound, "job not found", "")
		return
	}

	resp := ToGetJobResponse(job)

	a.respondJSON(w, http.StatusOK, resp)
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

	jobs, err := a.jobStore.ListJobs()
	if err != nil {
		a.respondError(w, http.StatusInternalServerError, "failed to list jobs", err.Error())
		return
	}

	allJobs := make([]JobSummary, 0)
	for _, job := range jobs {
		// Apply status filter if provided
		if statusFilter != "" && string(job.Status) != statusFilter {
			continue
		}

		summary := ToJobSummary(job)
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
	jobIDStr := r.PathValue("id")
	if jobIDStr == "" {
		a.respondError(w, http.StatusBadRequest, "job ID required", "")
		return
	}

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid job ID format", err.Error())
		return
	}

	// Check if job exists
	job, err := a.jobStore.GetJobByID(jobID)
	if err != nil {
		a.respondError(w, http.StatusInternalServerError, "failed to get job", err.Error())
		return
	}

	if job == nil {
		a.respondError(w, http.StatusNotFound, "job not found", "")
		return
	}

	tasks, err := a.taskStore.ListTasksByJobID(jobID)
	if err != nil {
		a.respondError(w, http.StatusInternalServerError, "failed to list tasks", err.Error())
		return
	}

	taskInfos := make([]TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		taskInfos = append(taskInfos, ToTaskInfo(task))
	}

	resp := GetTasksResponse{Tasks: taskInfos}

	a.respondJSON(w, http.StatusOK, resp)
}

// validateCreateJobRequest validates the create job request
func (a *API) validateCreateJobRequest(req *SubmitJobRequest) error {
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

func NewServer(
	addr string,
	jobStore core.JobStore,
	taskStore core.TaskStore,
	logger logging.Logger,
) *http.Server {
	api := NewAPI(jobStore, taskStore, logger)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	handler := ChainMiddleware(
		mux,
		RecoveryMiddleware(logger),
		LoggingMiddleware(logger),
	)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}
}
