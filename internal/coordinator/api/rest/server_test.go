package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nemanja-m/gomr/internal/coordinator/storage"
)

func TestSubmitJob(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := SubmitJobRequest{
		Name: "test-wordcount",
		Input: InputConfig{
			Type:   "s3",
			Paths:  []string{"s3://bucket/input/*.txt"},
			Format: "text",
		},
		Executors: ExecutorsConfig{
			Map: ExecutorSpec{
				Type: "docker",
				URI:  "wordcount-map:latest",
			},
			Reduce: ExecutorSpec{
				Type: "docker",
				URI:  "wordcount-reduce:latest",
			},
		},
		Config: JobConfig{
			NumReducers: 5,
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var resp SubmitJobResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.JobID == "" {
		t.Error("Expected job ID to be set")
	}

	if resp.Status != "PENDING" {
		t.Errorf("Expected status PENDING, got %s", resp.Status)
	}

	if resp.EstimatedMapTasks != 0 {
		t.Errorf("Expected 0 map tasks (determined dynamically), got %d", resp.EstimatedMapTasks)
	}

	if resp.EstimatedReduceTasks != 5 {
		t.Errorf("Expected 5 reduce tasks, got %d", resp.EstimatedReduceTasks)
	}
}

func TestSubmitJobValidation(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	tests := []struct {
		name    string
		req     SubmitJobRequest
		wantErr bool
	}{
		{
			name: "missing name",
			req: SubmitJobRequest{
				Input: InputConfig{
					Type:  "s3",
					Paths: []string{"s3://bucket/input/*.txt"},
				},
				Executors: ExecutorsConfig{
					Map: ExecutorSpec{
						Type: "docker",
						URI:  "map:latest",
					},
					Reduce: ExecutorSpec{
						Type: "docker",
						URI:  "reduce:latest",
					},
				},
				Config: JobConfig{
					NumReducers: 5,
				},
			},
			wantErr: true,
		},
		{
			name: "missing input paths",
			req: SubmitJobRequest{
				Name: "test-job",
				Input: InputConfig{
					Type: "s3",
				},
				Executors: ExecutorsConfig{
					Map: ExecutorSpec{
						Type: "docker",
						URI:  "map:latest",
					},
					Reduce: ExecutorSpec{
						Type: "docker",
						URI:  "reduce:latest",
					},
				},
				Config: JobConfig{
					NumReducers: 5,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid numReducers",
			req: SubmitJobRequest{
				Name: "test-job",
				Input: InputConfig{
					Type:  "s3",
					Paths: []string{"s3://bucket/input/*.txt"},
				},
				Executors: ExecutorsConfig{
					Map: ExecutorSpec{
						Type: "docker",
						URI:  "map:latest",
					},
					Reduce: ExecutorSpec{
						Type: "docker",
						URI:  "reduce:latest",
					},
				},
				Config: JobConfig{
					NumReducers: 0,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.req)
			httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, httpReq)

			if tt.wantErr && w.Code != http.StatusBadRequest {
				t.Errorf("Expected status 400, got %d", w.Code)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// First create a job
	req := SubmitJobRequest{
		Name: "test-job",
		Input: InputConfig{
			Type:   "s3",
			Paths:  []string{"s3://bucket/input/*.txt"},
			Format: "text",
		},
		Executors: ExecutorsConfig{
			Map: ExecutorSpec{
				Type: "docker",
				URI:  "map:latest",
			},
			Reduce: ExecutorSpec{
				Type: "docker",
				URI:  "reduce:latest",
			},
		},
		Config: JobConfig{
			NumReducers: 5,
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var createResp SubmitJobResponse
	json.NewDecoder(w.Body).Decode(&createResp)

	// Now get the job
	httpReq = httptest.NewRequest(http.MethodGet, "/api/jobs/"+createResp.JobID, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var getResp GetJobResponse
	if err := json.NewDecoder(w.Body).Decode(&getResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if getResp.JobID != createResp.JobID {
		t.Errorf("Expected job ID %s, got %s", createResp.JobID, getResp.JobID)
	}

	if getResp.Name != "test-job" {
		t.Errorf("Expected name test-job, got %s", getResp.Name)
	}
}

func TestGetJobNotFound(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Use a valid UUID that doesn't exist in the store
	httpReq := httptest.NewRequest(http.MethodGet, "/api/jobs/00000000-0000-0000-0000-000000000000", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestListJobs(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Create several jobs
	for range 3 {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:   "s3",
				Paths:  []string{"s3://bucket/input/*.txt"},
				Format: "text",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		body, _ := json.Marshal(req)
		httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httpReq)
	}

	// List jobs
	httpReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp ListJobsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Total != 3 {
		t.Errorf("Expected 3 jobs, got %d", resp.Total)
	}

	if len(resp.Jobs) != 3 {
		t.Errorf("Expected 3 jobs in response, got %d", len(resp.Jobs))
	}
}

func TestListJobsPagination(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Create 15 jobs
	for range 15 {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:   "s3",
				Paths:  []string{"s3://bucket/input/*.txt"},
				Format: "text",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		body, _ := json.Marshal(req)
		httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httpReq)
	}

	// Get first page
	httpReq := httptest.NewRequest(http.MethodGet, "/api/jobs?limit=10&offset=0", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var resp ListJobsResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if resp.Total != 15 {
		t.Errorf("Expected total 15, got %d", resp.Total)
	}

	if len(resp.Jobs) != 10 {
		t.Errorf("Expected 10 jobs in first page, got %d", len(resp.Jobs))
	}

	if resp.NextOffset == nil || *resp.NextOffset != 10 {
		t.Error("Expected next offset to be 10")
	}

	// Get second page
	httpReq = httptest.NewRequest(http.MethodGet, "/api/jobs?limit=10&offset=10", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var resp2 ListJobsResponse
	json.NewDecoder(w.Body).Decode(&resp2)

	if len(resp2.Jobs) != 5 {
		t.Errorf("Expected 5 jobs in second page, got %d", len(resp2.Jobs))
	}

	if resp2.NextOffset != nil {
		t.Errorf("Expected no next offset on last page, got %v (total=%d, offset=%d, limit=%d)", *resp2.NextOffset, resp2.Total, resp2.Offset, resp2.Limit)
	}
}

func TestGetJobTasks(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Create a job
	req := SubmitJobRequest{
		Name: "test-job",
		Input: InputConfig{
			Type:   "s3",
			Paths:  []string{"s3://bucket/input/*.txt"},
			Format: "text",
		},
		Executors: ExecutorsConfig{
			Map: ExecutorSpec{
				Type: "docker",
				URI:  "map:latest",
			},
			Reduce: ExecutorSpec{
				Type: "docker",
				URI:  "reduce:latest",
			},
		},
		Config: JobConfig{
			NumReducers: 5,
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var createResp SubmitJobResponse
	json.NewDecoder(w.Body).Decode(&createResp)

	// Get tasks
	httpReq = httptest.NewRequest(http.MethodGet, "/api/jobs/"+createResp.JobID+"/tasks", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp GetTasksResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Initially should have no tasks
	if len(resp.Tasks) != 0 {
		t.Errorf("Expected 0 tasks initially, got %d", len(resp.Tasks))
	}
}

func TestMethodNotAllowed(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Try DELETE on /api/jobs
	httpReq := httptest.NewRequest(http.MethodDelete, "/api/jobs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestListJobsReturnsEmptyArray(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// List jobs when there are none
	httpReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Save body for later check
	bodyBytes := w.Body.Bytes()

	var resp ListJobsResponse
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Jobs == nil {
		t.Error("Expected Jobs to be empty array, got nil")
	}

	if len(resp.Jobs) != 0 {
		t.Errorf("Expected 0 jobs, got %d", len(resp.Jobs))
	}

	// Check the raw JSON to ensure it's [] not null
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, `"jobs":[]`) {
		t.Errorf("Expected JSON to contain 'jobs':[], got: %s", bodyStr)
	}
}

func TestGetJobTasksReturnsEmptyArray(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// First create a job
	req := SubmitJobRequest{
		Name: "test-job",
		Input: InputConfig{
			Type:   "s3",
			Paths:  []string{"s3://bucket/input/*.txt"},
			Format: "text",
		},
		Executors: ExecutorsConfig{
			Map: ExecutorSpec{
				Type: "docker",
				URI:  "map:latest",
			},
			Reduce: ExecutorSpec{
				Type: "docker",
				URI:  "reduce:latest",
			},
		},
		Config: JobConfig{
			NumReducers: 5,
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var createResp SubmitJobResponse
	json.NewDecoder(w.Body).Decode(&createResp)

	// Get tasks for the job
	httpReq = httptest.NewRequest(http.MethodGet, "/api/jobs/"+createResp.JobID+"/tasks", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Save body for later check
	bodyBytes := w.Body.Bytes()

	var resp GetTasksResponse
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Tasks == nil {
		t.Error("Expected Tasks to be empty array, got nil")
	}

	if len(resp.Tasks) != 0 {
		t.Errorf("Expected 0 tasks, got %d", len(resp.Tasks))
	}

	// Check the raw JSON to ensure it's [] not null
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, `"tasks":[]`) {
		t.Errorf("Expected JSON to contain 'tasks':[], got: %s", bodyStr)
	}
}

func TestGetJobErrorsReturnsEmptyArray(t *testing.T) {
	jobStore := storage.NewInMemoryJobStore()
	taskStore := storage.NewInMemoryTaskStore()
	api := NewAPI(jobStore, taskStore, newMockLogger())
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// First create a job
	req := SubmitJobRequest{
		Name: "test-job",
		Input: InputConfig{
			Type:   "s3",
			Paths:  []string{"s3://bucket/input/*.txt"},
			Format: "text",
		},
		Executors: ExecutorsConfig{
			Map: ExecutorSpec{
				Type: "docker",
				URI:  "map:latest",
			},
			Reduce: ExecutorSpec{
				Type: "docker",
				URI:  "reduce:latest",
			},
		},
		Config: JobConfig{
			NumReducers: 5,
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	var createResp SubmitJobResponse
	json.NewDecoder(w.Body).Decode(&createResp)

	// Get the job
	httpReq = httptest.NewRequest(http.MethodGet, "/api/jobs/"+createResp.JobID, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Save body for later check
	bodyBytes := w.Body.Bytes()

	var resp GetJobResponse
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Errors == nil {
		t.Error("Expected Errors to be empty array, got nil")
	}

	if len(resp.Errors) != 0 {
		t.Errorf("Expected 0 errors, got %d", len(resp.Errors))
	}

	// Check the raw JSON to ensure it's [] not null
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, `"errors":[]`) {
		t.Errorf("Expected JSON to contain 'errors':[], got: %s", bodyStr)
	}
}
