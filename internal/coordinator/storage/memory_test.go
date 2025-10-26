package storage

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

func TestGetJobs_StatusFilter(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create jobs with different statuses
	pendingJob := &core.Job{
		ID:          uuid.New(),
		Name:        "pending-job",
		Status:      core.JobStatusPending,
		SubmittedAt: time.Now(),
	}
	store.SaveJob(pendingJob)

	runningJob := &core.Job{
		ID:          uuid.New(),
		Name:        "running-job",
		Status:      core.JobStatusRunning,
		SubmittedAt: time.Now(),
	}
	store.SaveJob(runningJob)

	completedJob := &core.Job{
		ID:          uuid.New(),
		Name:        "completed-job",
		Status:      core.JobStatusCompleted,
		SubmittedAt: time.Now(),
	}
	store.SaveJob(completedJob)

	// Test filter for PENDING status
	pendingStatus := core.JobStatusPending
	filter := core.JobFilter{
		Status: &pendingStatus,
		Limit:  10,
		Offset: 0,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 1 {
		t.Errorf("Expected total 1, got %d", total)
	}

	if len(jobs) != 1 {
		t.Errorf("Expected 1 job, got %d", len(jobs))
	}

	if jobs[0].Status != core.JobStatusPending {
		t.Errorf("Expected PENDING status, got %s", jobs[0].Status)
	}

	// Test filter for RUNNING status
	runningStatus := core.JobStatusRunning
	filter.Status = &runningStatus

	jobs, total, err = store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 1 {
		t.Errorf("Expected total 1, got %d", total)
	}

	if len(jobs) != 1 {
		t.Errorf("Expected 1 job, got %d", len(jobs))
	}

	if jobs[0].Status != core.JobStatusRunning {
		t.Errorf("Expected RUNNING status, got %s", jobs[0].Status)
	}
}

func TestGetJobs_NoStatusFilter(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create jobs with different statuses
	for range 5 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "job",
			Status:      core.JobStatusPending,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	// Test without status filter
	filter := core.JobFilter{
		Limit:  10,
		Offset: 0,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 5 {
		t.Errorf("Expected total 5, got %d", total)
	}

	if len(jobs) != 5 {
		t.Errorf("Expected 5 jobs, got %d", len(jobs))
	}
}

func TestGetJobs_Pagination(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create 15 jobs
	for range 15 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "job",
			Status:      core.JobStatusPending,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	// Test first page
	filter := core.JobFilter{
		Limit:  10,
		Offset: 0,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 15 {
		t.Errorf("Expected total 15, got %d", total)
	}

	if len(jobs) != 10 {
		t.Errorf("Expected 10 jobs in first page, got %d", len(jobs))
	}

	// Test second page
	filter.Offset = 10

	jobs, total, err = store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 15 {
		t.Errorf("Expected total 15, got %d", total)
	}

	if len(jobs) != 5 {
		t.Errorf("Expected 5 jobs in second page, got %d", len(jobs))
	}
}

func TestGetJobs_StatusFilterAndPagination(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create 15 PENDING jobs and 5 RUNNING jobs
	for range 15 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "pending-job",
			Status:      core.JobStatusPending,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	for range 5 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "running-job",
			Status:      core.JobStatusRunning,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	// Test first page of PENDING jobs
	pendingStatus := core.JobStatusPending
	filter := core.JobFilter{
		Status: &pendingStatus,
		Limit:  10,
		Offset: 0,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 15 {
		t.Errorf("Expected total 15 PENDING jobs, got %d", total)
	}

	if len(jobs) != 10 {
		t.Errorf("Expected 10 jobs in first page, got %d", len(jobs))
	}

	// Verify all jobs are PENDING
	for _, job := range jobs {
		if job.Status != core.JobStatusPending {
			t.Errorf("Expected PENDING status, got %s", job.Status)
		}
	}

	// Test second page of PENDING jobs
	filter.Offset = 10

	jobs, total, err = store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 15 {
		t.Errorf("Expected total 15 PENDING jobs, got %d", total)
	}

	if len(jobs) != 5 {
		t.Errorf("Expected 5 jobs in second page, got %d", len(jobs))
	}
}

func TestGetJobs_EmptyResults(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create only PENDING jobs
	for range 5 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "pending-job",
			Status:      core.JobStatusPending,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	// Filter for COMPLETED jobs (should be empty)
	completedStatus := core.JobStatusCompleted
	filter := core.JobFilter{
		Status: &completedStatus,
		Limit:  10,
		Offset: 0,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 0 {
		t.Errorf("Expected total 0, got %d", total)
	}

	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs, got %d", len(jobs))
	}
}

func TestGetJobs_OffsetBeyondTotal(t *testing.T) {
	store := NewInMemoryJobStore()

	// Create 5 jobs
	for range 5 {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "job",
			Status:      core.JobStatusPending,
			SubmittedAt: time.Now(),
		}
		store.SaveJob(job)
	}

	// Use offset beyond total
	filter := core.JobFilter{
		Limit:  10,
		Offset: 100,
	}

	jobs, total, err := store.GetJobs(filter)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if total != 5 {
		t.Errorf("Expected total 5, got %d", total)
	}

	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs when offset is beyond total, got %d", len(jobs))
	}
}
