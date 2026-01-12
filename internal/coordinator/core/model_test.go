package core

import (
	"testing"
	"time"
)

func TestJob_Duration(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		startedAt   *time.Time
		completedAt *time.Time
		want        time.Duration
	}{
		{
			name:        "both nil returns zero",
			startedAt:   nil,
			completedAt: nil,
			want:        0,
		},
		{
			name:        "completed job returns duration",
			startedAt:   ptrTime(now),
			completedAt: ptrTime(now.Add(5 * time.Minute)),
			want:        5 * time.Minute,
		},
		{
			name:        "zero duration when started and completed at same time",
			startedAt:   ptrTime(now),
			completedAt: ptrTime(now),
			want:        0,
		},
		{
			name:        "sub-second duration",
			startedAt:   ptrTime(now),
			completedAt: ptrTime(now.Add(500 * time.Millisecond)),
			want:        500 * time.Millisecond,
		},
		{
			name:        "long duration",
			startedAt:   ptrTime(now),
			completedAt: ptrTime(now.Add(24 * time.Hour)),
			want:        24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{
				StartedAt:   tt.startedAt,
				CompletedAt: tt.completedAt,
			}

			got := job.Duration()
			if got != tt.want {
				t.Errorf("Job.Duration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
