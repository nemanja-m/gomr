package jobs

import (
	"fmt"
	"sync"

	"github.com/nemanja-m/gomr/pkg/core"
)

type JobBuilder func() core.Job

type Registry struct {
	mu   sync.RWMutex
	jobs map[string]JobBuilder
}

var registry = &Registry{
	jobs: make(map[string]JobBuilder),
}

func Register(name string, jobBuilder JobBuilder) {
	registry.Register(name, jobBuilder)
}

func (r *Registry) Register(name string, jobBuilder JobBuilder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[name] = jobBuilder
}

func Build(name string, config map[string]string) (core.Job, error) {
	return registry.Build(name, config)
}

func (r *Registry) Build(name string, config map[string]string) (core.Job, error) {
	builder, exists := r.jobs[name]
	if !exists {
		return nil, fmt.Errorf("job '%s' is not registered", name)
	}
	job := builder()
	if err := job.Configure(config); err != nil {
		return nil, fmt.Errorf("failed to configure job '%s': %w", name, err)
	}
	if err := job.Validate(); err != nil {
		return nil, fmt.Errorf("job '%s' validation failed: %w", name, err)
	}
	return job, nil
}

func List() []string {
	return registry.List()
}

func (r *Registry) List() []string {
	var names []string
	for name := range r.jobs {
		names = append(names, name)
	}
	return names
}

func Describe(name string) (string, error) {
	return registry.Describe(name)
}

func (r *Registry) Describe(name string) (string, error) {
	builder, exists := r.jobs[name]
	if !exists {
		return "", fmt.Errorf("job '%s' is not registered", name)
	}
	job := builder()
	return job.Describe(), nil
}
