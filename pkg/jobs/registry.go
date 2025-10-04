package jobs

import (
	"fmt"

	"github.com/nemanja-m/gomr/pkg/core"
)

type Job struct {
	Map    core.MapFunc
	Reduce core.ReduceFunc
}

var registry = make(map[string]Job)

func Register(name string, job Job) error {
	if _, exists := registry[name]; exists {
		return fmt.Errorf("job already registered: %s", name)
	}
	registry[name] = job
	return nil
}

func Get(name string) (Job, error) {
	job, exists := registry[name]
	if !exists {
		return Job{}, fmt.Errorf("job not found: %s", name)
	}
	return job, nil
}

func List() []string {
	var names []string
	for name := range registry {
		names = append(names, name)
	}
	return names
}
