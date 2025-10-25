package core

type KeyValue struct {
	Key   string
	Value string
}

type Job interface {
	Map(key, value string) []KeyValue
	Reduce(key string, values []string) KeyValue

	Configure(config map[string]string) error
	Validate() error

	Name() string
	Describe() string
}

type JobState int

const (
	JobStateSubmitted JobState = iota
	JobStatePlanning
	JobStateRunning
	JobStateSucceeded
	JobStateFailed
)

type TaskState int

const (
	TaskStatePending TaskState = iota
	TaskStateRunning
	TaskStateSucceeded
	TaskStateFailed
	TaskStateKilled
)
