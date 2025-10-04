package core

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) KeyValue

type KeyValue struct {
	Key   string
	Value string
}

type JobConfig struct {
	Input       string
	Output      string
	NumReducers int
	MapFunc     MapFunc
	ReduceFunc  ReduceFunc
}
