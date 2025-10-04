package local

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/nemanja-m/gomr/pkg/core"
)

type Engine struct {
	config core.JobConfig
}

func NewEngine(config core.JobConfig) *Engine {
	return &Engine{config: config}
}

func (e *Engine) Run() error {
	lines, err := e.readAllLines()
	if err != nil {
		return err
	}

	mapped := e.runMap(lines)
	partitioned := e.runShuffle(mapped)
	results := e.runReduce(partitioned)

	if err := e.writeResults(results); err != nil {
		return err
	}

	return nil
}

func (e *Engine) readAllLines() ([]Line, error) {
	matches, err := filepath.Glob(e.config.Input)
	if err != nil {
		return nil, err
	}
	if matches == nil {
		return nil, fmt.Errorf("no files matched the input pattern: %s", e.config.Input)
	}

	var allLines []Line
	for _, file := range matches {
		lines, err := ReadLines(file)
		if err != nil {
			return nil, err
		}

		allLines = append(allLines, lines...)
	}

	return allLines, nil
}

func (e *Engine) runMap(lines []Line) []core.KeyValue {
	var results []core.KeyValue
	for _, line := range lines {
		kvs := e.config.MapFunc(fmt.Sprintf("%s:%d", line.Filename, line.Number), line.Text)
		results = append(results, kvs...)
	}
	return results
}

func (e *Engine) runShuffle(mapped []core.KeyValue) map[int][]core.KeyValue {
	var partitioned = make(map[int][]core.KeyValue)
	for _, kv := range mapped {
		partition := core.Partition(kv.Key, e.config.NumReducers)
		partitioned[partition] = append(partitioned[partition], kv)
	}

	for _, records := range partitioned {
		slices.SortFunc(records, func(left, right core.KeyValue) int {
			return cmp.Compare(left.Key, right.Key)
		})
	}

	return partitioned
}

func (e *Engine) runReduce(partitioned map[int][]core.KeyValue) map[int][]core.KeyValue {
	var results = make(map[int][]core.KeyValue)
	for part, sortedPartition := range partitioned {
		results[part] = e.reducePartition(sortedPartition)
	}
	return results
}

func (e *Engine) reducePartition(sortedPartition []core.KeyValue) []core.KeyValue {
	var results []core.KeyValue

	i := 0
	for i < len(sortedPartition) {
		key := sortedPartition[i].Key
		values := []string{}

		for i < len(sortedPartition) && sortedPartition[i].Key == key {
			values = append(values, sortedPartition[i].Value)
			i++
		}

		// Streaming reduce
		results = append(results, e.config.ReduceFunc(key, values))
	}

	return results
}

func (e *Engine) writeResults(results map[int][]core.KeyValue) error {
	for part, records := range results {
		partFilename := fmt.Sprintf("part-%04d.tsv", part)
		outputPath := filepath.Join(e.config.Output, partFilename)

		lines := make([]string, len(records))
		for _, record := range records {
			lines = append(lines, fmt.Sprintf("%s\t%s\n", record.Key, record.Value))
		}

		if err := WriteLines(outputPath, lines); err != nil {
			return err
		}
	}
	return nil
}
