package local

import (
	"cmp"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/nemanja-m/gomr/pkg/core"
)

type Config struct {
	Job         core.Job
	Input       string
	Output      string
	NumMappers  int
	NumReducers int
}

type Engine struct {
	config Config
}

func NewEngine(config Config) *Engine {
	return &Engine{config: config}
}

func (e *Engine) Run() error {
	// Create temporary directory for intermediate shuffle files.
	shuffleDir, err := os.MkdirTemp("", "gomr-job-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(shuffleDir)

	// Run map tasks
	inputFiles, err := FindFiles(e.config.Input)
	if err != nil {
		return err
	}

	mapperPool := NewPool(e.config.NumMappers)
	mapperPool.Start()

	for mapperId, file := range inputFiles {
		mapperPool.Submit(func() {
			log.Printf("Starting map task %d", mapperId)
			if err := e.runMapTask(mapperId, shuffleDir, file); err != nil {
				log.Printf("Error running map task %d: %v", mapperId, err)
			} else {
				log.Printf("Completed map task %d", mapperId)
			}
		})
	}
	mapperPool.Close()

	// Run reduce tasks
	reducerPool := NewPool(e.config.NumReducers)
	reducerPool.Start()
	for reducerId := 0; reducerId < e.config.NumReducers; reducerId++ {
		reducerPool.Submit(func() {
			log.Printf("Starting reduce task %d", reducerId)
			if err := e.runReduceTask(reducerId, shuffleDir); err != nil {
				log.Printf("Error running reduce task %d: %v", reducerId, err)
			} else {
				log.Printf("Completed reduce task %d", reducerId)
			}
		})
	}
	reducerPool.Close()

	return nil
}

func (e *Engine) runMapTask(mapperId int, shuffleDir, filePath string) error {
	lines, err := ReadLines(filePath)
	if err != nil {
		return err
	}

	// Map input lines to key-value pairs
	var mapped []core.KeyValue
	for _, line := range lines {
		kvs := e.config.Job.Map(fmt.Sprintf("%s:%d", line.Filename, line.Number), line.Text)
		mapped = append(mapped, kvs...)
	}

	// Partition the mapped data.
	var partitioned = make(map[int][]core.KeyValue)
	for _, kv := range mapped {
		partition := core.Partition(kv.Key, e.config.NumReducers)
		partitioned[partition] = append(partitioned[partition], kv)
	}

	// Shuffle is implicitly handled by how the mappers write their outputs and
	// how reducers read it.  Each mapper writes N files, one per reducer, and
	// each reducer reads its input from all mappers.
	shuffleDir = filepath.Join(shuffleDir, fmt.Sprintf("map-%04d", mapperId))
	if err := WritePartitions(shuffleDir, partitioned); err != nil {
		return err
	}

	return nil
}

func (e *Engine) runReduceTask(reducerId int, shuffleDir string) error {
	matches, err := FindFiles(filepath.Join(shuffleDir, "**", fmt.Sprintf("part-%04d.txt", reducerId)))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		log.Printf("Reducer %d: No input files found, producing empty output", reducerId)
		return nil
	}

	// Read and merge all input files for this reducer
	var allRecords []core.KeyValue
	for _, file := range matches {
		records, err := ReadRecords(file)
		if err != nil {
			return err
		}
		allRecords = append(allRecords, records...)
	}
	slices.SortFunc(allRecords, func(left, right core.KeyValue) int {
		return cmp.Compare(left.Key, right.Key)
	})

	// Reduce the merged input
	var results []core.KeyValue
	for i := 0; i < len(allRecords); {
		key := allRecords[i].Key
		values := []string{}

		for i < len(allRecords) && allRecords[i].Key == key {
			values = append(values, allRecords[i].Value)
			i++
		}

		// Streaming reduce
		results = append(results, e.config.Job.Reduce(key, values))
	}

	return WritePartitions(e.config.Output, map[int][]core.KeyValue{reducerId: results})
}
