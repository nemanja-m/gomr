package main

import (
	"log"

	"github.com/nemanja-m/gomr/examples/wordcount"
	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/local"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	config := core.JobConfig{
		Input:       "examples/data/input/*.txt",
		Output:      "examples/data/output/",
		NumReducers: 4,
		MapFunc:     wordcount.Map,
		ReduceFunc:  wordcount.Reduce,
	}

	engine := local.NewEngine(config)

	log.Printf(
		"Starting job with input: %s, output: %s, numReducers: %d",
		config.Input,
		config.Output,
		config.NumReducers,
	)

	if err := engine.Run(); err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	log.Println("Job completed successfully")
}
