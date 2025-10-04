package main

import (
	"flag"
	"log"

	"github.com/nemanja-m/gomr/examples/wordcount"
	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/local"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	input := flag.String("input", "", "input files glob pattern")
	output := flag.String("output", "", "output directory")
	reducers := flag.Int("reducers", 4, "number of reducers (overrides default)")
	flag.Parse()

	if *input == "" {
		log.Fatal("Input pattern must be specified using the -input flag")
	}
	if *output == "" {
		log.Fatal("Output directory must be specified using the -output flag")
	}
	if *reducers <= 0 {
		log.Fatal("Number of reducers must be >= 0")
	}

	config := core.JobConfig{
		Input:       *input,
		Output:      *output,
		NumReducers: *reducers,
		MapFunc:     wordcount.Map,
		ReduceFunc:  wordcount.Reduce,
	}

	engine := local.NewEngine(config)

	log.Printf(
		"Starting job with input: %s, output: %s, reducers: %d",
		config.Input,
		config.Output,
		config.NumReducers,
	)

	if err := engine.Run(); err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	log.Println("Job completed successfully")
}
