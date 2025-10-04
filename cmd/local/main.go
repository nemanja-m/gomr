package main

import (
	"flag"
	"log"

	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/jobs"
	"github.com/nemanja-m/gomr/pkg/local"

	_ "github.com/nemanja-m/gomr/examples/grep"
	_ "github.com/nemanja-m/gomr/examples/wordcount"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		input    = flag.String("input", "", "input files glob pattern")
		output   = flag.String("output", "", "output directory")
		reducers = flag.Int("reducers", 4, "number of reducers (overrides default)")
		jobName  = flag.String("job", "", "job to run (e.g., wordcount, grep)")
	)
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

	job, err := jobs.Get(*jobName)
	if err != nil {
		log.Fatalf("Unknown job: '%s'. Available jobs: %v", *jobName, jobs.List())
	}

	config := core.JobConfig{
		Input:       *input,
		Output:      *output,
		NumReducers: *reducers,
		MapFunc:     job.Map,
		ReduceFunc:  job.Reduce,
	}

	engine := local.NewEngine(config)

	log.Printf(
		"Starting job: %s with input: %s, output: %s, reducers: %d",
		*jobName,
		config.Input,
		config.Output,
		config.NumReducers,
	)

	if err := engine.Run(); err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	log.Println("Job completed successfully")
}
