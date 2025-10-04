package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/nemanja-m/gomr/pkg/jobs"
	"github.com/nemanja-m/gomr/pkg/local"

	_ "github.com/nemanja-m/gomr/examples/grep"
	_ "github.com/nemanja-m/gomr/examples/wordcount"
)

func main() {
	var (
		jobName     = flag.String("job", "", "Job to run: "+strings.Join(jobs.List(), ", "))
		input       = flag.String("input", "", "Input files glob pattern")
		output      = flag.String("output", "", "Output directory")
		reducers    = flag.Int("reducers", 4, "Number of reducers (overrides default)")
		jobArgs     = flag.String("args", "", "Job arguments (key1=val1,key2=val2)")
		listJobs    = flag.Bool("list", false, "List available jobs")
		describeJob = flag.String("describe", "", "Describe the specified job")
	)
	flag.Parse()

	if *listJobs {
		fmt.Printf("\n")
		for _, name := range jobs.List() {
			desc, _ := jobs.Describe(name)
			fmt.Printf("%s: %s\n", name, desc)
		}
		return
	}

	if *describeJob != "" {
		desc, err := jobs.Describe(*describeJob)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(desc)
		return
	}

	if *jobName == "" {
		log.Fatal("Job name must be specified using the -job flag")
	}
	if *input == "" {
		log.Fatal("Input pattern must be specified using the -input flag")
	}
	if *output == "" {
		log.Fatal("Output directory must be specified using the -output flag")
	}
	if *reducers <= 0 {
		log.Fatal("Number of reducers must be >= 0")
	}

	job, err := jobs.Build(*jobName, parseJobArgs(*jobArgs))
	if err != nil {
		log.Fatal(err)
	}

	config := local.Config{
		Job:         job,
		Input:       *input,
		Output:      *output,
		NumReducers: *reducers,
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

func parseJobArgs(args string) map[string]string {
	config := make(map[string]string)
	if args == "" {
		return config
	}

	for pair := range strings.SplitSeq(args, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			config[kv[0]] = kv[1]
		}
	}

	return config
}
