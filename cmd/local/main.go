package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"

	"github.com/nemanja-m/gomr/local"
)

func main() {
	var (
		jobName     = flag.String("job", "", "Job to run: "+strings.Join(local.List(), ", "))
		input       = flag.String("input", "", "Input files glob pattern")
		output      = flag.String("output", "", "Output directory")
		mappers     = flag.Int("mappers", runtime.NumCPU(), "Number of mappers (overrides default)")
		reducers    = flag.Int("reducers", 4, "Number of reducers (overrides default)")
		jobArgs     = flag.String("args", "", "Job arguments (key1=val1,key2=val2)")
		listJobs    = flag.Bool("list", false, "List available jobs")
		describeJob = flag.String("describe", "", "Describe the specified job")
		help        = flag.Bool("help", false, "Print usage information")
	)
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	if *listJobs {
		fmt.Printf("\n")
		for _, name := range local.List() {
			desc, _ := local.Describe(name)
			fmt.Printf("%s: %s\n", name, desc)
		}
		return
	}

	if *describeJob != "" {
		desc, err := local.Describe(*describeJob)
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
	if *mappers <= 0 {
		log.Fatal("Number of mappers must be >= 0")
	}
	if *reducers <= 0 {
		log.Fatal("Number of reducers must be >= 0")
	}

	job, err := local.Build(*jobName, parseJobArgs(*jobArgs))
	if err != nil {
		log.Fatal(err)
	}

	config := local.Config{
		Job:         job,
		Input:       *input,
		Output:      *output,
		NumMappers:  *mappers,
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
