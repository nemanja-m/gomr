# gomr

A MapReduce implementation in Go for parallel data processing.

## Quick Start

```bash
# Build
make build

# Run a job
make run ARGS="-job wordcount -input 'data/**/*.txt' -output output/"

# Run tests
make test
```

## Usage

```bash
./bin/gomr -job <name> -input <pattern> -output <dir> [options]
```

List available jobs

```
./bin/gomr -list
```

### Options

- `-job` - Job name (use `-list` to see available jobs)
- `-input` - Input file glob pattern (supports `**` for recursive matching)
- `-output` - Output directory
- `-mappers` - Number of mapper tasks (default: number of CPUs)
- `-reducers` - Number of reducer tasks (default: 4)
- `-args` - Job-specific configuration (format: `key1=val1,key2=val2`)

### Examples

```bash
# Word count
./bin/gomr -job wordcount -input "data/*.txt" -output output/

# Case-sensitive word count
./bin/gomr -job wordcount -input "data/*.txt" -output output/ -args case-sensitive=true

# Grep for pattern
./bin/gomr -job grep -input "logs/**/*.log" -output matches/ -args pattern=ERROR
```

## Creating Custom Jobs

Implement the `core.Job` interface and register in `init()`:

```go
package myjob

import (
    "github.com/nemanja-m/gomr/pkg/core"
    "github.com/nemanja-m/gomr/pkg/jobs"
)

func init() {
    jobs.Register("myjob", func() core.Job {
        return &MyJob{}
    })
}

type MyJob struct{}

func (j *MyJob) Map(key, value string) []core.KeyValue {
    // Map logic
}

func (j *MyJob) Reduce(key string, values []string) core.KeyValue {
    // Reduce logic
}

// Implement Configure(), Validate(), Name(), Describe()
```

See `examples/wordcount` and `examples/grep` for reference implementations.
