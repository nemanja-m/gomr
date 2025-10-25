# gomr

A MapReduce implementation in Go for parallel data processing.

## Quick Start

```bash
# Build
make build

# Run tests
make test
```

## Local Mode

This version implements local map-reduce with multiprocessing.

### Usage

```bash
./bin/local -job <name> -input <pattern> -output <dir> [options]
```

List available jobs

```
./bin/local -list
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
./bin/local -job wordcount -input "data/*.txt" -output output/

# Case-sensitive word count
./bin/local -job wordcount -input "data/*.txt" -output output/ -args case-sensitive=true

# Grep for pattern
./bin/local -job grep -input "logs/**/*.log" -output matches/ -args pattern=ERROR
```

## Creating Custom Jobs

Implement the `Job` interface and register in `init()`:

```go
package myjob

func init() {
    jobs.Register("myjob", func() Job {
        return &MyJob{}
    })
}

type MyJob struct{}

func (j *MyJob) Map(key, value string) []KeyValue {
    // Map logic
}

func (j *MyJob) Reduce(key string, values []string) KeyValue {
    // Reduce logic
}

// Implement Configure(), Validate(), Name(), Describe()
```

See [wordcount](./local/wordcount.go) and [grep](./local/grep.go) examples for reference implementations.
