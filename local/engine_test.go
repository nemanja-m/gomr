package local

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// simpleJob counts total words per file (keyed by filename:linenumber but we'll reduce by file)
type simpleJob struct{}

func (j *simpleJob) Map(key, value string) []KeyValue {
	// Count words in the value
	n := 0
	for f := range strings.FieldsSeq(value) {
		if f != "" {
			n++
		}
	}
	fileName, _, _ := strings.Cut(key, ":")
	return []KeyValue{{Key: fileName, Value: strconv.Itoa(n)}}
}

func (j *simpleJob) Reduce(key string, values []string) KeyValue {
	// sum values
	sum := 0
	for _, v := range values {
		i, _ := strconv.Atoi(v)
		sum += i
	}
	return KeyValue{Key: key, Value: strconv.Itoa(sum)}
}

func (j *simpleJob) Configure(map[string]string) error { return nil }
func (j *simpleJob) Validate() error                   { return nil }
func (j *simpleJob) Name() string                      { return "simple" }
func (j *simpleJob) Describe() string                  { return "simple word count" }

func TestEngine_Run_WordCount(t *testing.T) {
	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	outputDir := filepath.Join(tmpDir, "output")
	require.NoError(t, os.MkdirAll(inputDir, 0o755))

	// Create two input files
	f1 := filepath.Join(inputDir, "a.txt")
	f2 := filepath.Join(inputDir, "b.txt")
	require.NoError(t, os.WriteFile(f1, []byte("hello world\nthis is a test\n"), 0o644))
	require.NoError(t, os.WriteFile(f2, []byte("another test\nwith words\n"), 0o644))

	// Create shuffle dir inside tmpDir so cleanup is local
	shuffleRoot := filepath.Join(tmpDir, "shuffle")
	require.NoError(t, os.MkdirAll(shuffleRoot, 0o755))

	cfg := Config{
		Job:         &simpleJob{},
		Input:       filepath.Join(inputDir, "**", "*.txt"),
		Shuffle:     &shuffleRoot,
		Output:      outputDir,
		NumMappers:  2,
		NumReducers: 1,
	}

	e := NewEngine(cfg)

	require.NoError(t, e.Run())

	// Verify reducer output file exists
	outFiles, err := FindFiles(filepath.Join(outputDir, "**", "part-0000.txt"))
	require.NoError(t, err)
	require.NotEmpty(t, outFiles)

	file, err := os.ReadFile(outFiles[0])
	require.NoError(t, err)

	text := string(file)
	require.Contains(t, text, "a.txt 6")
	require.Contains(t, text, "b.txt 4")

	// Cleanup is handled by deferred RemoveAll in Run via mkTemp, but also remove output
	require.NoError(t, os.RemoveAll(outputDir))
}
