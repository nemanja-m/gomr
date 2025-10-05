package local

import (
	"bufio"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/nemanja-m/gomr/pkg/core"
)

const (
	DefaultBufferSize = 1024 * 1024 // 1MB
)

type Line struct {
	Filename string
	Number   int
	Text     string
}

func FindFiles(pattern string) ([]string, error) {
	matches, err := doublestar.FilepathGlob(pattern)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, name := range matches {
		info, err := os.Lstat(name)
		if err != nil {
			continue
		}
		if info.Mode().IsRegular() {
			files = append(files, name)
		}
	}
	return files, nil
}

func ReadLines(filePath string, bufferSize ...int) ([]Line, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if len(bufferSize) == 0 {
		bufferSize = []int{DefaultBufferSize}
	}
	buffer := make([]byte, bufferSize[0])

	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, bufferSize[0])

	var lines []Line
	for i := 1; scanner.Scan(); i++ {
		lines = append(lines, Line{
			Filename: filePath,
			Number:   i,
			Text:     scanner.Text(),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func ReadRecords(filePath string, bufferSize ...int) ([]core.KeyValue, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if len(bufferSize) == 0 {
		bufferSize = []int{DefaultBufferSize}
	}
	buffer := make([]byte, bufferSize[0])

	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, bufferSize[0])

	var records []core.KeyValue

	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok {
			return nil, fmt.Errorf("invalid record format in line: %q", scanner.Text())
		}
		value = strings.TrimLeft(value, " \t")
		records = append(records, core.KeyValue{Key: key, Value: value})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func WritePartitions(outputDir string, partitions map[int][]core.KeyValue) error {
	// Ensure output directory exists
	dir := filepath.Dir(filepath.Join(outputDir, "dummy"))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory tree: %w", err)
	}

	// Write each partition to a separate file
	for part, records := range partitions {
		partFilename := fmt.Sprintf("part-%04d.txt", part)
		outputPath := filepath.Join(outputDir, partFilename)

		if err := WriteRecords(outputPath, slices.Values(records)); err != nil {
			return err
		}
	}

	return nil
}

func WriteRecords(filePath string, records iter.Seq[core.KeyValue]) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for record := range records {
		if _, err := fmt.Fprintf(file, "%s %s\n", record.Key, record.Value); err != nil {
			return err
		}
	}

	return nil
}
