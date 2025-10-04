package local

import (
	"bufio"
	"os"
)

const (
	DefaultBufferSize = 1024 * 1024 // 1MB
)

type Line struct {
	Filename string
	Number   int
	Text     string
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

func WriteLines(filePath string, lines []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}
