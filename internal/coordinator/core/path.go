package core

import (
	"os"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/google/uuid"
)

func FindLocalFiles(patterns []string) ([]string, error) {
	var files []string
	for _, pattern := range patterns {
		matches, err := doublestar.FilepathGlob(pattern)
		if err != nil {
			return nil, err
		}
		for _, name := range matches {
			info, err := os.Lstat(name)
			if err != nil {
				continue
			}
			if info.Mode().IsRegular() {
				files = append(files, name)
			}
		}
	}
	return files, nil
}

func CreateLocalShuffleDir(jobID uuid.UUID) (string, error) {
	return os.MkdirTemp("", "gomr-shuffle-job-"+jobID.String())
}
