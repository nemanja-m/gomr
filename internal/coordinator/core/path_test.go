package core

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestFindLocalFiles(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Create test directory structure
	//   tmpDir/
	//     file1.txt
	//     file2.txt
	//     subdir/
	//       file3.txt
	//       file4.log
	//     emptydir/
	//     symlink.txt -> file1.txt

	file1 := filepath.Join(tmpDir, "file1.txt")
	file2 := filepath.Join(tmpDir, "file2.txt")
	subdir := filepath.Join(tmpDir, "subdir")
	file3 := filepath.Join(subdir, "file3.txt")
	file4 := filepath.Join(subdir, "file4.log")
	emptydir := filepath.Join(tmpDir, "emptydir")
	symlink := filepath.Join(tmpDir, "symlink.txt")

	// Create files
	if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(subdir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file3, []byte("content3"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file4, []byte("content4"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(emptydir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file1, symlink); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{
			name:     "single file pattern",
			patterns: []string{file1},
			want:     []string{file1},
		},
		{
			name:     "wildcard pattern matching multiple files",
			patterns: []string{filepath.Join(tmpDir, "*.txt")},
			want:     []string{file1, file2},
		},
		{
			name:     "recursive pattern",
			patterns: []string{filepath.Join(tmpDir, "**/*.txt")},
			want:     []string{file1, file2, file3},
		},
		{
			name:     "pattern matching specific extension",
			patterns: []string{filepath.Join(tmpDir, "**/*.log")},
			want:     []string{file4},
		},
		{
			name:     "multiple patterns",
			patterns: []string{file1, file3},
			want:     []string{file1, file3},
		},
		{
			name:     "multiple patterns with wildcards",
			patterns: []string{filepath.Join(tmpDir, "*.txt"), filepath.Join(subdir, "*.log")},
			want:     []string{file1, file2, file4},
		},
		{
			name:     "empty patterns",
			patterns: []string{},
			want:     []string{},
		},
		{
			name:     "pattern with no matches",
			patterns: []string{filepath.Join(tmpDir, "*.nonexistent")},
			want:     []string{},
		},
		{
			name:     "pattern matching all files recursively",
			patterns: []string{filepath.Join(tmpDir, "**/*")},
			want:     []string{file1, file2, file3, file4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FindLocalFiles(tt.patterns)
			if err != nil {
				t.Fatalf("FindLocalFiles() error = %v", err)
			}

			// Sort both slices for comparison
			if len(got) != len(tt.want) {
				t.Errorf("FindLocalFiles() got %d files, want %d files", len(got), len(tt.want))
				t.Errorf("got: %v", got)
				t.Errorf("want: %v", tt.want)
				return
			}

			// Convert to map for easier comparison (order doesn't matter)
			gotMap := make(map[string]bool)
			for _, f := range got {
				gotMap[f] = true
			}
			wantMap := make(map[string]bool)
			for _, f := range tt.want {
				wantMap[f] = true
			}

			for f := range wantMap {
				if !gotMap[f] {
					t.Errorf("FindLocalFiles() missing expected file: %v", f)
				}
			}
			for f := range gotMap {
				if !wantMap[f] {
					t.Errorf("FindLocalFiles() got unexpected file: %v", f)
				}
			}
		})
	}
}

func TestFindLocalFiles_ExcludesDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a directory that matches the pattern
	dir := filepath.Join(tmpDir, "test.txt")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a regular file that matches the pattern
	file := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(file, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := FindLocalFiles([]string{filepath.Join(tmpDir, "*.txt")})
	if err != nil {
		t.Fatalf("FindLocalFiles() error = %v", err)
	}

	// Should only return the file, not the directory
	if len(got) != 1 {
		t.Errorf("FindLocalFiles() got %d files, want 1", len(got))
		return
	}
	if got[0] != file {
		t.Errorf("FindLocalFiles() got %v, want %v", got[0], file)
	}
}

func TestFindLocalFiles_ExcludesSymlinks(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a regular file
	file := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(file, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a symlink
	symlink := filepath.Join(tmpDir, "link.txt")
	if err := os.Symlink(file, symlink); err != nil {
		t.Fatal(err)
	}

	got, err := FindLocalFiles([]string{filepath.Join(tmpDir, "*.txt")})
	if err != nil {
		t.Fatalf("FindLocalFiles() error = %v", err)
	}

	// Should only return the regular file, not the symlink
	if len(got) != 1 {
		t.Errorf("FindLocalFiles() got %d files, want 1", len(got))
		return
	}
	if got[0] != file {
		t.Errorf("FindLocalFiles() got %v, want %v", got[0], file)
	}
}

func TestFindLocalFiles_InvalidPattern(t *testing.T) {
	// Invalid glob pattern with unmatched bracket
	_, err := FindLocalFiles([]string{"[invalid"})
	if err == nil {
		t.Error("FindLocalFiles() expected error for invalid pattern, got nil")
	}
}

func TestCreateLocalShuffleDir(t *testing.T) {
	jobID := uuid.New()

	dir, err := CreateLocalShuffleDir(jobID)
	if err != nil {
		t.Fatalf("CreateLocalShuffleDir() error = %v", err)
	}
	defer os.RemoveAll(dir)

	// Verify directory was created
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("CreateLocalShuffleDir() did not create a directory")
	}

	// Verify directory name contains job ID
	if !filepath.IsAbs(dir) {
		t.Error("CreateLocalShuffleDir() did not return absolute path")
	}
	baseName := filepath.Base(dir)
	expectedPrefix := "gomr-shuffle-job-" + jobID.String()
	if len(baseName) < len(expectedPrefix) || baseName[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("CreateLocalShuffleDir() directory name = %v, want prefix %v", baseName, expectedPrefix)
	}
}

func TestCreateLocalShuffleDir_UniqueDirectories(t *testing.T) {
	jobID := uuid.New()

	dir1, err := CreateLocalShuffleDir(jobID)
	if err != nil {
		t.Fatalf("CreateLocalShuffleDir() first call error = %v", err)
	}
	defer os.RemoveAll(dir1)

	dir2, err := CreateLocalShuffleDir(jobID)
	if err != nil {
		t.Fatalf("CreateLocalShuffleDir() second call error = %v", err)
	}
	defer os.RemoveAll(dir2)

	// Even with the same job ID, directories should be unique (due to temp suffix)
	if dir1 == dir2 {
		t.Error("CreateLocalShuffleDir() should create unique directories for each call")
	}

	// Both should exist
	if _, err := os.Stat(dir1); err != nil {
		t.Errorf("first directory doesn't exist: %v", err)
	}
	if _, err := os.Stat(dir2); err != nil {
		t.Errorf("second directory doesn't exist: %v", err)
	}
}

func TestCreateLocalShuffleDir_Writable(t *testing.T) {
	jobID := uuid.New()

	dir, err := CreateLocalShuffleDir(jobID)
	if err != nil {
		t.Fatalf("CreateLocalShuffleDir() error = %v", err)
	}
	defer os.RemoveAll(dir)

	// Verify we can write to the directory
	testFile := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Errorf("cannot write to shuffle directory: %v", err)
	}

	// Verify we can read from the file
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Errorf("cannot read from shuffle directory: %v", err)
	}
	if string(content) != "test" {
		t.Errorf("file content = %v, want %v", string(content), "test")
	}
}
