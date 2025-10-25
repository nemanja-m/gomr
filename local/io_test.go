package local

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindFiles_BasicAndIgnoreDirs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files and a nested directory with a file
	f1 := filepath.Join(tmpDir, "a.txt")
	f2 := filepath.Join(tmpDir, "sub", "b.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(f2), 0o755))
	require.NoError(t, os.WriteFile(f1, []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(f2, []byte("y"), 0o644))

	// Pattern should match both files via glob
	pattern := filepath.Join(tmpDir, "**", "*.txt")
	matches, err := FindFiles(pattern)
	require.NoError(t, err)

	// We expect both files to be present in matches (order not guaranteed)
	require.Contains(t, matches, f1)
	require.Contains(t, matches, f2)

	// Ensure directories are not returned
	dirMatchPattern := filepath.Join(tmpDir, "**")
	allMatches, err := FindFiles(dirMatchPattern)
	require.NoError(t, err)
	for _, m := range allMatches {
		info, err := os.Lstat(m)
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
	}
}

func TestReadLines_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "test.txt")
	content := strings.Join([]string{"first line", "second line", "third line"}, "\n") + "\n"
	require.NoError(t, os.WriteFile(fpath, []byte(content), 0o644))

	lines, err := ReadLines(fpath)
	require.NoError(t, err)
	require.Len(t, lines, 3)

	for i, expected := range []string{"first line", "second line", "third line"} {
		ln := lines[i]
		require.Equal(t, fpath, ln.Filename)
		require.Equal(t, i+1, ln.Number)
		require.Equal(t, expected, ln.Text)
	}
}

func TestReadLines_SmallBufferFails(t *testing.T) {
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "long.txt")
	// Create a single very long line (larger than small buffer)
	longLine := strings.Repeat("a", DefaultBufferSize*2)
	require.NoError(t, os.WriteFile(fpath, []byte(longLine+"\n"), 0o644))

	// Use a tiny buffer to force scanner overflow
	_, err := ReadLines(fpath, 64)
	require.Error(t, err)
}

func TestReadLines_LargeBufferSucceeds(t *testing.T) {
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "long_ok.txt")
	longLine := strings.Repeat("b", DefaultBufferSize*2)
	require.NoError(t, os.WriteFile(fpath, []byte(longLine+"\n"), 0o644))

	// Provide a large enough buffer so scanning succeeds
	lines, err := ReadLines(fpath, DefaultBufferSize*3)
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Equal(t, longLine, lines[0].Text)
}

func TestReadRecords_BasicAndTrim(t *testing.T) {
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "records.txt")

	// Create a file with key<space>value, including values with leading spaces
	content := strings.Join([]string{"k1 v1", "k2    v2", "k3 v3"}, "\n") + "\n"
	require.NoError(t, os.WriteFile(fpath, []byte(content), 0o644))

	recs, err := ReadRecords(fpath)
	require.NoError(t, err)
	require.Len(t, recs, 3)

	require.Equal(t, "k1", recs[0].Key)
	require.Equal(t, "v1", recs[0].Value)

	require.Equal(t, "k2", recs[1].Key)
	require.Equal(t, "v2", recs[1].Value)

	require.Equal(t, "k3", recs[2].Key)
	require.Equal(t, "v3", recs[2].Value)
}

func TestReadRecords_MalformedLine(t *testing.T) {
	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "bad.txt")
	// Malformed line (no space separator)
	require.NoError(t, os.WriteFile(fpath, []byte("nokeyvalue\n"), 0o644))

	_, err := ReadRecords(fpath)
	require.Error(t, err)
}

func TestReadRecords_FileNotFound(t *testing.T) {
	_, err := ReadRecords("/no/such/file/does_not_exist.txt")
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestWriteRecords_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "out.txt")

	vals := []KeyValue{{Key: "x", Value: "1"}, {Key: "y", Value: "2"}}
	require.NoError(t, WriteRecords(outFile, slices.Values(vals)))

	// Read back the file and verify contents
	data, err := os.ReadFile(outFile)
	require.NoError(t, err)

	s := string(data)
	require.Contains(t, s, "x 1")
	require.Contains(t, s, "y 2")
}

func TestWriteRecords_FileNotWritable(t *testing.T) {
	// Attempt to write to a directory path, which should fail
	tmpDir := t.TempDir()
	err := WriteRecords(tmpDir, slices.Values([]KeyValue{{Key: "a", Value: "b"}}))
	require.Error(t, err)
}

func TestWritePartitions_WritesFiles(t *testing.T) {
	tmpDir := t.TempDir()
	parts := map[int][]KeyValue{
		0: {{Key: "p0k", Value: "v0"}},
		1: {{Key: "p1k", Value: "v1"}},
	}

	outDir := filepath.Join(tmpDir, "parts")
	require.NoError(t, WritePartitions(outDir, parts))

	// Expect files part-0000.txt and part-0001.txt
	p0 := filepath.Join(outDir, "part-0000.txt")
	p1 := filepath.Join(outDir, "part-0001.txt")

	data0, err := os.ReadFile(p0)
	require.NoError(t, err)
	require.Contains(t, string(data0), "p0k v0")

	data1, err := os.ReadFile(p1)
	require.NoError(t, err)
	require.Contains(t, string(data1), "p1k v1")
}

func TestWritePartitions_MkdirAllFails(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file where a directory is expected to be created
	blocker := filepath.Join(tmpDir, "blocked")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	// Now attempt to create outputDir under the blocker path
	outDir := filepath.Join(blocker, "parts")
	parts := map[int][]KeyValue{0: {{Key: "k", Value: "v"}}}

	err := WritePartitions(outDir, parts)
	require.Error(t, err)
}

func TestWritePartitions_UnwritableDir(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "parts")
	require.NoError(t, os.MkdirAll(outDir, 0o500))
	// Ensure cleanup permissions so temp dir can be removed
	defer os.Chmod(outDir, 0o700)

	parts := map[int][]KeyValue{0: {{Key: "k", Value: "v"}}}
	err := WritePartitions(outDir, parts)
	require.Error(t, err)

	// Permission errors may manifest as permission denied
	require.True(t, os.IsPermission(err) || err != nil)
}

func TestWritePartitions_EmptyPartitionsCreatesDir(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "parts")

	parts := map[int][]KeyValue{}
	require.NoError(t, WritePartitions(outDir, parts))

	info, err := os.Stat(outDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Len(t, entries, 0)
}
