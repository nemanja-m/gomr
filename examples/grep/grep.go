package grep

import (
	"strings"

	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/jobs"
)

func init() {
	jobs.Register("grep", jobs.Job{
		Map:    Map,
		Reduce: Reduce,
	})
}

func Map(key, line string) []core.KeyValue {
	if strings.Contains(line, "Gregor") {
		return []core.KeyValue{{Key: key, Value: line}}
	}
	return []core.KeyValue{}
}

func Reduce(key string, values []string) core.KeyValue {
	return core.KeyValue{Key: key, Value: values[0]}
}
