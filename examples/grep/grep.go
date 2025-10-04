package grep

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/jobs"
)

const name = "grep"

func init() {
	jobs.Register(name, func() core.Job {
		return &GrepJob{}
	})
}

type GrepJob struct {
	pattern *regexp.Regexp
}

func (g *GrepJob) Name() string {
	return name
}

func (g *GrepJob) Describe() string {
	return "searches for lines matching a specified pattern"
}

func (g *GrepJob) Configure(config map[string]string) error {
	var expr string
	if p, ok := config["pattern"]; ok {
		expr = p
	} else {
		return fmt.Errorf("pattern must be specified in the job configuration")
	}

	if cs, ok := config["case-sensitive"]; ok && (strings.ToLower(cs) == "false" || cs == "0") {
		expr = "(?i)" + expr
	}

	pattern, err := regexp.Compile(expr)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}
	g.pattern = pattern
	return nil
}

func (g *GrepJob) Validate() error {
	if g.pattern == nil {
		return fmt.Errorf("pattern must be specified in the job configuration")
	}
	return nil
}

func (g *GrepJob) Map(key, line string) []core.KeyValue {
	match := g.pattern.MatchString(line)
	if match {
		return []core.KeyValue{{Key: key, Value: line}}
	}
	return nil
}

func (g *GrepJob) Reduce(key string, values []string) core.KeyValue {
	return core.KeyValue{Key: key, Value: strings.TrimSpace(values[0])}
}
