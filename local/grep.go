package local

import (
	"fmt"
	"regexp"
	"strings"
)

func init() {
	Register("grep", func() Job {
		return &GrepJob{}
	})
}

type GrepJob struct {
	pattern *regexp.Regexp
}

func (g *GrepJob) Name() string {
	return "grep"
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

func (g *GrepJob) Map(key, line string) []KeyValue {
	match := g.pattern.MatchString(line)
	if match {
		return []KeyValue{{Key: key, Value: line}}
	}
	return nil
}

func (g *GrepJob) Reduce(key string, values []string) KeyValue {
	return KeyValue{Key: key, Value: strings.TrimSpace(values[0])}
}
