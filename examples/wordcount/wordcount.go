package wordcount

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/nemanja-m/gomr/pkg/core"
	"github.com/nemanja-m/gomr/pkg/jobs"
)

const name = "wordcount"

func init() {
	jobs.Register(name, func() core.Job {
		return &WordCountJob{}
	})
}

type WordCountJob struct {
	caseSensitive bool
	pattern       *regexp.Regexp
}

func (wc *WordCountJob) Name() string {
	return name
}

func (wc *WordCountJob) Describe() string {
	return "counts occurrences of each word in the input text"
}

func (wc *WordCountJob) Configure(config map[string]string) error {
	if cs, ok := config["case-sensitive"]; ok && (strings.ToLower(cs) == "true" || cs == "1") {
		wc.caseSensitive = true
	}
	wc.pattern = regexp.MustCompile(`[^\p{L}\p{N}]+`)
	return nil
}

func (wc *WordCountJob) Validate() error {
	return nil
}

func (wc *WordCountJob) Map(_, line string) []core.KeyValue {
	var kvs []core.KeyValue
	for word := range strings.FieldsSeq(line) {
		// Keep alphanumeric UTF-8 characters
		word = wc.pattern.ReplaceAllString(word, "")
		word = strings.TrimSpace(word)

		if word == "" {
			continue
		}

		if !wc.caseSensitive {
			word = strings.ToLower(word)
		}

		kvs = append(kvs, core.KeyValue{Key: word, Value: "1"})
	}
	return kvs
}

func (wc *WordCountJob) Reduce(word string, counts []string) core.KeyValue {
	total := 0
	for _, count := range counts {
		val, _ := strconv.Atoi(count)
		total += val
	}
	return core.KeyValue{Key: word, Value: strconv.Itoa(total)}
}
