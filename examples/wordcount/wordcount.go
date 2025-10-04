package wordcount

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/nemanja-m/gomr/pkg/core"
)

func Map(_, line string) []core.KeyValue {
	var kvs []core.KeyValue
	for _, word := range strings.Split(line, " ") {
		word = strings.ToLower(word)

		// Keep alphanumeric UTF-8 characters
		pattern := regexp.MustCompile(`[^\p{L}\p{N}]+`)
		word = pattern.ReplaceAllString(word, "")
		word = strings.TrimSpace(word)

		if word == "" {
			continue
		}

		kvs = append(kvs, core.KeyValue{Key: word, Value: "1"})
	}
	return kvs
}

func Reduce(word string, counts []string) core.KeyValue {
	total := 0
	for _, count := range counts {
		val, _ := strconv.Atoi(count)
		total += val
	}
	return core.KeyValue{Key: word, Value: strconv.Itoa(total)}
}
