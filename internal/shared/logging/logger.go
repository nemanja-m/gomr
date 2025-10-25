package logging

import (
	"log/slog"
	"os"
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Fatal(msg string, args ...any)
}

type SlogLogger struct {
	log *slog.Logger
}

func NewSlogLogger(level slog.Level) Logger {
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.TimeValue(a.Value.Time().UTC())
			}
			return a
		},
	}
	sl := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	return &SlogLogger{log: sl}
}

func (sl *SlogLogger) Debug(msg string, args ...any) {
	sl.log.Debug(msg, args...)
}

func (sl *SlogLogger) Info(msg string, args ...any) {
	sl.log.Info(msg, args...)
}

func (sl *SlogLogger) Warn(msg string, args ...any) {
	sl.log.Warn(msg, args...)
}

func (sl *SlogLogger) Error(msg string, args ...any) {
	sl.log.Error(msg, args...)
}

func (sl *SlogLogger) Fatal(msg string, args ...any) {
	sl.log.Error(msg, args...)
	os.Exit(1)
}
