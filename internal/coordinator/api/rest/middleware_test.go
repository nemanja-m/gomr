package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// mockLogger is a test logger that captures log messages
type mockLogger struct {
	mu       sync.Mutex
	messages []string
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		messages: make([]string, 0),
	}
}

func (m *mockLogger) Debug(msg string, args ...any) {
	m.log("DEBUG", msg, args...)
}

func (m *mockLogger) Info(msg string, args ...any) {
	m.log("INFO", msg, args...)
}

func (m *mockLogger) Warn(msg string, args ...any) {
	m.log("WARN", msg, args...)
}

func (m *mockLogger) Error(msg string, args ...any) {
	m.log("ERROR", msg, args...)
}

func (m *mockLogger) Fatal(msg string, args ...any) {
	m.log("FATAL", msg, args...)
}

func (m *mockLogger) log(level, msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Format the message with args
	formatted := fmt.Sprintf("[%s] %s", level, msg)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			formatted += fmt.Sprintf(" %v=%v", args[i], args[i+1])
		}
	}
	m.messages = append(m.messages, formatted)
}

func (m *mockLogger) getOutput() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.Join(m.messages, "\n")
}

func TestLoggingMiddleware(t *testing.T) {
	logger := newMockLogger()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello, World!"))
	})

	wrapped := LoggingMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check that log was written
	logOutput := logger.getOutput()
	if !strings.Contains(logOutput, "GET") {
		t.Error("Expected log to contain method GET")
	}
	if !strings.Contains(logOutput, "/test") {
		t.Error("Expected log to contain path /test")
	}
	if !strings.Contains(logOutput, "200") {
		t.Error("Expected log to contain status code 200")
	}
	if !strings.Contains(logOutput, "HTTP request") {
		t.Error("Expected log to contain 'HTTP request'")
	}
}

func TestLoggingMiddlewareWithDifferentStatusCodes(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"OK", http.StatusOK},
		{"Created", http.StatusCreated},
		{"BadRequest", http.StatusBadRequest},
		{"NotFound", http.StatusNotFound},
		{"InternalServerError", http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := newMockLogger()

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			wrapped := LoggingMiddleware(logger)(handler)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			w := httptest.NewRecorder()

			wrapped.ServeHTTP(w, req)

			if w.Code != tt.statusCode {
				t.Errorf("Expected status %d, got %d", tt.statusCode, w.Code)
			}

			logOutput := logger.getOutput()
			if !strings.Contains(logOutput, fmt.Sprintf("%d", tt.statusCode)) {
				t.Errorf("Expected log to contain status code %d, got: %s", tt.statusCode, logOutput)
			}
		})
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	logger := newMockLogger()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	})

	wrapped := RecoveryMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Check that panic was logged
	logOutput := logger.getOutput()
	if !strings.Contains(logOutput, "ERROR") {
		t.Error("Expected log to contain ERROR level")
	}
	if !strings.Contains(logOutput, "Panic recovered") {
		t.Error("Expected log to contain 'Panic recovered'")
	}
	if !strings.Contains(logOutput, "something went wrong") {
		t.Error("Expected log to contain panic message")
	}
}

func TestChainMiddleware(t *testing.T) {
	logger := newMockLogger()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	wrapped := ChainMiddleware(
		handler,
		RecoveryMiddleware(logger),
		LoggingMiddleware(logger),
	)

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check that log was written (logging middleware was applied)
	logOutput := logger.getOutput()
	if !strings.Contains(logOutput, "POST") {
		t.Error("Expected log to contain method POST")
	}
}

func TestChainMiddlewareWithPanic(t *testing.T) {
	logger := newMockLogger()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	wrapped := ChainMiddleware(
		handler,
		RecoveryMiddleware(logger),
		LoggingMiddleware(logger),
	)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Panic log should be present - this validates that both middleware work together
	logOutput := logger.getOutput()
	if !strings.Contains(logOutput, "ERROR") {
		t.Errorf("Expected log to contain ERROR, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "/panic") {
		t.Error("Expected log to contain the request path")
	}
}

func TestResponseWriterCapturesStatusCode(t *testing.T) {
	logger := newMockLogger()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
	})

	wrapped := LoggingMiddleware(logger)(handler)
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	body := w.Body.String()
	if body != "created" {
		t.Errorf("Expected body 'created', got '%s'", body)
	}
}

func TestResponseWriterDefaultStatusCode(t *testing.T) {
	// Handler that writes without explicitly setting status code
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	logger := newMockLogger()

	wrapped := LoggingMiddleware(logger)(handler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	// Should default to 200 OK
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	logOutput := logger.getOutput()
	if !strings.Contains(logOutput, "200") {
		t.Error("Expected log to contain status code 200")
	}
}
