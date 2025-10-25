package coordinator

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestLoggingMiddleware(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello, World!"))
	})

	wrapped := LoggingMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check that log was written
	logOutput := buf.String()
	if !strings.Contains(logOutput, "[GET]") {
		t.Error("Expected log to contain method [GET]")
	}
	if !strings.Contains(logOutput, "/test") {
		t.Error("Expected log to contain path /test")
	}
	if !strings.Contains(logOutput, "200") {
		t.Error("Expected log to contain status code 200")
	}
	if !strings.Contains(logOutput, "13 bytes") {
		t.Error("Expected log to contain response size")
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
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer log.SetOutput(os.Stderr)

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			wrapped := LoggingMiddleware(handler)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			w := httptest.NewRecorder()

			wrapped.ServeHTTP(w, req)

			if w.Code != tt.statusCode {
				t.Errorf("Expected status %d, got %d", tt.statusCode, w.Code)
			}

			logOutput := buf.String()
			if !strings.Contains(logOutput, fmt.Sprintf("%d", tt.statusCode)) {
				t.Errorf("Expected log to contain status code %d, got: %s", tt.statusCode, logOutput)
			}
		})
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	})

	wrapped := RecoveryMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Check that panic was logged
	logOutput := buf.String()
	if !strings.Contains(logOutput, "[PANIC]") {
		t.Error("Expected log to contain [PANIC]")
	}
	if !strings.Contains(logOutput, "something went wrong") {
		t.Error("Expected log to contain panic message")
	}
}

func TestChainMiddleware(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	wrapped := ChainMiddleware(
		handler,
		RecoveryMiddleware,
		LoggingMiddleware,
	)

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check that log was written (logging middleware was applied)
	logOutput := buf.String()
	if !strings.Contains(logOutput, "[POST]") {
		t.Error("Expected log to contain method [POST]")
	}
}

func TestChainMiddlewareWithPanic(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	wrapped := ChainMiddleware(
		handler,
		RecoveryMiddleware,
		LoggingMiddleware,
	)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Panic log should be present - this validates that both middleware work together
	logOutput := buf.String()
	if !strings.Contains(logOutput, "[PANIC]") {
		t.Errorf("Expected log to contain [PANIC], got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "/panic") {
		t.Error("Expected log to contain the request path")
	}
}

func TestResponseWriterCapturesStatusCode(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
	})

	wrapped := LoggingMiddleware(handler)
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

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	wrapped := LoggingMiddleware(handler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	// Should default to 200 OK
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "200") {
		t.Error("Expected log to contain status code 200")
	}
}
