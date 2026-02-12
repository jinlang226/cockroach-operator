package tracelog

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
)

type traceContextKey struct{}

type traceState struct {
	id          string
	reconcileID string
	namespace   string
	name        string
	generation  int64
	seq         int
	mu          sync.Mutex
}

var (
	defaultTracePath  = "./operator-trace.json"
	fallbackTracePath = "/tmp/operator-trace.json"

	fileOnce   sync.Once
	filePath   string
	fileErr    error
	file       *os.File
	writer     *bufio.Writer
	firstEvent bool
	fileMu     sync.Mutex
	closeOnce  sync.Once
	sigOnce    sync.Once
)

func WithTraceState(ctx context.Context, namespace string, name string, generation int64, reconcileID string) context.Context {
	traceID := fmt.Sprintf("%s/%s-%d", namespace, name, time.Now().UnixNano())
	state := &traceState{
		id:          traceID,
		reconcileID: reconcileID,
		namespace:   namespace,
		name:        name,
		generation:  generation,
	}
	return context.WithValue(ctx, traceContextKey{}, state)
}

func Emit(ctx context.Context, logger logr.Logger, eventType string, details map[string]any) {
	if !enabled() {
		return
	}

	state, ok := ctx.Value(traceContextKey{}).(*traceState)
	if !ok || state == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}

	state.mu.Lock()
	state.seq++
	seq := state.seq
	state.mu.Unlock()

	if _, exists := details["traceId"]; !exists {
		details["traceId"] = state.id
	}
	if _, exists := details["reconcileId"]; !exists {
		details["reconcileId"] = state.reconcileID
	}
	if _, exists := details["stepSeq"]; !exists {
		details["stepSeq"] = seq
	}
	if _, exists := details["generation"]; !exists {
		details["generation"] = state.generation
	}
	if _, exists := details["namespace"]; !exists {
		details["namespace"] = state.namespace
	}
	if _, exists := details["name"]; !exists {
		details["name"] = state.name
	}

	entry := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"eventType": eventType,
		"details":   details,
	}

	logger.Info("trace event", "eventType", eventType, "details", details)
	writeEvent(logger, entry)
}

func enabled() bool {
	v, ok := os.LookupEnv("TRACE_LOG_ENABLED")
	if !ok || strings.TrimSpace(v) == "" {
		// Default to enabled so local runs work without extra env wiring.
		return true
	}

	switch strings.TrimSpace(strings.ToLower(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		// Keep the default behavior for unknown values.
		return true
	}
}

func writeEvent(logger logr.Logger, event map[string]any) {
	ensureFile(logger)
	if file == nil || writer == nil {
		return
	}

	fileMu.Lock()
	defer fileMu.Unlock()

	if !firstEvent {
		if _, err := writer.WriteString(",\n"); err != nil {
			logger.Error(err, "failed to write trace separator", "tracePath", filePath)
			return
		}
	}
	firstEvent = false

	enc := json.NewEncoder(writer)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(event); err != nil {
		logger.Error(err, "failed to write trace event", "tracePath", filePath)
		return
	}
	if err := writer.Flush(); err != nil {
		logger.Error(err, "failed to flush trace file", "tracePath", filePath)
	}
}

func ensureFile(logger logr.Logger) {
	fileOnce.Do(func() {
		filePath = os.Getenv("TRACE_LOG_PATH")
		if filePath == "" {
			filePath = defaultTracePath
		}

		// Truncate per run so we always emit one valid JSON document.
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil && filePath == defaultTracePath {
			// If relative path is not writable in container cwd, fall back to /tmp.
			filePath = fallbackTracePath
			f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		}
		if err != nil {
			fileErr = err
			logger.Error(err, "failed to open trace log file", "tracePath", filePath)
			return
		}
		file = f
		writer = bufio.NewWriter(file)
		firstEvent = true

		if _, err := writer.WriteString("{\"events\":[\n"); err != nil {
			fileErr = err
			logger.Error(err, "failed to initialize trace file", "tracePath", filePath)
			return
		}
		if err := writer.Flush(); err != nil {
			fileErr = err
			logger.Error(err, "failed to flush trace file header", "tracePath", filePath)
			return
		}
		startSignalHandler(logger)
		logger.Info("trace file initialized", "tracePath", filePath)
	})

	if fileErr != nil {
		logger.Error(fileErr, "trace file unavailable", "tracePath", filePath)
	}
}

func startSignalHandler(logger logr.Logger) {
	sigOnce.Do(func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-signals
			Close(logger)
		}()
	})
}

func Close(logger logr.Logger) {
	closeOnce.Do(func() {
		fileMu.Lock()
		defer fileMu.Unlock()

		if writer != nil {
			if _, err := writer.WriteString("]}\n"); err != nil {
				logger.Error(err, "failed to finalize trace file", "tracePath", filePath)
			}
			if err := writer.Flush(); err != nil {
				logger.Error(err, "failed to flush trace file", "tracePath", filePath)
			}
		}
		if file != nil {
			if err := file.Close(); err != nil {
				logger.Error(err, "failed to close trace file", "tracePath", filePath)
			}
		}
	})
}
