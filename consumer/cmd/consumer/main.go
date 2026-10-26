package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image/jpeg"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	BootstrapServers []string
	Topic            string
	GroupID          string
	RunID            string
	ExpectedFrames   int
	SinkMetricsOut   string
	Implementation   string
}

type OutputResult struct {
	FrameID            int    `json:"frame_id"`
	StreamID           string `json:"stream_id"`
	RunID              string `json:"run_id,omitempty"`
	CreatedAt          string `json:"created_at"`
	InputBytes         int    `json:"input_bytes,omitempty"`
	BatcherReceivedAt  string `json:"batcher_received_at"`
	BatchClosedAt      string `json:"batch_closed_at"`
	FunctionInvokedAt  string `json:"function_invoked_at"`
	FunctionStartedAt  string `json:"function_started_at"`
	FunctionFinishedAt string `json:"function_finished_at"`
	FunctionResponseAt string `json:"function_response_at"`
	ProcessingStatus   string `json:"processing_status"`
}

type SinkMetricsRecord struct {
	Type                      string  `json:"type"`
	RunID                     string  `json:"run_id"`
	Implementation            string  `json:"implementation"`
	StartedAt                 string  `json:"started_at"`
	FirstResultReceivedAt     string  `json:"first_result_received_at"`
	FinishedAt                string  `json:"finished_at"`
	ExpectedFrames            int     `json:"expected_frames"`
	ResultFrames              int     `json:"result_frames"`
	InputBytes                int64   `json:"input_bytes,omitempty"`
	OutputBytes               int64   `json:"output_bytes,omitempty"`
	Errors                    int     `json:"errors"`
	UniqueResultFrames        int     `json:"unique_result_frames"`
	DuplicateFrames           int     `json:"duplicate_frames"`
	MissingFrameIDs           []int   `json:"missing_frame_ids,omitempty"`
	TimeToFirstResultMs       int64   `json:"time_to_first_result_ms"`
	ActivationTriggerAt       string  `json:"activation_trigger_at,omitempty"`
	ActivationToFirstResultMs int64   `json:"activation_to_first_result_ms,omitempty"`
	ActivationTriggerOrigin   string  `json:"activation_trigger_origin,omitempty"`
	CompletionTimeMs          int64   `json:"completion_time_ms"`
	EffectiveFPS              float64 `json:"effective_fps"`
	Success                   bool    `json:"success"`
}

type Consumer struct {
	cfg                   Config
	startedAt             time.Time
	firstResultReceivedAt time.Time
	activationTriggerAt   time.Time
	finishedAt            time.Time
	resultFrames          int
	seenFrameIDs          map[int]bool
	duplicateFrames       int
	inputBytes            int64
	outputBytes           int64
	errors                int
	activeRunID           string
	progressMu            sync.Mutex
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.BootstrapServers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	})
	defer reader.Close()

	consumer := &Consumer{
		cfg:          cfg,
		startedAt:    time.Now(),
		seenFrameIDs: make(map[int]bool),
	}

	runErr := consumer.run(ctx, reader)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Printf("consumer stopped with error: %v", runErr)
	}

	record := consumer.metrics()
	if err := writeSinkMetrics(cfg.SinkMetricsOut, record); err != nil {
		log.Fatalf("write sink metrics: %v", err)
	}
	log.Printf("wrote sink metrics to %s for run_id=%s", cfg.SinkMetricsOut, record.RunID)
	if !record.Success {
		log.Fatalf("benchmark result validation failed: expected=%d unique=%d successful=%d duplicates=%d errors=%d", record.ExpectedFrames, record.UniqueResultFrames, record.ResultFrames, record.DuplicateFrames, record.Errors)
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Fatalf("consumer failed: %v", runErr)
	}
}

func parseFlags() (Config, error) {
	var cfg Config
	var bootstrapServers string

	expectedFramesDefault, err := envInt("EXPECTED_FRAMES", 300)
	if err != nil {
		return cfg, err
	}

	flag.StringVar(&bootstrapServers, "bootstrap-servers", "localhost:9092", "comma-separated Kafka bootstrap servers")
	flag.StringVar(&cfg.Topic, "topic", envString("OUTPUT_TOPIC", "results"), "Kafka topic containing per-frame results")
	flag.StringVar(&cfg.GroupID, "group-id", envString("KAFKA_GROUP_ID", "batch-faas-benchmark-consumer"), "Kafka consumer group ID")
	flag.StringVar(&cfg.RunID, "run-id", strings.TrimSpace(os.Getenv("RUN_ID")), "benchmark run ID to match; empty means adopt the first seen run ID")
	flag.IntVar(&cfg.ExpectedFrames, "expected-frames", expectedFramesDefault, "benchmark result target")
	flag.StringVar(&cfg.SinkMetricsOut, "sink-metrics-out", envString("SINK_METRICS_OUTPUT", "e2e-sink-metrics.jsonl"), "JSONL file for sink-side e2e benchmark summary")
	flag.StringVar(&cfg.Implementation, "implementation", "batch-faas", "implementation label written into sink metrics")
	flag.Parse()

	cfg.BootstrapServers = splitAndTrim(bootstrapServers)
	if len(cfg.BootstrapServers) == 0 {
		return cfg, errors.New("bootstrap-servers must not be empty")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return cfg, errors.New("topic must not be empty")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return cfg, errors.New("group-id must not be empty")
	}
	if cfg.ExpectedFrames <= 0 {
		return cfg, errors.New("expected-frames must be positive")
	}
	if strings.TrimSpace(cfg.SinkMetricsOut) == "" {
		return cfg, errors.New("sink-metrics-out must not be empty")
	}
	if strings.TrimSpace(cfg.Implementation) == "" {
		return cfg, errors.New("implementation must not be empty")
	}

	return cfg, nil
}

func (c *Consumer) run(ctx context.Context, reader *kafka.Reader) error {
	progressTicker := time.NewTicker(60 * time.Second)
	defer progressTicker.Stop()
	progressDone := make(chan struct{})
	progressStopped := make(chan struct{})
	defer func() {
		close(progressDone)
		<-progressStopped
	}()
	go func() {
		defer close(progressStopped)
		for {
			select {
			case <-progressTicker.C:
				c.logProgress()
			case <-progressDone:
				return
			}
		}
	}()

	for {
		c.progressMu.Lock()
		if len(c.seenFrameIDs) >= c.cfg.ExpectedFrames {
			c.finishedAt = time.Now()
			c.progressMu.Unlock()
			return nil
		}
		c.progressMu.Unlock()

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.finishedAt = time.Now()
				return ctx.Err()
			}
			c.finishedAt = time.Now()
			return fmt.Errorf("read kafka message: %w", err)
		}

		receivedAt := time.Now()
		runID := messageHeader(msg, "run_id")
		if !c.matchesRunID(runID) {
			continue
		}

		c.progressMu.Lock()
		if c.firstResultReceivedAt.IsZero() {
			c.firstResultReceivedAt = receivedAt
		}
		// The first input observed by the batcher is carried on every result.
		// Keep the earliest value so this remains correct if Kafka delivers
		// results out of order.
		if batcherReceivedAt := messageHeader(msg, "batcher_received_at"); batcherReceivedAt != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, batcherReceivedAt); err == nil &&
				(c.activationTriggerAt.IsZero() || parsed.Before(c.activationTriggerAt)) {
				c.activationTriggerAt = parsed
			}
		}
		c.progressMu.Unlock()
		frameID, err := messageHeaderInt(msg, "frame_id")
		if err != nil {
			c.progressMu.Lock()
			c.errors++
			c.progressMu.Unlock()
			log.Printf("invalid frame_id header at offset %d: %v", msg.Offset, err)
			continue
		}
		c.progressMu.Lock()
		if c.seenFrameIDs[frameID] {
			c.duplicateFrames++
			c.progressMu.Unlock()
			log.Printf("duplicate result for frame_id=%d run_id=%s", frameID, runID)
			continue
		}
		c.seenFrameIDs[frameID] = true

		c.outputBytes += int64(len(msg.Value))
		inputBytes, _ := messageHeaderInt(msg, "payload_bytes")
		c.inputBytes += int64(inputBytes)
		status := messageHeader(msg, "processing_status")
		c.progressMu.Unlock()
		if status == "ok" {
			if _, err := jpeg.Decode(bytes.NewReader(msg.Value)); err != nil {
				c.progressMu.Lock()
				c.errors++
				c.progressMu.Unlock()
				log.Printf("invalid JPEG result for frame_id=%d: %v", frameID, err)
			} else {
				c.progressMu.Lock()
				c.resultFrames++
				c.progressMu.Unlock()
			}
		} else {
			c.progressMu.Lock()
			c.errors++
			c.progressMu.Unlock()
			log.Printf("error result for frame_id=%d run_id=%s status=%s", frameID, runID, status)
		}

		c.progressMu.Lock()
		resultFrames := c.resultFrames
		activeRunID := c.activeRunID
		uniqueFrames := len(c.seenFrameIDs)
		c.progressMu.Unlock()
		if resultFrames == 1 || resultFrames%50 == 0 {
			log.Printf("received %d successful results for run_id=%s", resultFrames, activeRunID)
		}

		if uniqueFrames >= c.cfg.ExpectedFrames {
			c.progressMu.Lock()
			c.finishedAt = time.Now()
			c.progressMu.Unlock()
			return nil
		}
	}
}

func (c *Consumer) logProgress() {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	log.Printf(
		"progress: unique=%d/%d successful=%d errors=%d duplicates=%d elapsed=%s",
		len(c.seenFrameIDs),
		c.cfg.ExpectedFrames,
		c.resultFrames,
		c.errors,
		c.duplicateFrames,
		time.Since(c.startedAt).Round(time.Second),
	)
}

func messageHeader(msg kafka.Message, key string) string {
	for _, header := range msg.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func messageHeaderInt(msg kafka.Message, key string) (int, error) {
	return strconv.Atoi(messageHeader(msg, key))
}

func (c *Consumer) matchesRunID(runID string) bool {
	if c.cfg.RunID != "" {
		if runID == c.cfg.RunID {
			c.activeRunID = c.cfg.RunID
			return true
		}
		return false
	}
	if c.activeRunID == "" {
		c.activeRunID = runID
		return true
	}
	return runID == c.activeRunID
}

func (c *Consumer) metrics() SinkMetricsRecord {
	if c.finishedAt.IsZero() {
		c.finishedAt = time.Now()
	}

	firstResultReceivedAt := ""
	timeToFirstResultMs := int64(0)
	activationTriggerAt := ""
	activationToFirstResultMs := int64(0)
	if !c.firstResultReceivedAt.IsZero() {
		firstResultReceivedAt = c.firstResultReceivedAt.UTC().Format(time.RFC3339Nano)
		timeToFirstResultMs = c.firstResultReceivedAt.Sub(c.startedAt).Milliseconds()
	}
	if !c.activationTriggerAt.IsZero() {
		activationTriggerAt = c.activationTriggerAt.UTC().Format(time.RFC3339Nano)
		if !c.firstResultReceivedAt.IsZero() {
			activationToFirstResultMs = c.firstResultReceivedAt.Sub(c.activationTriggerAt).Milliseconds()
		}
	}

	completionTimeMs := c.finishedAt.Sub(c.startedAt).Milliseconds()
	effectiveFPS := 0.0
	if completionTimeMs > 0 {
		effectiveFPS = float64(c.resultFrames) / (float64(completionTimeMs) / 1000.0)
	}

	runID := c.cfg.RunID
	if runID == "" {
		runID = c.activeRunID
	}

	missing := make([]int, 0)
	for frameID := 1; frameID <= c.cfg.ExpectedFrames; frameID++ {
		if !c.seenFrameIDs[frameID] {
			missing = append(missing, frameID)
		}
	}
	if len(missing) > 0 {
		log.Printf("missing frame IDs: %v", missing)
	}

	return SinkMetricsRecord{
		Type:                      "e2e_sink",
		RunID:                     runID,
		Implementation:            c.cfg.Implementation,
		StartedAt:                 c.startedAt.UTC().Format(time.RFC3339Nano),
		FirstResultReceivedAt:     firstResultReceivedAt,
		FinishedAt:                c.finishedAt.UTC().Format(time.RFC3339Nano),
		ExpectedFrames:            c.cfg.ExpectedFrames,
		ResultFrames:              c.resultFrames,
		InputBytes:                c.inputBytes,
		OutputBytes:               c.outputBytes,
		Errors:                    c.errors,
		UniqueResultFrames:        len(c.seenFrameIDs),
		DuplicateFrames:           c.duplicateFrames,
		MissingFrameIDs:           missing,
		TimeToFirstResultMs:       timeToFirstResultMs,
		ActivationTriggerAt:       activationTriggerAt,
		ActivationToFirstResultMs: activationToFirstResultMs,
		ActivationTriggerOrigin:   "first batcher input received",
		CompletionTimeMs:          completionTimeMs,
		EffectiveFPS:              effectiveFPS,
		Success:                   len(c.seenFrameIDs) == c.cfg.ExpectedFrames && c.resultFrames == c.cfg.ExpectedFrames && c.errors == 0 && c.duplicateFrames == 0,
	}
}

func writeSinkMetrics(path string, record SinkMetricsRecord) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(record)
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return parsed, nil
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
