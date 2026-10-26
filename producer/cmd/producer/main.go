package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const defaultProducerWriteBatchSize = 32

type Config struct {
	InputDir          string
	BootstrapServers  []string
	Topic             string
	StreamID          string
	Pattern           string
	WriteBatchSize    int
	Acks              kafka.RequiredAcks
	BurstSize         int
	StepFPS           []float64
	StepDuration      time.Duration
	Loop              bool
	MaxFrames         int
	StartFrameID      int
	RunID             string
	ExpectedFrames    int
	SourceMetricsOut  string
	stopAfterExpected bool
}

type PreloadedFrame struct {
	Name         string
	Payload      []byte
	PayloadBytes int
}

type SourceMetricsRecord struct {
	Type             string `json:"type"`
	RunID            string `json:"run_id"`
	StartedAt        string `json:"started_at"`
	FirstFrameSentAt string `json:"first_frame_sent_at"`
	FinishedAt       string `json:"finished_at"`
	FramesSent       int    `json:"frames_sent"`
	BytesSent        int64  `json:"bytes_sent"`
	DurationMs       int64  `json:"duration_ms"`
	Interrupted      bool   `json:"interrupted"`
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	frameFiles, err := collectFrameFiles(cfg.InputDir)
	if err != nil {
		log.Fatalf("collect frame files: %v", err)
	}

	preloadStarted := time.Now()
	frames, err := preloadFrames(frameFiles)
	if err != nil {
		log.Fatalf("preload frames: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BootstrapServers...),
		Topic:        cfg.Topic,
		RequiredAcks: cfg.Acks,
		Balancer:     &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := &Producer{
		cfg:         cfg,
		frames:      frames,
		writer:      writer,
		nextFrameID: cfg.StartFrameID,
	}

	log.Printf("preloaded %d frames from %s in %s", len(frames), cfg.InputDir, time.Since(preloadStarted))

	producer.startedAt = time.Now()
	switch cfg.Pattern {
	case "steady":
		err = producer.runSteady(ctx)
	case "burst":
		err = producer.runBurst(ctx)
	case "step":
		err = producer.runStep(ctx)
	default:
		err = fmt.Errorf("unsupported pattern %q", cfg.Pattern)
	}

	interrupted := ctx.Err() != nil || err != nil
	if metricsErr := writeSourceMetrics(cfg.SourceMetricsOut, producer.sourceMetrics(interrupted)); metricsErr != nil {
		log.Printf("write source metrics: %v", metricsErr)
	} else {
		log.Printf("wrote source metrics to %s", cfg.SourceMetricsOut)
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("producer failed: %v", err)
	}
}

type Producer struct {
	cfg         Config
	frames      []PreloadedFrame
	writer      *kafka.Writer
	nextFrameID int
	fileIndex   int
	sentCount   int
	bytesSent   int64
	startedAt   time.Time
	firstSentAt time.Time
}

func parseFlags() (Config, error) {
	var cfg Config
	var bootstrapServers string
	var stepFPS string
	var ackMode string
	expectedFramesDefault, err := envInt("EXPECTED_FRAMES", 300)
	if err != nil {
		return cfg, err
	}
	expectedFramesFromEnv := strings.TrimSpace(os.Getenv("EXPECTED_FRAMES")) != ""

	flag.StringVar(&cfg.InputDir, "input-dir", "", "directory containing frame images")
	flag.StringVar(&bootstrapServers, "bootstrap-servers", "localhost:9092", "comma-separated Kafka bootstrap servers")
	flag.StringVar(&cfg.Topic, "topic", "frames", "Kafka topic for frame messages")
	flag.StringVar(&cfg.StreamID, "stream-id", "camera-a", "stream ID written into each frame message")
	flag.StringVar(&cfg.Pattern, "pattern", "steady", "traffic pattern: steady, burst, step")
	flag.IntVar(&cfg.WriteBatchSize, "write-batch-size", defaultProducerWriteBatchSize, "number of frame messages per Kafka write")
	flag.StringVar(&ackMode, "acks", "all", "Kafka acknowledgment mode: all or one")
	flag.IntVar(&cfg.BurstSize, "burst-size", 100, "number of frames to send in burst mode")
	flag.StringVar(&stepFPS, "step-fps", "10,20,40", "comma-separated FPS levels for step mode")
	flag.DurationVar(&cfg.StepDuration, "step-duration", 30*time.Second, "duration per FPS level in step mode")
	flag.BoolVar(&cfg.Loop, "loop", false, "loop frame files when the directory is exhausted")
	flag.IntVar(&cfg.MaxFrames, "max-frames", 0, "optional hard stop after N emitted frames, 0 means unlimited")
	flag.IntVar(&cfg.StartFrameID, "start-frame-id", 1, "first emitted frame_id")
	flag.StringVar(&cfg.RunID, "run-id", strings.TrimSpace(os.Getenv("RUN_ID")), "benchmark run ID written into emitted frame messages")
	flag.IntVar(&cfg.ExpectedFrames, "expected-frames", expectedFramesDefault, "benchmark frame target; explicit flag or EXPECTED_FRAMES also stops producer at this count")
	flag.StringVar(&cfg.SourceMetricsOut, "source-metrics-out", envString("SOURCE_METRICS_OUTPUT", "e2e-source-metrics.jsonl"), "JSONL file for source-side e2e benchmark summary")
	flag.Parse()

	if strings.TrimSpace(cfg.RunID) == "" {
		cfg.RunID = fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
	}

	expectedFramesFromFlag := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "expected-frames" {
			expectedFramesFromFlag = true
		}
	})
	cfg.stopAfterExpected = expectedFramesFromEnv || expectedFramesFromFlag

	if strings.TrimSpace(cfg.InputDir) == "" {
		return cfg, errors.New("input-dir is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return cfg, errors.New("topic is required")
	}
	if strings.TrimSpace(cfg.StreamID) == "" {
		return cfg, errors.New("stream-id is required")
	}
	cfg.BootstrapServers = splitAndTrim(bootstrapServers)
	if len(cfg.BootstrapServers) == 0 {
		return cfg, errors.New("bootstrap-servers must not be empty")
	}
	if cfg.WriteBatchSize <= 0 {
		return cfg, errors.New("write-batch-size must be positive")
	}
	if cfg.BurstSize <= 0 {
		return cfg, errors.New("burst-size must be positive")
	}
	if cfg.StepDuration <= 0 {
		return cfg, errors.New("step-duration must be positive")
	}
	if cfg.StartFrameID <= 0 {
		return cfg, errors.New("start-frame-id must be positive")
	}
	if cfg.ExpectedFrames <= 0 {
		return cfg, errors.New("expected-frames must be positive")
	}
	if strings.TrimSpace(cfg.SourceMetricsOut) == "" {
		return cfg, errors.New("source-metrics-out must not be empty")
	}

	levels, err := parseStepFPS(stepFPS)
	if err != nil {
		return cfg, err
	}
	cfg.StepFPS = levels

	acks, err := parseAcks(ackMode)
	if err != nil {
		return cfg, err
	}
	cfg.Acks = acks

	switch cfg.Pattern {
	case "steady", "burst", "step":
	default:
		return cfg, errors.New("pattern must be one of: steady, burst, step")
	}

	return cfg, nil
}

func parseStepFPS(raw string) ([]float64, error) {
	parts := splitAndTrim(raw)
	if len(parts) == 0 {
		return nil, errors.New("step-fps must not be empty")
	}

	out := make([]float64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseFloat(part, 64)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid step-fps value %q", part)
		}
		out = append(out, value)
	}
	return out, nil
}

func parseAcks(raw string) (kafka.RequiredAcks, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "all":
		return kafka.RequireAll, nil
	case "one":
		return kafka.RequireOne, nil
	default:
		return kafka.RequireNone, errors.New("acks must be one of: all, one")
	}
}

func collectFrameFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := strings.ToLower(filepath.Ext(entry.Name()))
		switch ext {
		case ".jpg", ".jpeg", ".png":
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}

	slices.Sort(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no .jpg, .jpeg, or .png files found in %s", dir)
	}
	return files, nil
}

func preloadFrames(files []string) ([]PreloadedFrame, error) {
	frames := make([]PreloadedFrame, 0, len(files))
	for _, filePath := range files {
		raw, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read frame file %s: %w", filePath, err)
		}

		frames = append(frames, PreloadedFrame{
			Name:         filepath.Base(filePath),
			Payload:      raw,
			PayloadBytes: len(raw),
		})
	}

	return frames, nil
}

func (p *Producer) runSteady(ctx context.Context) error {
	for {
		if p.shouldStop() {
			return nil
		}
		flushed, err := p.sendNextBatch(ctx, p.cfg.WriteBatchSize)
		if err != nil {
			if errors.Is(err, errDatasetExhausted) {
				return nil
			}
			return err
		}
		if flushed == 0 {
			return nil
		}
	}
}

func (p *Producer) runBurst(ctx context.Context) error {
	remaining := p.cfg.BurstSize
	for remaining > 0 {
		if p.shouldStop() {
			return nil
		}
		batchSize := min(remaining, p.cfg.WriteBatchSize)
		flushed, err := p.sendNextBatch(ctx, batchSize)
		if err != nil {
			if errors.Is(err, errDatasetExhausted) {
				return nil
			}
			return err
		}
		if flushed == 0 {
			return nil
		}
		remaining -= flushed
	}
	return nil
}

func (p *Producer) runStep(ctx context.Context) error {
	for {
		for _, fps := range p.cfg.StepFPS {
			if p.shouldStop() {
				return nil
			}
			if err := p.runStepPhase(ctx, fps, p.cfg.StepDuration); err != nil {
				return err
			}
		}
		if !p.cfg.Loop {
			return nil
		}
	}
}

func (p *Producer) runStepPhase(ctx context.Context, fps float64, duration time.Duration) error {
	interval := time.Duration(float64(time.Second) / fps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if p.shouldStop() {
			return nil
		}
		if _, err := p.sendNextBatch(ctx, 1); err != nil {
			if errors.Is(err, errDatasetExhausted) {
				return nil
			}
			return err
		}
		if p.shouldStop() {
			return nil
		}
		<-ticker.C
	}

	return nil
}

func (p *Producer) sendNextBatch(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	type pendingFrame struct {
		name         string
		payloadBytes int
	}

	messages := make([]kafka.Message, 0, limit)
	frames := make([]pendingFrame, 0, limit)

	for len(messages) < limit && !p.shouldStop() {
		frame, err := p.nextFrame()
		if err != nil {
			if errors.Is(err, errDatasetExhausted) && len(messages) > 0 {
				break
			}
			return 0, err
		}

		frameID := p.nextFrameID + len(messages)
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)

		messages = append(messages, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%d", p.cfg.StreamID, frameID)),
			Value: frame.Payload,
			Headers: []kafka.Header{
				{Key: "frame_id", Value: []byte(strconv.Itoa(frameID))},
				{Key: "stream_id", Value: []byte(p.cfg.StreamID)},
				{Key: "run_id", Value: []byte(p.cfg.RunID)},
				{Key: "created_at", Value: []byte(createdAt)},
				{Key: "payload_bytes", Value: []byte(strconv.Itoa(frame.PayloadBytes))},
			},
		})
		frames = append(frames, pendingFrame{
			name:         frame.Name,
			payloadBytes: frame.PayloadBytes,
		})
	}

	if len(messages) == 0 {
		return 0, nil
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return 0, fmt.Errorf("write kafka messages: %w", err)
	}

	if p.firstSentAt.IsZero() {
		p.firstSentAt = time.Now()
	}

	for _, frame := range frames {
		p.nextFrameID++
		p.sentCount++
		p.bytesSent += int64(frame.payloadBytes)
		if p.sentCount == 1 || p.sentCount%50 == 0 {
			log.Printf("sent %d frames, latest=%s", p.sentCount, frame.name)
		}
	}

	return len(messages), nil
}

func (p *Producer) nextFrame() (PreloadedFrame, error) {
	if len(p.frames) == 0 {
		return PreloadedFrame{}, errors.New("no preloaded frames configured")
	}
	if p.fileIndex >= len(p.frames) {
		if !p.cfg.Loop {
			return PreloadedFrame{}, errDatasetExhausted
		}
		p.fileIndex = 0
	}

	frame := p.frames[p.fileIndex]
	p.fileIndex++
	return frame, nil
}

func (p *Producer) shouldStop() bool {
	if p.cfg.MaxFrames > 0 && p.sentCount >= p.cfg.MaxFrames {
		return true
	}
	return p.cfg.stopAfterExpected && p.sentCount >= p.cfg.ExpectedFrames
}

func (p *Producer) sourceMetrics(interrupted bool) SourceMetricsRecord {
	finishedAt := time.Now()
	firstFrameSentAt := ""
	if !p.firstSentAt.IsZero() {
		firstFrameSentAt = p.firstSentAt.UTC().Format(time.RFC3339Nano)
	}

	return SourceMetricsRecord{
		Type:             "e2e_source",
		RunID:            p.cfg.RunID,
		StartedAt:        p.startedAt.UTC().Format(time.RFC3339Nano),
		FirstFrameSentAt: firstFrameSentAt,
		FinishedAt:       finishedAt.UTC().Format(time.RFC3339Nano),
		FramesSent:       p.sentCount,
		BytesSent:        p.bytesSent,
		DurationMs:       finishedAt.Sub(p.startedAt).Milliseconds(),
		Interrupted:      interrupted,
	}
}

func writeSourceMetrics(path string, record SourceMetricsRecord) error {
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var errDatasetExhausted = errors.New("frame dataset exhausted")
