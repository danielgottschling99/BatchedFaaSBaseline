package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

type Config struct {
	KafkaBootstrapServers []string
	InputTopic            string
	OutputTopic           string
	OpenFaaSFunctionURL   string
	OpenFaaSUsername      string
	OpenFaaSPassword      string
	BatchSize             int
	MaxWait               time.Duration
	KafkaGroupID          string
	RunID                 string
	HealthAddr            string
	FunctionTimeout       time.Duration
}

type BatchedFrame struct {
	FrameID           int    `msgpack:"frame_id"`
	StreamID          string `msgpack:"stream_id"`
	RunID             string `msgpack:"-"`
	CreatedAt         string `msgpack:"created_at"`
	Payload           []byte `msgpack:"payload"`
	PayloadBytes      int    `msgpack:"-"`
	BatcherReceivedAt string `msgpack:"batcher_received_at"`
}

type FunctionBatchRequest struct {
	Frames []BatchedFrame `msgpack:"frames"`
}

type FunctionFrameResult struct {
	FrameID            int    `msgpack:"frame_id"`
	StreamID           string `msgpack:"stream_id"`
	CreatedAt          string `msgpack:"created_at"`
	FunctionStartedAt  string `msgpack:"function_started_at"`
	FunctionFinishedAt string `msgpack:"function_finished_at"`
	ProcessingStatus   string `msgpack:"processing_status"`
	OutputPayload      []byte `msgpack:"output_payload,omitempty"`
}

type FunctionBatchResponse struct {
	Results []FunctionFrameResult `msgpack:"results"`
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

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	log.Printf(
		"starting batcher: brokers=%s input_topic=%s output_topic=%s group_id=%s batch_size=%d max_wait=%s function_url=%s",
		strings.Join(cfg.KafkaBootstrapServers, ","),
		cfg.InputTopic,
		cfg.OutputTopic,
		cfg.KafkaGroupID,
		cfg.BatchSize,
		cfg.MaxWait,
		cfg.OpenFaaSFunctionURL,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var ready atomic.Bool
	healthServer := startHealthServer(cfg.HealthAddr, &ready, cfg.RunID)
	defer healthServer.Shutdown(context.Background())
	if err := waitForKafka(ctx, cfg); err != nil {
		log.Fatalf("Kafka is not ready: %v (check the advertised listener and topic names)", err)
	}
	ready.Store(true)
	log.Printf("batcher is ready and connected to Kafka")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBootstrapServers,
		Topic:   cfg.InputTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBootstrapServers...),
		Topic:        cfg.OutputTopic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.LeastBytes{},
	}
	defer writer.Close()

	httpClient := &http.Client{Timeout: cfg.FunctionTimeout}
	frameCh := make(chan BatchedFrame)
	errCh := make(chan error, 1)

	go consumeFrames(ctx, reader, frameCh, errCh)

	if err := runBatchLoop(ctx, cfg, httpClient, writer, frameCh, errCh); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("batch loop failed: %v", err)
	}
}

func loadConfig() (Config, error) {
	var cfg Config

	servers := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	if servers == "" {
		return cfg, errors.New("KAFKA_BOOTSTRAP_SERVERS is required")
	}

	batchSize, err := strconv.Atoi(strings.TrimSpace(os.Getenv("BATCH_SIZE")))
	if err != nil || batchSize <= 0 {
		return cfg, errors.New("BATCH_SIZE must be a positive integer")
	}

	maxWaitMs, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MAX_WAIT_MS")))
	if err != nil || maxWaitMs <= 0 {
		return cfg, errors.New("MAX_WAIT_MS must be a positive integer")
	}

	cfg = Config{
		KafkaBootstrapServers: splitAndTrim(servers),
		InputTopic:            requiredEnv("INPUT_TOPIC"),
		OutputTopic:           requiredEnv("OUTPUT_TOPIC"),
		OpenFaaSFunctionURL:   requiredEnv("OPENFAAS_FUNCTION_URL"),
		OpenFaaSUsername:      requiredEnv("OPENFAAS_USERNAME"),
		OpenFaaSPassword:      requiredEnv("OPENFAAS_PASSWORD"),
		BatchSize:             batchSize,
		MaxWait:               time.Duration(maxWaitMs) * time.Millisecond,
		KafkaGroupID:          requiredEnv("KAFKA_GROUP_ID"),
		RunID:                 requiredEnv("RUN_ID"),
		HealthAddr:            envOrDefault("HEALTH_ADDR", ":8090"),
		FunctionTimeout:       time.Duration(envIntOrDefault("FUNCTION_TIMEOUT_SECONDS", 120)) * time.Second,
	}

	if cfg.InputTopic == "" || cfg.OutputTopic == "" || cfg.OpenFaaSFunctionURL == "" || cfg.KafkaGroupID == "" {
		return cfg, errors.New("missing required environment variables")
	}

	return cfg, nil
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func startHealthServer(addr string, ready *atomic.Bool, runID string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "batcher is waiting for Kafka", http.StatusServiceUnavailable)
			return
		}
		if requestedRunID := strings.TrimSpace(r.URL.Query().Get("run_id")); requestedRunID != "" && requestedRunID != runID {
			http.Error(w, "batcher is ready for a different run", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready run_id=" + runID + "\n"))
	})
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server failed: %v", err)
		}
	}()
	return server
}

func waitForKafka(ctx context.Context, cfg Config) error {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(checkCtx, "tcp", cfg.KafkaBootstrapServers[0], cfg.InputTopic, 0)
	if err != nil {
		return err
	}
	return conn.Close()
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

func requiredEnv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func consumeFrames(ctx context.Context, reader *kafka.Reader, frameCh chan<- BatchedFrame, errCh chan<- error) {
	defer close(frameCh)

	receivedCount := 0

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			errCh <- fmt.Errorf("read kafka message: %w", err)
			return
		}

		frameID, err := messageHeaderInt(msg, "frame_id")
		if err != nil {
			log.Printf("skip invalid frame message at offset %d: invalid frame_id: %v", msg.Offset, err)
			continue
		}
		streamID := messageHeader(msg, "stream_id")
		runID := messageHeader(msg, "run_id")
		createdAt := messageHeader(msg, "created_at")
		payloadBytes, err := messageHeaderInt(msg, "payload_bytes")
		if streamID == "" || runID == "" || createdAt == "" || err != nil {
			log.Printf("skip invalid frame message at offset %d: missing or invalid metadata", msg.Offset)
			continue
		}
		if cfgRunID := strings.TrimSpace(os.Getenv("RUN_ID")); cfgRunID != "" && runID != cfgRunID {
			continue
		}

		frame := BatchedFrame{
			FrameID:           frameID,
			StreamID:          streamID,
			RunID:             runID,
			CreatedAt:         createdAt,
			Payload:           msg.Value,
			PayloadBytes:      payloadBytes,
			BatcherReceivedAt: nowUTC(),
		}

		receivedCount++
		if receivedCount == 1 || receivedCount%50 == 0 {
			log.Printf(
				"received %d frames from kafka, latest_offset=%d frame_id=%d stream_id=%s payload_bytes=%d",
				receivedCount,
				msg.Offset,
				frame.FrameID,
				frame.StreamID,
				frame.PayloadBytes,
			)
		}

		select {
		case frameCh <- frame:
		case <-ctx.Done():
			return
		}
	}
}

func runBatchLoop(
	ctx context.Context,
	cfg Config,
	httpClient *http.Client,
	writer *kafka.Writer,
	frameCh <-chan BatchedFrame,
	errCh <-chan error,
) error {
	var (
		batch []BatchedFrame
		timer *time.Timer
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
	}

	flush := func(reason string) error {
		if len(batch) == 0 {
			return nil
		}

		frames := make([]BatchedFrame, len(batch))
		copy(frames, batch)
		batch = batch[:0]
		stopTimer()

		log.Printf(
			"closing batch: reason=%s frames=%d first_frame_id=%d last_frame_id=%d",
			reason,
			len(frames),
			frames[0].FrameID,
			frames[len(frames)-1].FrameID,
		)

		return flushBatch(ctx, cfg, httpClient, writer, frames, reason)
	}

	for {
		var timerCh <-chan time.Time
		if timer != nil {
			timerCh = timer.C
		}

		select {
		case <-ctx.Done():
			return flush("shutdown")
		case err := <-errCh:
			if err != nil {
				return err
			}
		case frame, ok := <-frameCh:
			if !ok {
				return flush("input_closed")
			}

			if len(batch) == 0 {
				timer = time.NewTimer(cfg.MaxWait)
			}

			batch = append(batch, frame)
			if len(batch) >= cfg.BatchSize {
				if err := flush("batch_size"); err != nil {
					return err
				}
			}
		case <-timerCh:
			if err := flush("max_wait"); err != nil {
				return err
			}
		}
	}
}

func flushBatch(
	ctx context.Context,
	cfg Config,
	httpClient *http.Client,
	writer *kafka.Writer,
	frames []BatchedFrame,
	reason string,
) error {
	batchClosedAt := nowUTC()
	functionInvokedAt := nowUTC()
	invokeStarted := time.Now()

	reqBody, err := msgpack.Marshal(FunctionBatchRequest{Frames: frames})
	if err != nil {
		return fmt.Errorf("marshal function request: %w", err)
	}

	log.Printf(
		"invoking function: reason=%s frames=%d request_bytes=%d first_frame_id=%d last_frame_id=%d",
		reason,
		len(frames),
		len(reqBody),
		frames[0].FrameID,
		frames[len(frames)-1].FrameID,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.OpenFaaSFunctionURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("build function request: %w", err)
	}
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("Accept", "application/msgpack")
	if cfg.OpenFaaSUsername != "" {
		req.SetBasicAuth(cfg.OpenFaaSUsername, cfg.OpenFaaSPassword)
	}

	resp, err := httpClient.Do(req)
	functionResponseAt := nowUTC()
	if err != nil {
		log.Printf(
			"function invoke failed: reason=%s frames=%d duration=%s error=%v",
			reason,
			len(frames),
			time.Since(invokeStarted),
			err,
		)
		return publishErrorResults(ctx, writer, frames, batchClosedAt, functionInvokedAt, functionResponseAt, "function_invoke_error")
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf(
			"function returned non-2xx: reason=%s frames=%d duration=%s status=%d",
			reason,
			len(frames),
			time.Since(invokeStarted),
			resp.StatusCode,
		)
		return publishErrorResults(ctx, writer, frames, batchClosedAt, functionInvokedAt, functionResponseAt, fmt.Sprintf("function_http_%d", resp.StatusCode))
	}

	var functionResp FunctionBatchResponse
	if err := msgpack.NewDecoder(resp.Body).Decode(&functionResp); err != nil {
		return publishErrorResults(ctx, writer, frames, batchClosedAt, functionInvokedAt, functionResponseAt, "function_response_decode_error")
	}

	if len(functionResp.Results) != len(frames) {
		log.Printf(
			"function result count mismatch: reason=%s frames=%d results=%d duration=%s",
			reason,
			len(frames),
			len(functionResp.Results),
			time.Since(invokeStarted),
		)
		return publishErrorResults(ctx, writer, frames, batchClosedAt, functionInvokedAt, functionResponseAt, "function_result_count_mismatch")
	}

	resultByFrameID := make(map[string]FunctionFrameResult, len(functionResp.Results))
	for _, result := range functionResp.Results {
		resultByFrameID[resultKey(result.StreamID, result.FrameID)] = result
	}

	messages := make([]kafka.Message, 0, len(frames))
	for _, frame := range frames {
		functionResult, ok := resultByFrameID[resultKey(frame.StreamID, frame.FrameID)]
		if !ok {
			functionResult = FunctionFrameResult{
				FrameID:          frame.FrameID,
				StreamID:         frame.StreamID,
				CreatedAt:        frame.CreatedAt,
				ProcessingStatus: "missing_function_result",
			}
		}

		status := functionResult.ProcessingStatus
		var outputPayload []byte
		if status == "ok" {
			outputPayload = functionResult.OutputPayload
			if len(outputPayload) == 0 {
				status = "function_output_decode_error"
				outputPayload = nil
			}
		}
		messages = append(messages, resultMessage(frame, batchClosedAt, functionInvokedAt,
			functionResult.FunctionStartedAt, functionResult.FunctionFinishedAt,
			functionResponseAt, status, outputPayload))
	}

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("publish results: %w", err)
	}

	log.Printf(
		"flushed batch: reason=%s frames=%d request_bytes=%d duration=%s",
		reason,
		len(frames),
		len(reqBody),
		time.Since(invokeStarted),
	)
	return nil
}

func publishErrorResults(
	ctx context.Context,
	writer *kafka.Writer,
	frames []BatchedFrame,
	batchClosedAt string,
	functionInvokedAt string,
	functionResponseAt string,
	status string,
) error {
	messages := make([]kafka.Message, 0, len(frames))
	for _, frame := range frames {
		messages = append(messages, resultMessage(frame, batchClosedAt, functionInvokedAt,
			"", "", functionResponseAt, status, nil))
	}

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return err
	}

	log.Printf("published error results: status=%s frames=%d", status, len(frames))
	return nil
}

func resultMessage(frame BatchedFrame, batchClosedAt, functionInvokedAt,
	functionStartedAt, functionFinishedAt, functionResponseAt, status string,
	outputPayload []byte) kafka.Message {
	header := func(key, value string) kafka.Header {
		return kafka.Header{Key: key, Value: []byte(value)}
	}
	return kafka.Message{
		Key:   []byte(fmt.Sprintf("%s:%d", frame.StreamID, frame.FrameID)),
		Value: outputPayload,
		Headers: []kafka.Header{
			header("frame_id", strconv.Itoa(frame.FrameID)),
			header("stream_id", frame.StreamID),
			header("run_id", frame.RunID),
			header("created_at", frame.CreatedAt),
			header("payload_bytes", strconv.Itoa(frame.PayloadBytes)),
			header("output_payload_bytes", strconv.Itoa(len(outputPayload))),
			header("batcher_received_at", frame.BatcherReceivedAt),
			header("batch_closed_at", batchClosedAt),
			header("function_invoked_at", functionInvokedAt),
			header("function_started_at", functionStartedAt),
			header("function_finished_at", functionFinishedAt),
			header("function_response_at", functionResponseAt),
			header("processing_status", status),
		},
	}
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
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

func resultKey(streamID string, frameID int) string {
	return fmt.Sprintf("%s:%d", streamID, frameID)
}
