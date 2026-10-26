package main

import (
	"bytes"
	"image/jpeg"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	_ "image/gif"
	_ "image/png"
)

type BatchedFrame struct {
	FrameID           int    `msgpack:"frame_id"`
	StreamID          string `msgpack:"stream_id"`
	CreatedAt         string `msgpack:"created_at"`
	Payload           []byte `msgpack:"payload"`
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

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleBatch)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("starting function on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("function server failed: %v", err)
	}
}

func handleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FunctionBatchRequest
	if err := msgpack.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid msgpack", http.StatusBadRequest)
		return
	}

	results := make([]FunctionFrameResult, 0, len(req.Frames))
	for _, frame := range req.Frames {
		startedAt := nowUTC()
		status, output := processFrame(frame)
		results = append(results, FunctionFrameResult{
			FrameID:            frame.FrameID,
			StreamID:           frame.StreamID,
			CreatedAt:          frame.CreatedAt,
			FunctionStartedAt:  startedAt,
			FunctionFinishedAt: nowUTC(),
			ProcessingStatus:   status,
			OutputPayload:      output,
		})
	}

	w.Header().Set("Content-Type", "application/msgpack")
	if err := msgpack.NewEncoder(w).Encode(FunctionBatchResponse{Results: results}); err != nil {
		http.Error(w, "encode response failed", http.StatusInternalServerError)
	}
}

func processFrame(frame BatchedFrame) (string, []byte) {
	img, err := jpeg.Decode(bytes.NewReader(frame.Payload))
	if err != nil {
		return "jpeg_decode_error", nil
	}

	mask := segment(img)
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, mask, &jpeg.Options{Quality: 85}); err != nil {
		return "jpeg_encode_error", nil
	}
	return "ok", encoded.Bytes()
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
