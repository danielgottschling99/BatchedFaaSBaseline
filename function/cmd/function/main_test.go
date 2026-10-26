package main

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"net/http/httptest"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func testJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 20), G: uint8(y * 20), B: 100, A: 255})
		}
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, img, &jpeg.Options{Quality: 85}); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestMessagePackBinaryBatchRoundTrip(t *testing.T) {
	payload := testJPEG(t)
	wire, err := msgpack.Marshal(FunctionBatchRequest{Frames: []BatchedFrame{{FrameID: 7, StreamID: "camera-a", CreatedAt: "now", Payload: payload}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded FunctionBatchRequest
	if err := msgpack.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Frames[0].Payload, payload) {
		t.Fatal("binary JPEG payload changed during MessagePack round trip")
	}
}

func TestHandleBatchReportsMixedResults(t *testing.T) {
	good := testJPEG(t)
	wire, err := msgpack.Marshal(FunctionBatchRequest{Frames: []BatchedFrame{
		{FrameID: 1, StreamID: "camera-a", CreatedAt: "now", Payload: good},
		{FrameID: 2, StreamID: "camera-a", CreatedAt: "now", Payload: []byte("not-a-jpeg")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/", bytes.NewReader(wire))
	res := httptest.NewRecorder()
	handleBatch(res, req)
	if res.Code != 200 || res.Header().Get("Content-Type") != "application/msgpack" {
		t.Fatalf("unexpected response: status=%d content-type=%q", res.Code, res.Header().Get("Content-Type"))
	}
	var decoded FunctionBatchResponse
	if err := msgpack.Unmarshal(res.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Results) != 2 || decoded.Results[0].ProcessingStatus != "ok" || decoded.Results[1].ProcessingStatus != "jpeg_decode_error" {
		t.Fatalf("unexpected mixed results: %+v", decoded.Results)
	}
	if len(decoded.Results[0].OutputPayload) == 0 {
		t.Fatal("successful result has no binary output payload")
	}
}

func TestHandleBatchRejectsMalformedMessagePack(t *testing.T) {
	req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte("bad")))
	res := httptest.NewRecorder()
	handleBatch(res, req)
	if res.Code != 400 {
		t.Fatalf("expected 400, got %d", res.Code)
	}
}
