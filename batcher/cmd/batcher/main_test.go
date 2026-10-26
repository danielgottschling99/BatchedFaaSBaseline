package main

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestFunctionMessagePackBinaryRoundTrip(t *testing.T) {
	input := []byte{0xff, 0xd8, 0x00, 0x01}
	wire, err := msgpack.Marshal(FunctionBatchRequest{Frames: []BatchedFrame{{FrameID: 3, StreamID: "camera-a", CreatedAt: "now", Payload: input}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded FunctionBatchRequest
	if err := msgpack.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Frames[0].Payload, input) {
		t.Fatal("binary input payload changed during round trip")
	}
}

func TestFunctionMessagePackMalformedResponse(t *testing.T) {
	var response FunctionBatchResponse
	if err := msgpack.Unmarshal([]byte{0xc1}, &response); err == nil {
		t.Fatal("expected malformed MessagePack response to fail")
	}
}
