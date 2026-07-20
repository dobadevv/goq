package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	env, err := Encode(TypePublish, Publish{Topic: "emails", Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, env); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != TypePublish {
		t.Errorf("Type = %q, want %q", got.Type, TypePublish)
	}
	var p Publish
	if err := got.Decode(&p); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if p.Topic != "emails" || string(p.Payload) != "hello" {
		t.Errorf("payload = %+v, want {emails hello}", p)
	}
}

func TestReadFramePartialReads(t *testing.T) {
	env, _ := Encode(TypeAck, Ack{MessageID: "m1"})
	var buf bytes.Buffer
	_ = WriteFrame(&buf, env)
	// oneByteReader forces ReadFrame to reassemble across many short reads.
	got, err := ReadFrame(iotest_oneByteReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != TypeAck {
		t.Errorf("Type = %q, want %q", got.Type, TypeAck)
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, MaxFrameSize+1)
	buf.Write(hdr)
	if _, err := ReadFrame(&buf); err != ErrFrameTooLarge {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestWriteFrameRejectsOversize(t *testing.T) {
	big := make([]byte, MaxFrameSize)
	env, _ := Encode(TypePublish, Publish{Topic: "t", Payload: big})
	if err := WriteFrame(io.Discard, env); err != ErrFrameTooLarge {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestConnectPayloadRoundTrip(t *testing.T) {
	env, err := Encode(TypeConnect, Connect{Role: "producer", ClientID: "p1", Username: "alice", Password: "s3cret"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, env); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	var c Connect
	if err := got.Decode(&c); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := Connect{Role: "producer", ClientID: "p1", Username: "alice", Password: "s3cret"}
	if c != want {
		t.Errorf("Connect = %+v, want %+v", c, want)
	}
}

func iotest_oneByteReader(b []byte) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		for _, c := range b {
			_, _ = pw.Write([]byte{c})
		}
		_ = pw.Close()
	}()
	return pr
}
