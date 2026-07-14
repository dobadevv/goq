package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// MaxFrameSize bounds a single JSON frame body to guard against oversized
// allocations from malformed or hostile input.
const MaxFrameSize = 1 << 20 // 1 MiB

// ErrFrameTooLarge is returned when a frame body would exceed MaxFrameSize.
var ErrFrameTooLarge = errors.New("protocol: frame exceeds max size")

// WriteFrame marshals e and writes a 4-byte big-endian length prefix followed
// by the JSON body.
func WriteFrame(w io.Writer, e Envelope) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(body) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// ReadFrame reads one length-prefixed JSON frame and unmarshals it.
func ReadFrame(r io.Reader) (Envelope, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Envelope{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return Envelope{}, ErrFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Envelope{}, err
	}
	var e Envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return Envelope{}, err
	}
	return e, nil
}
