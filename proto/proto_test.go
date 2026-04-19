package proto

import (
	"bytes"
	"testing"
)

func TestBuilderParserRoundTrip(t *testing.T) {
	body := NewBuilder().
		Byte(0x42).
		U16(12345).
		U32(1_000_000).
		U64(1 << 40).
		String("hello").
		Bytes([]byte{0x00, 0xff, 'x', '\n'}).
		Partitions([]int32{0, 3, 7}).
		Build()

	p := NewParser(body)

	if got, err := p.Byte(); err != nil || got != 0x42 {
		t.Fatalf("Byte: got 0x%02x err=%v", got, err)
	}
	if got, err := p.U16(); err != nil || got != 12345 {
		t.Fatalf("U16: got %d err=%v", got, err)
	}
	if got, err := p.U32(); err != nil || got != 1_000_000 {
		t.Fatalf("U32: got %d err=%v", got, err)
	}
	if got, err := p.U64(); err != nil || got != 1<<40 {
		t.Fatalf("U64: got %d err=%v", got, err)
	}
	if got, err := p.String(); err != nil || got != "hello" {
		t.Fatalf("String: got %q err=%v", got, err)
	}
	if got, err := p.Bytes(); err != nil || !bytes.Equal(got, []byte{0x00, 0xff, 'x', '\n'}) {
		t.Fatalf("Bytes: got %x err=%v", got, err)
	}
	if got, err := p.Partitions(); err != nil || !equalInts32(got, []int32{0, 3, 7}) {
		t.Fatalf("Partitions: got %v err=%v", got, err)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	body := NewBuilder().String("topic-a").U64(42).Bytes([]byte("payload\nwith\nnewlines")).Build()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, OpMsg, body); err != nil {
		t.Fatal(err)
	}
	op, got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpMsg {
		t.Fatalf("op=0x%02x, want OpMsg", byte(op))
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch\n got %x\nwant %x", got, body)
	}
}

func TestFrameEmptyBody(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, OpOk, nil); err != nil {
		t.Fatal(err)
	}
	op, body, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpOk {
		t.Fatalf("op=0x%02x, want OpOk", byte(op))
	}
	if len(body) != 0 {
		t.Fatalf("body len=%d, want 0", len(body))
	}
}

func TestParserShort(t *testing.T) {
	p := NewParser([]byte{0x01})
	if _, err := p.U16(); err != ErrShort {
		t.Fatalf("U16 on short: got %v, want ErrShort", err)
	}

	p = NewParser([]byte{0x00, 0x05, 'a', 'b'}) // string claims len=5 but only 2 bytes follow
	if _, err := p.String(); err != ErrShort {
		t.Fatalf("String past end: got %v, want ErrShort", err)
	}
}

func equalInts32(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
