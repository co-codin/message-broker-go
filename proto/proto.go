// Package proto defines the binary wire protocol for minibroker.
//
// Every frame on the wire is: [u32 total-len][u8 op][op-specific body].
// Strings are [u16 len][bytes]; payloads are [u32 len][bytes]; offsets are
// u64; partition numbers are u32. All integers are big-endian.
package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Op byte

const (
	OpPub       Op = 0x01
	OpSub       Op = 0x02
	OpUnsub     Op = 0x03
	OpCommit    Op = 0x04
	OpQuit      Op = 0x05
	OpOk        Op = 0x10
	OpErr       Op = 0x11
	OpMsg       Op = 0x20
	OpRebalance Op = 0x21
)

// SUB modes.
const (
	SubHead   byte = 0x00 // body: [u32 partition]
	SubOffset byte = 0x01 // body: [u32 partition][u64 offset]
	SubGroup  byte = 0x02 // body: [string group]  (all assigned partitions)
)

const maxFrameLen = 100 << 20

func WriteFrame(w io.Writer, op Op, body []byte) error {
	total := uint32(1 + len(body))
	var head [5]byte
	binary.BigEndian.PutUint32(head[:4], total)
	head[4] = byte(op)
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func ReadFrame(r io.Reader) (Op, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 || length > maxFrameLen {
		return 0, nil, fmt.Errorf("invalid frame length %d", length)
	}
	frame := make([]byte, length)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, err
	}
	return Op(frame[0]), frame[1:], nil
}

// Builder constructs an op body. Use .Build() to retrieve the bytes.
type Builder struct {
	buf []byte
}

func NewBuilder() *Builder { return &Builder{buf: make([]byte, 0, 64)} }

func (b *Builder) Byte(v byte) *Builder { b.buf = append(b.buf, v); return b }

func (b *Builder) U16(v uint16) *Builder {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], v)
	b.buf = append(b.buf, x[:]...)
	return b
}

func (b *Builder) U32(v uint32) *Builder {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	b.buf = append(b.buf, x[:]...)
	return b
}

func (b *Builder) U64(v uint64) *Builder {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	b.buf = append(b.buf, x[:]...)
	return b
}

func (b *Builder) String(s string) *Builder {
	b.U16(uint16(len(s)))
	b.buf = append(b.buf, s...)
	return b
}

func (b *Builder) Bytes(p []byte) *Builder {
	b.U32(uint32(len(p)))
	b.buf = append(b.buf, p...)
	return b
}

func (b *Builder) Partitions(parts []int32) *Builder {
	b.U32(uint32(len(parts)))
	for _, p := range parts {
		b.U32(uint32(p))
	}
	return b
}

func (b *Builder) Build() []byte { return b.buf }

// Parser reads typed fields out of a body.
type Parser struct {
	buf []byte
	pos int
}

func NewParser(body []byte) *Parser { return &Parser{buf: body} }

var ErrShort = errors.New("body too short")

func (p *Parser) Byte() (byte, error) {
	if p.pos >= len(p.buf) {
		return 0, ErrShort
	}
	v := p.buf[p.pos]
	p.pos++
	return v, nil
}

func (p *Parser) U16() (uint16, error) {
	if p.pos+2 > len(p.buf) {
		return 0, ErrShort
	}
	v := binary.BigEndian.Uint16(p.buf[p.pos : p.pos+2])
	p.pos += 2
	return v, nil
}

func (p *Parser) U32() (uint32, error) {
	if p.pos+4 > len(p.buf) {
		return 0, ErrShort
	}
	v := binary.BigEndian.Uint32(p.buf[p.pos : p.pos+4])
	p.pos += 4
	return v, nil
}

func (p *Parser) U64() (uint64, error) {
	if p.pos+8 > len(p.buf) {
		return 0, ErrShort
	}
	v := binary.BigEndian.Uint64(p.buf[p.pos : p.pos+8])
	p.pos += 8
	return v, nil
}

func (p *Parser) String() (string, error) {
	l, err := p.U16()
	if err != nil {
		return "", err
	}
	if p.pos+int(l) > len(p.buf) {
		return "", ErrShort
	}
	s := string(p.buf[p.pos : p.pos+int(l)])
	p.pos += int(l)
	return s, nil
}

func (p *Parser) Bytes() ([]byte, error) {
	l, err := p.U32()
	if err != nil {
		return nil, err
	}
	if p.pos+int(l) > len(p.buf) {
		return nil, ErrShort
	}
	out := make([]byte, l)
	copy(out, p.buf[p.pos:p.pos+int(l)])
	p.pos += int(l)
	return out, nil
}

func (p *Parser) Partitions() ([]int32, error) {
	n, err := p.U32()
	if err != nil {
		return nil, err
	}
	out := make([]int32, n)
	for i := range out {
		v, err := p.U32()
		if err != nil {
			return nil, err
		}
		out[i] = int32(v)
	}
	return out, nil
}
