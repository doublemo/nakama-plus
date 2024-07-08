// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package bytes

import (
	"errors"
	"math"
)

const (
	// Packet_MAXLIMITT 最大封包长度限制
	Packet_MAXLIMITT = 65535
)

// BytesBuffer  封包处理
type BytesBuffer struct {
	// pos 包位置指针
	pos int

	// data 数据
	data []byte
}

// Data 获取所有数据
func (b *BytesBuffer) Data() []byte {
	if b.data == nil {
		b.data = make([]byte, 0)
	}

	return b.data
}

// Bytes 获取当前指定所指位置到最后的数量
func (b *BytesBuffer) Bytes() []byte {
	if b.data == nil || b.pos > len(b.data) {
		return make([]byte, 0)
	}

	return b.data[b.pos:]
}

// Len 获取二进制数据长度
func (b *BytesBuffer) Len() int {
	if b.data == nil {
		return 0
	}

	return len(b.data)
}

// ReadByte 读取一个字节
func (b *BytesBuffer) ReadByte() (m byte, err error) {
	if b.data == nil || b.pos >= len(b.data) {
		err = errors.New("BytesBuffer: out of range")
		return
	}

	m = b.data[b.pos]
	b.pos++
	return
}

// ReadBytes 读取指定长度字节
func (b *BytesBuffer) ReadBytes(size int) (m []byte, err error) {
	if b.data == nil || (b.pos+size) > len(b.data) {
		err = errors.New("BytesBuffer: out of range")
		return
	}

	m = b.data[b.pos : b.pos+size]
	b.pos += size
	return
}

// ReadBool 读取一个布尔
func (b *BytesBuffer) ReadBool() (bool, error) {
	m, err := b.ReadByte()
	if err != nil {
		return false, err
	}

	return m == byte(1), nil
}

// ReadString 读取字符串
func (b *BytesBuffer) ReadString() (string, error) {
	bytes, err := b.ReadUint16Bytes()
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

// ReadInt8 读取int8
func (b *BytesBuffer) ReadInt8() (int8, error) {
	m, err := b.ReadUint8()
	return int8(m), err
}

// ReadUint8 读取uint8
func (b *BytesBuffer) ReadUint8() (uint8, error) {
	m, err := b.ReadByte()
	if err != nil {
		return 0, err
	}

	return uint8(m), nil
}

// ReadInt16 读取int16
func (b *BytesBuffer) ReadInt16() (int16, error) {
	m, err := b.ReadUint16()
	return int16(m), err
}

// ReadUint16 读取uint16
func (b *BytesBuffer) ReadUint16() (uint16, error) {
	bytes, err := b.ReadBytes(2)
	if err != nil {
		return 0, err
	}

	return uint16(bytes[0])<<8 | uint16(bytes[1]), nil
}

// ReadInt24 读取int24
func (b *BytesBuffer) ReadInt24() (int32, error) {
	m, err := b.ReadUint24()
	return int32(m), err
}

// ReadUint24 读取uint24
func (b *BytesBuffer) ReadUint24() (uint32, error) {
	bytes, err := b.ReadBytes(3)
	if err != nil {
		return 0, err
	}
	return uint32(bytes[0])<<16 | uint32(bytes[1])<<8 | uint32(bytes[2]), nil
}

// ReadInt32 读取int32
func (b *BytesBuffer) ReadInt32() (int32, error) {
	m, err := b.ReadUint32()
	return int32(m), err
}

// ReadUint32 读取uint32
func (b *BytesBuffer) ReadUint32() (uint32, error) {
	bytes, err := b.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return uint32(bytes[0])<<24 | uint32(bytes[1])<<16 | uint32(bytes[2])<<8 | uint32(bytes[3]), nil
}

// ReadInt64 读取int64
func (b *BytesBuffer) ReadInt64() (int64, error) {
	m, err := b.ReadUint64()
	return int64(m), err
}

// ReadUint64 读取uint64
func (b *BytesBuffer) ReadUint64() (uint64, error) {
	bytes, err := b.ReadBytes(8)
	if err != nil {
		return 0, err
	}

	var m uint64
	for i, v := range bytes {
		m |= uint64(v) << uint((7-i)*8)
	}

	return m, nil
}

// ReadFloat32 读取float32
func (b *BytesBuffer) ReadFloat32() (float32, error) {
	bits, err := b.ReadUint32()
	if err != nil {
		return 0, err
	}

	m := math.Float32frombits(bits)
	if math.IsNaN(float64(m)) || math.IsInf(float64(m), 0) {
		return 0, nil
	}

	return m, nil
}

// ReadFloat64 读取float64
func (b *BytesBuffer) ReadFloat64() (float64, error) {
	bits, err := b.ReadUint64()
	if err != nil {
		return 0, err
	}

	m := math.Float64frombits(bits)
	if math.IsNaN(m) || math.IsInf(m, 0) {
		return 0, nil
	}

	return m, nil
}

// ReadUint16Bytes 读取uint16长度的数据
func (b *BytesBuffer) ReadUint16Bytes() ([]byte, error) {
	size, err := b.ReadUint16()
	if err != nil {
		return nil, err
	}

	if b.pos+int(size) > len(b.data) {
		b.pos -= 2
		return nil, errors.New("BytesBuffer: out of range")
	}

	bytes := b.data[b.pos : b.pos+int(size)]
	b.pos += int(size)
	return bytes, nil
}

// WriteBytes 写入数量
func (b *BytesBuffer) WriteBytes(bytes ...byte) error {
	if b.data == nil {
		b.data = make([]byte, 0)
	}

	dataSize := len(b.data)
	byteSize := len(bytes)
	if dataSize+byteSize > Packet_MAXLIMITT {
		return errors.New("BytesBuffer: limit on 65535 bytes is exceeded")
	}

	b.data = append(b.data, bytes...)
	return nil
}

func (b *BytesBuffer) WriteBytesV2(bytes []byte) error {
	if b.data == nil {
		b.data = make([]byte, 0)
	}

	dataSize := len(b.data)
	byteSize := len(bytes)
	if dataSize+byteSize > Packet_MAXLIMITT {
		return errors.New("BytesBuffer: limit on 65535 bytes is exceeded")
	}

	newData := make([]byte, dataSize+byteSize)
	copy(newData[0:], b.data[0:])
	copy(newData[dataSize:], bytes[0:])
	b.data = newData
	return nil
}

// WriteZeros 写入指定数量的0
func (b *BytesBuffer) WriteZeros(n int) error {
	return b.WriteBytes(make([]byte, n)...)
}

// WriteBool 写入一个布尔
func (b *BytesBuffer) WriteBool(v bool) error {
	if v {
		return b.WriteBytes(byte(1))
	}

	return b.WriteBytes(byte(0))
}

// WriteByte 写入一个字符
func (b *BytesBuffer) WriteByte(v byte) error {
	return b.WriteBytes(v)
}

// WriteString 写入字符串
func (b *BytesBuffer) WriteString(v string) error {
	return b.WriteUint16Bytes([]byte(v))
}

// WriteInt8 int8
func (b *BytesBuffer) WriteInt8(v int8) error {
	return b.WriteUint8(uint8(v))
}

// WriteUint8 uint8
func (b *BytesBuffer) WriteUint8(v uint8) error {
	return b.WriteBytes(byte(v))
}

// WriteInt16 int16
func (b *BytesBuffer) WriteInt16(v int16) error {
	return b.WriteUint16(uint16(v))
}

// WriteUint16 uint16
func (b *BytesBuffer) WriteUint16(v uint16) error {
	return b.WriteBytes(byte(v>>8), byte(v))
}

// WriteInt24 int24
func (b *BytesBuffer) WriteInt24(v int32) error {
	return b.WriteUint24(uint32(v))
}

// WriteUint24 uint24
func (b *BytesBuffer) WriteUint24(v uint32) error {
	return b.WriteBytes(byte(v>>16), byte(v>>8), byte(v))
}

// WriteInt32 int32
func (b *BytesBuffer) WriteInt32(v int32) error {
	return b.WriteUint32(uint32(v))
}

// WriteUint32 uint32
func (b *BytesBuffer) WriteUint32(v uint32) error {
	return b.WriteBytes(byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteInt64 int64
func (b *BytesBuffer) WriteInt64(v int64) error {
	return b.WriteUint64(uint64(v))
}

// WriteUint64 uint64
func (b *BytesBuffer) WriteUint64(v uint64) error {
	return b.WriteBytes(byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteFloat32 float32
func (b *BytesBuffer) WriteFloat32(f float32) error {
	return b.WriteUint32(math.Float32bits(f))
}

// WriteFloat64 float64
func (b *BytesBuffer) WriteFloat64(f float64) error {
	return b.WriteUint64(math.Float64bits(f))
}

// WriteUint16Bytes 写入带有长度为uint16的二进制
func (b *BytesBuffer) WriteUint16Bytes(bytes []byte) error {
	if b.data == nil {
		b.data = make([]byte, 0)
	}

	if len(b.data)+2+len(bytes) > Packet_MAXLIMITT {
		return errors.New("BytesBuffer: limit on 65535 bytes is exceeded")
	}

	b.WriteUint16(uint16(len(bytes)))
	return b.WriteBytes(bytes...)
}

// Reset 重置
func (b *BytesBuffer) Reset() {
	b.pos = 0
	b.data = make([]byte, 0)
}

// ResetPos 重置pos的位置
func (b *BytesBuffer) ResetPos() {
	b.pos = 0
}

// NewBytesBuffer 创建
func NewBytesBuffer(data []byte) *BytesBuffer {
	if data == nil {
		data = make([]byte, 0)
	}

	return &BytesBuffer{
		data: data,
		pos:  0,
	}
}
