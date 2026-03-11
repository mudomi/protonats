package gen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

func TestGetServiceOptions_Nil(t *testing.T) {
	assert.Nil(t, GetServiceOptions(nil))
}

func TestGetServiceOptions_NoExtension(t *testing.T) {
	assert.Nil(t, GetServiceOptions(&descriptorpb.ServiceOptions{}))
}

func TestGetServiceOptions_WithSubjectPrefix(t *testing.T) {
	inner := encodeString(1, "orders.v1")
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "orders.v1", result.SubjectPrefix)
}

func TestGetMethodOptions_NoExtension(t *testing.T) {
	assert.Nil(t, GetMethodOptions(&descriptorpb.MethodOptions{}))
}

func TestGetMethodOptions_Publish(t *testing.T) {
	inner := encodeVarint(1, 1) // PUBLISH
	inner = append(inner, encodeString(2, "events.created")...)
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, MethodTypePublish, result.Type)
	assert.Equal(t, "events.created", result.Subject)
}

func TestGetMethodOptions_JetstreamConsume(t *testing.T) {
	inner := encodeVarint(1, 3) // JETSTREAM_CONSUME
	inner = append(inner, encodeString(3, "ORDER_EVENTS")...)
	inner = append(inner, encodeString(4, "order-processor")...)
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, MethodTypeJetstreamConsume, result.Type)
	assert.Equal(t, "ORDER_EVENTS", result.Stream)
	assert.Equal(t, "order-processor", result.Consumer)
}

func TestGetFieldOptions_SubjectToken(t *testing.T) {
	inner := encodeVarint(1, 1) // subject_token = true
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.FieldOptions{}, ext).(*descriptorpb.FieldOptions)
	result := GetFieldOptions(opts)
	require.NotNil(t, result)
	assert.True(t, result.SubjectToken)
}

func TestWireFormat_Roundtrip(t *testing.T) {
	data := encodeString(1, "hello")
	data = append(data, encodeVarint(2, 42)...)
	data = append(data, encodeString(3, "world")...)

	fields := map[uint32]interface{}{}
	parseWireFormat(data, func(num uint32, wt int, d []byte) {
		if wt == 2 {
			fields[num] = string(d)
		} else {
			fields[num] = d[0]
		}
	})

	assert.Equal(t, "hello", fields[uint32(1)])
	assert.Equal(t, byte(42), fields[uint32(2)])
	assert.Equal(t, "world", fields[uint32(3)])
}

// ── Helpers ──

// roundTrip marshals a proto message, appends extra bytes, and unmarshals back.
func roundTrip(t *testing.T, msg proto.Message, extra []byte) proto.Message {
	t.Helper()
	b, err := proto.Marshal(msg)
	require.NoError(t, err)
	b = append(b, extra...)

	out := proto.Clone(msg)
	proto.Reset(out)
	require.NoError(t, proto.Unmarshal(b, out))
	return out
}

func encodeVarint(fieldNum uint32, val uint64) []byte {
	tag := uint64(fieldNum<<3) | 0 // wire type 0
	var out []byte
	out = appendUvarint(out, tag)
	out = appendUvarint(out, val)
	return out
}

func encodeString(fieldNum uint32, s string) []byte {
	tag := uint64(fieldNum<<3) | 2 // wire type 2
	var out []byte
	out = appendUvarint(out, tag)
	out = appendUvarint(out, uint64(len(s)))
	out = append(out, s...)
	return out
}

func encodeLengthDelimited(fieldNum uint32, data []byte) []byte {
	tag := uint64(fieldNum<<3) | 2
	var out []byte
	out = appendUvarint(out, tag)
	out = appendUvarint(out, uint64(len(data)))
	out = append(out, data...)
	return out
}

func appendUvarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}
