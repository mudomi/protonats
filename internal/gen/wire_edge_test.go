package gen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

func TestConsumeVarint_SingleByteZero(t *testing.T) {
	val, n := consumeVarint([]byte{0x00})
	assert.Equal(t, uint64(0), val)
	assert.Equal(t, 1, n)
}

func TestConsumeVarint_SingleByteMax(t *testing.T) {
	val, n := consumeVarint([]byte{0x7F})
	assert.Equal(t, uint64(127), val)
	assert.Equal(t, 1, n)
}

func TestConsumeVarint_TwoBytes(t *testing.T) {
	// 300 = 0b100101100 → varint: 0xAC 0x02
	val, n := consumeVarint([]byte{0xAC, 0x02})
	assert.Equal(t, uint64(300), val)
	assert.Equal(t, 2, n)
}

func TestConsumeTag_FieldZero(t *testing.T) {
	// Field number 0 is technically invalid but parseable.
	data := []byte{0x00} // field 0, wire type 0
	num, wt, n := consumeTag(data)
	assert.Equal(t, uint32(0), num)
	assert.Equal(t, 0, wt)
	assert.Equal(t, 1, n)
}

func TestConsumeTag_AllWireTypes(t *testing.T) {
	for wt := 0; wt <= 5; wt++ {
		tag := uint64(1<<3) | uint64(wt)
		var data []byte
		data = appendUvarint(data, tag)
		_, gotWt, n := consumeTag(data)
		assert.Greater(t, n, 0)
		assert.Equal(t, wt, gotWt)
	}
}

func TestGetExtensionBytes_TruncatedData(t *testing.T) {
	// Test that truncated extension data is handled gracefully by the parser.
	// We build valid wire bytes directly instead of roundtripping through proto.
	inner := encodeString(1, "data")
	ext := encodeLengthDelimited(50100, inner)
	// Truncate: remove last byte so the length-delimited content is short.
	truncated := ext[:len(ext)-1]

	result := &ServiceOptions{}
	parseWireFormat(truncated, func(fieldNum uint32, wireType int, data []byte) {
		if fieldNum == 1 {
			result.SubjectPrefix = string(data)
		}
	})
	// Parser should bail without crashing.
}

func TestGetExtensionBytes_EmptyExtensionData(t *testing.T) {
	// Extension field 50100 with zero-length data.
	ext := encodeLengthDelimited(50100, []byte{})
	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "", result.SubjectPrefix) // empty data → empty fields
}

func TestParseWireFormat_MultipleOccurrencesOfSameField(t *testing.T) {
	// In proto wire format, last occurrence of a scalar field wins.
	data := encodeString(1, "first")
	data = append(data, encodeString(1, "second")...)

	var values []string
	parseWireFormat(data, func(num uint32, wt int, d []byte) {
		if num == 1 {
			values = append(values, string(d))
		}
	})
	// Both occurrences are seen by the parser.
	assert.Equal(t, []string{"first", "second"}, values)
}

func TestParseWireFormat_InterleavedFieldTypes(t *testing.T) {
	// Mix varint, string, and more varint fields.
	data := encodeVarint(1, 42)
	data = append(data, encodeString(2, "hello")...)
	data = append(data, encodeVarint(3, 99)...)
	data = append(data, encodeString(4, "world")...)
	data = append(data, encodeVarint(1, 7)...) // field 1 again

	fields := map[uint32][]interface{}{}
	parseWireFormat(data, func(num uint32, wt int, d []byte) {
		if wt == 2 {
			fields[num] = append(fields[num], string(d))
		} else {
			fields[num] = append(fields[num], d[0])
		}
	})
	assert.Equal(t, []interface{}{byte(42), byte(7)}, fields[1])
	assert.Equal(t, []interface{}{"hello"}, fields[2])
	assert.Equal(t, []interface{}{byte(99)}, fields[3])
	assert.Equal(t, []interface{}{"world"}, fields[4])
}

func TestGetServiceOptions_EmptyPrefix(t *testing.T) {
	// Explicitly set subject_prefix to empty string.
	inner := encodeString(1, "")
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "", result.SubjectPrefix)
}

func TestGetMethodOptions_RequestReplyExplicit(t *testing.T) {
	// Explicitly setting type to 0 (REQUEST_REPLY).
	inner := encodeVarint(1, 0)
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, MethodTypeRequestReply, result.Type)
}

func TestGetMethodOptions_SubjectOnly(t *testing.T) {
	// Only subject set, no type.
	inner := encodeString(2, "custom.subject")
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, MethodTypeRequestReply, result.Type) // default
	assert.Equal(t, "custom.subject", result.Subject)
}

func TestGetFieldOptions_NotSubjectToken(t *testing.T) {
	// subject_token explicitly false.
	inner := encodeVarint(1, 0)
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.FieldOptions{}, ext).(*descriptorpb.FieldOptions)
	result := GetFieldOptions(opts)
	require.NotNil(t, result)
	assert.False(t, result.SubjectToken)
}

func TestGetServiceOptions_MicroFalse(t *testing.T) {
	inner := encodeVarint(2, 0) // micro = false
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.False(t, result.Micro)
}

func TestParseWireFormat_Truncated64BitFixed(t *testing.T) {
	// Wire type 1 (64-bit fixed) but only 4 bytes available.
	tag := uint64(1<<3) | 1
	var data []byte
	data = appendUvarint(data, tag)
	data = append(data, 0, 0, 0, 0) // only 4 bytes, need 8

	called := false
	parseWireFormat(data, func(uint32, int, []byte) { called = true })
	assert.False(t, called) // should bail, not call handler
}

func TestParseWireFormat_Truncated32BitFixed(t *testing.T) {
	// Wire type 5 (32-bit fixed) but only 2 bytes available.
	tag := uint64(1<<3) | 5
	var data []byte
	data = appendUvarint(data, tag)
	data = append(data, 0, 0) // only 2 bytes, need 4

	called := false
	parseWireFormat(data, func(uint32, int, []byte) { called = true })
	assert.False(t, called)
}

func TestGetExtensionBytes_SkipsVarintFields(t *testing.T) {
	// Build raw bytes with a varint field before the extension.
	var raw []byte
	raw = append(raw, encodeVarint(1, 99)...)            // deprecated field
	raw = append(raw, encodeString(2, "some_option")...) // unknown string field
	inner := encodeString(1, "target")
	raw = append(raw, encodeLengthDelimited(50100, inner)...)

	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, raw).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "target", result.SubjectPrefix)
}
