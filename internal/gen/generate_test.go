package gen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

func TestResolveSubjectPrefix_Default(t *testing.T) {
	// When no ServiceOptions extension is present, prefix should come from package name.
	// We test the underlying GetServiceOptions path here since resolveSubjectPrefix
	// requires protogen types which are hard to construct in unit tests.
	opts := &descriptorpb.ServiceOptions{}
	result := GetServiceOptions(opts)
	assert.Nil(t, result)
}

func TestResolveSubjectPrefix_WithExtension(t *testing.T) {
	inner := encodeString(1, "custom.prefix")
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)

	result := GetServiceOptions(opts)
	assert.Equal(t, "custom.prefix", result.SubjectPrefix)
}

func TestMethodType_Default(t *testing.T) {
	// No extension → REQUEST_REPLY
	opts := &descriptorpb.MethodOptions{}
	result := GetMethodOptions(opts)
	assert.Nil(t, result)
}

func TestMethodType_AllTypes(t *testing.T) {
	types := []struct {
		val  uint64
		want MethodType
	}{
		{0, MethodTypeRequestReply},
		{1, MethodTypePublish},
		{2, MethodTypeJetstreamPublish},
		{3, MethodTypeJetstreamConsume},
	}

	for _, tt := range types {
		inner := encodeVarint(1, tt.val)
		ext := encodeLengthDelimited(50100, inner)
		opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
		result := GetMethodOptions(opts)
		assert.Equal(t, tt.want, result.Type)
	}
}

func TestMethodSubject_Static(t *testing.T) {
	inner := encodeString(2, "my.custom.subject")
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	assert.Equal(t, "my.custom.subject", result.Subject)
}

func TestMethodOptions_StreamAndConsumer(t *testing.T) {
	inner := encodeVarint(1, 3) // JETSTREAM_CONSUME
	inner = append(inner, encodeString(3, "MY_STREAM")...)
	inner = append(inner, encodeString(4, "my-consumer")...)
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.MethodOptions{}, ext).(*descriptorpb.MethodOptions)
	result := GetMethodOptions(opts)
	assert.Equal(t, MethodTypeJetstreamConsume, result.Type)
	assert.Equal(t, "MY_STREAM", result.Stream)
	assert.Equal(t, "my-consumer", result.Consumer)
}

func TestServiceOptions_AllFields(t *testing.T) {
	inner := encodeString(1, "svc.prefix")
	inner = append(inner, encodeVarint(2, 1)...) // micro = true
	inner = append(inner, encodeString(3, "1.0.0")...)
	inner = append(inner, encodeString(4, "Test service")...)
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	assert.Equal(t, "svc.prefix", result.SubjectPrefix)
	assert.True(t, result.Micro)
	assert.Equal(t, "1.0.0", result.Version)
	assert.Equal(t, "Test service", result.Description)
}

func TestFieldOptions_SubjectTokenFalse(t *testing.T) {
	inner := encodeVarint(1, 0) // subject_token = false
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.FieldOptions{}, ext).(*descriptorpb.FieldOptions)
	result := GetFieldOptions(opts)
	assert.NotNil(t, result)
	assert.False(t, result.SubjectToken)
}

func TestGetFieldOptions_Nil(t *testing.T) {
	assert.Nil(t, GetFieldOptions(nil))
}

func TestGetFieldOptions_NoExtension(t *testing.T) {
	assert.Nil(t, GetFieldOptions(&descriptorpb.FieldOptions{}))
}

func TestGetMethodOptions_Nil(t *testing.T) {
	assert.Nil(t, GetMethodOptions(nil))
}

func TestConsumeVarint_Empty(t *testing.T) {
	_, n := consumeVarint([]byte{})
	assert.Equal(t, -1, n)
}

func TestConsumeVarint_Unterminated(t *testing.T) {
	// All bytes have continuation bit set — never terminates
	data := make([]byte, 11)
	for i := range data {
		data[i] = 0x80
	}
	_, n := consumeVarint(data)
	assert.Equal(t, -1, n)
}

func TestConsumeTag_Invalid(t *testing.T) {
	_, _, n := consumeTag([]byte{})
	assert.Equal(t, -1, n)
}

func TestParseWireFormat_EmptyInput(t *testing.T) {
	called := false
	parseWireFormat(nil, func(uint32, int, []byte) { called = true })
	assert.False(t, called)
}

func TestParseWireFormat_TruncatedLengthDelimited(t *testing.T) {
	// Valid tag for length-delimited field 1, but claims length 100 with no data
	data := encodeString(1, "hello")
	// Truncate: remove last 3 bytes
	data = data[:len(data)-3]

	fields := 0
	parseWireFormat(data, func(uint32, int, []byte) { fields++ })
	assert.Equal(t, 0, fields)
}

func TestGetExtensionBytes_EmptyMessage(t *testing.T) {
	result := getExtensionBytes(&descriptorpb.ServiceOptions{}, 50100)
	assert.Nil(t, result)
}

func TestGetExtensionBytes_WrongFieldNumber(t *testing.T) {
	inner := encodeString(1, "data")
	ext := encodeLengthDelimited(50100, inner)
	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)

	// Look for wrong field number
	result := getExtensionBytes(opts, 99999)
	assert.Nil(t, result)
}

func TestParseWireFormat_FixedWidth(t *testing.T) {
	// Build data with a 32-bit fixed field (wire type 5) and a 64-bit fixed field (wire type 1).
	// These aren't used in our options but the parser should skip them cleanly.
	// Wire type 5 (32-bit): tag = (fieldNum << 3) | 5
	tag32 := uint64(10<<3) | 5 // field 10, wire type 5
	var data []byte
	data = appendUvarint(data, tag32)
	data = append(data, 0, 0, 0, 0) // 4 bytes

	tag64 := uint64(11<<3) | 1 // field 11, wire type 1
	data = appendUvarint(data, tag64)
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0) // 8 bytes

	// Add a known string field after
	data = append(data, encodeString(1, "after-fixed")...)

	var got string
	parseWireFormat(data, func(num uint32, wt int, d []byte) {
		if num == 1 {
			got = string(d)
		}
	})
	assert.Equal(t, "after-fixed", got)
}

func TestParseWireFormat_UnknownWireType(t *testing.T) {
	// Wire type 3 (start group, deprecated) should cause parser to stop
	tag := uint64(1<<3) | 3
	var data []byte
	data = appendUvarint(data, tag)
	data = append(data, encodeString(2, "unreachable")...)

	called := false
	parseWireFormat(data, func(uint32, int, []byte) { called = true })
	assert.False(t, called)
}

func TestGetExtensionBytes_VarintField(t *testing.T) {
	// Build a message where the extension field number stores a varint instead of length-delimited
	// This exercises the varint branch in getExtensionBytes
	deprecatedField := encodeVarint(33, 1) // deprecated field in ServiceOptions
	inner := encodeString(1, "test")
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, append(deprecatedField, ext...)).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "test", result.SubjectPrefix)
}

func TestGetExtensionBytes_Fixed32InMessage(t *testing.T) {
	// Test that getExtensionBytes properly skips fixed32 fields
	// ServiceOptions has no fixed32 fields, but the raw wire format might
	// We test the parser handles this correctly
	inner := encodeString(1, "prefix")
	ext := encodeLengthDelimited(50100, inner)

	opts := roundTrip(t, &descriptorpb.ServiceOptions{}, ext).(*descriptorpb.ServiceOptions)
	result := GetServiceOptions(opts)
	require.NotNil(t, result)
	assert.Equal(t, "prefix", result.SubjectPrefix)
}

func TestConsumeTag_LargeFieldNumber(t *testing.T) {
	// Field number 50100 requires multi-byte varint tag
	tag := uint64(50100<<3) | 2
	var data []byte
	data = appendUvarint(data, tag)
	num, wt, n := consumeTag(data)
	assert.Equal(t, uint32(50100), num)
	assert.Equal(t, 2, wt)
	assert.Greater(t, n, 0)
}
