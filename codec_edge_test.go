package protonats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtoCodec_NilMessage(t *testing.T) {
	codec := ProtoCodec{}
	// Marshal nil produces empty output (valid empty proto message).
	data, err := codec.Marshal(nil)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestProtoCodec_EmptyMessage(t *testing.T) {
	codec := ProtoCodec{}
	data, err := codec.Marshal(&emptypb.Empty{})
	require.NoError(t, err)

	out := &emptypb.Empty{}
	require.NoError(t, codec.Unmarshal(data, out))
}

func TestProtoCodec_UnmarshalEmptyBytes(t *testing.T) {
	codec := ProtoCodec{}
	out := &wrapperspb.StringValue{}
	// Empty bytes is valid proto — all fields get zero values.
	require.NoError(t, codec.Unmarshal([]byte{}, out))
	assert.Equal(t, "", out.Value)
}

func TestProtoCodec_UnmarshalNilBytes(t *testing.T) {
	codec := ProtoCodec{}
	out := &wrapperspb.StringValue{}
	require.NoError(t, codec.Unmarshal(nil, out))
	assert.Equal(t, "", out.Value)
}

func TestProtoCodec_UnmarshalInvalidBytes(t *testing.T) {
	codec := ProtoCodec{}
	out := &wrapperspb.StringValue{}
	err := codec.Unmarshal([]byte{0xFF, 0xFF, 0xFF}, out)
	assert.Error(t, err)
}

func TestProtoCodec_UnmarshalTruncated(t *testing.T) {
	codec := ProtoCodec{}
	data, _ := codec.Marshal(wrapperspb.String("hello world"))
	// Truncate the data.
	err := codec.Unmarshal(data[:len(data)/2], &wrapperspb.StringValue{})
	assert.Error(t, err)
}

func TestProtoCodec_WrongMessageType(t *testing.T) {
	codec := ProtoCodec{}
	// Marshal a StringValue, unmarshal as Int32Value.
	data, _ := codec.Marshal(wrapperspb.String("hello"))
	out := &wrapperspb.Int32Value{}
	// Proto is lenient — unknown fields are silently ignored.
	require.NoError(t, codec.Unmarshal(data, out))
}

func TestJSONCodec_EmptyObject(t *testing.T) {
	// protojson wraps google.protobuf.StringValue specially — {} is invalid for it.
	// Use a regular proto message instead.
	out := &emptypb.Empty{}
	require.NoError(t, JSONCodec.Unmarshal([]byte(`{}`), out))
}

func TestJSONCodec_InvalidJSON(t *testing.T) {
	out := &wrapperspb.StringValue{}
	assert.Error(t, JSONCodec.Unmarshal([]byte(`not json`), out))
}

func TestJSONCodec_NullValue(t *testing.T) {
	out := &wrapperspb.StringValue{}
	// protojson rejects bare null.
	assert.Error(t, JSONCodec.Unmarshal([]byte(`null`), out))
}

func TestJSONCodec_ExtraFields(t *testing.T) {
	out := &wrapperspb.StringValue{}
	// protojson rejects unknown fields by default.
	err := JSONCodec.Unmarshal([]byte(`{"value":"hi","unknown":123}`), out)
	assert.Error(t, err)
}

func TestJSONCodec_WrongJSONType(t *testing.T) {
	out := &wrapperspb.StringValue{}
	assert.Error(t, JSONCodec.Unmarshal([]byte(`[1,2,3]`), out))
}

func TestCodec_Roundtrip_EmptyString(t *testing.T) {
	for name, codec := range map[string]Codec{"proto": ProtoCodec{}, "json": JSONCodec} {
		t.Run(name, func(t *testing.T) {
			msg := wrapperspb.String("")
			data, err := codec.Marshal(msg)
			require.NoError(t, err)
			out := &wrapperspb.StringValue{}
			require.NoError(t, codec.Unmarshal(data, out))
			assert.Equal(t, "", out.Value)
		})
	}
}

func TestCodec_Roundtrip_Unicode(t *testing.T) {
	for name, codec := range map[string]Codec{"proto": ProtoCodec{}, "json": JSONCodec} {
		t.Run(name, func(t *testing.T) {
			msg := wrapperspb.String("日本語 🎉 émojis")
			data, err := codec.Marshal(msg)
			require.NoError(t, err)
			out := &wrapperspb.StringValue{}
			require.NoError(t, codec.Unmarshal(data, out))
			assert.Equal(t, "日本語 🎉 émojis", out.Value)
		})
	}
}
