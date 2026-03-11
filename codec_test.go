package protonats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtoCodec(t *testing.T) {
	codec := ProtoCodec{}
	assert.Equal(t, "application/protobuf", codec.ContentType())

	data, err := codec.Marshal(wrapperspb.String("hello"))
	require.NoError(t, err)

	out := &wrapperspb.StringValue{}
	require.NoError(t, codec.Unmarshal(data, out))
	assert.Equal(t, "hello", out.Value)
}

func TestJSONCodec(t *testing.T) {
	assert.Equal(t, "application/json", JSONCodec.ContentType())

	data, err := JSONCodec.Marshal(wrapperspb.String("hello"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "hello")

	out := &wrapperspb.StringValue{}
	require.NoError(t, JSONCodec.Unmarshal(data, out))
	assert.Equal(t, "hello", out.Value)
}
