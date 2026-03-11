package protonats

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Codec defines how proto messages are serialized for NATS transport.
type Codec interface {
	Marshal(proto.Message) ([]byte, error)
	Unmarshal([]byte, proto.Message) error
	ContentType() string
}

// ProtoCodec uses protobuf binary format.
type ProtoCodec struct{}

func (ProtoCodec) Marshal(msg proto.Message) ([]byte, error)  { return proto.Marshal(msg) }
func (ProtoCodec) Unmarshal(b []byte, msg proto.Message) error { return proto.Unmarshal(b, msg) }
func (ProtoCodec) ContentType() string                         { return "application/protobuf" }

// JSONCodec uses protobuf JSON format.
var JSONCodec Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Marshal(msg proto.Message) ([]byte, error)  { return protojson.Marshal(msg) }
func (jsonCodec) Unmarshal(b []byte, msg proto.Message) error { return protojson.Unmarshal(b, msg) }
func (jsonCodec) ContentType() string                         { return "application/json" }
