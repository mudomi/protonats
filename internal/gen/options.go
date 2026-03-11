package gen

import (
	"math"

	"google.golang.org/protobuf/proto"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

const extensionFieldNumber = 50100

type MethodType int32

const (
	MethodTypeRequestReply    MethodType = 0
	MethodTypePublish         MethodType = 1
	MethodTypeJetstreamPublish MethodType = 2
	MethodTypeJetstreamConsume MethodType = 3
)

type ServiceOptions struct {
	SubjectPrefix string
	Micro         bool
	Version       string
	Description   string
}

type MethodOptions struct {
	Type     MethodType
	Subject  string
	Stream   string
	Consumer string
}

type FieldOptions struct {
	SubjectToken bool
}

// GetServiceOptions extracts ProtoNats options from a service descriptor.
func GetServiceOptions(opts *descriptorpb.ServiceOptions) *ServiceOptions {
	if opts == nil {
		return nil
	}
	raw := getExtensionBytes(opts, extensionFieldNumber)
	if raw == nil {
		return nil
	}
	result := &ServiceOptions{}
	parseWireFormat(raw, func(fieldNum uint32, wireType int, data []byte) {
		switch fieldNum {
		case 1:
			result.SubjectPrefix = string(data)
		case 2:
			result.Micro = len(data) > 0 && data[0] != 0
		case 3:
			result.Version = string(data)
		case 4:
			result.Description = string(data)
		}
	})
	return result
}

// GetMethodOptions extracts ProtoNats options from a method descriptor.
func GetMethodOptions(opts *descriptorpb.MethodOptions) *MethodOptions {
	if opts == nil {
		return nil
	}
	raw := getExtensionBytes(opts, extensionFieldNumber)
	if raw == nil {
		return nil
	}
	result := &MethodOptions{}
	parseWireFormat(raw, func(fieldNum uint32, wireType int, data []byte) {
		switch fieldNum {
		case 1:
			if wireType == 0 && len(data) > 0 {
				result.Type = MethodType(data[0])
			}
		case 2:
			result.Subject = string(data)
		case 3:
			result.Stream = string(data)
		case 4:
			result.Consumer = string(data)
		}
	})
	return result
}

// GetFieldOptions extracts ProtoNats options from a field descriptor.
func GetFieldOptions(opts *descriptorpb.FieldOptions) *FieldOptions {
	if opts == nil {
		return nil
	}
	raw := getExtensionBytes(opts, extensionFieldNumber)
	if raw == nil {
		return nil
	}
	result := &FieldOptions{}
	parseWireFormat(raw, func(fieldNum uint32, wireType int, data []byte) {
		if fieldNum == 1 {
			result.SubjectToken = len(data) > 0 && data[0] != 0
		}
	})
	return result
}

// getExtensionBytes extracts raw bytes for a specific extension field number
// by re-marshaling the options and scanning the wire format.
func getExtensionBytes(msg proto.Message, fieldNumber int) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil
	}
	for len(b) > 0 {
		num, wt, n := consumeTag(b)
		if n < 0 {
			return nil
		}
		b = b[n:]

		switch wt {
		case 0: // varint
			_, vn := consumeVarint(b)
			if vn < 0 {
				return nil
			}
			b = b[vn:]
		case 1: // 64-bit fixed
			if len(b) < 8 {
				return nil
			}
			b = b[8:]
		case 2: // length-delimited
			length, vn := consumeVarint(b)
			if vn < 0 || uint64(len(b)-vn) < length {
				return nil
			}
			if int(num) == fieldNumber {
				return b[vn : vn+int(length)]
			}
			b = b[vn+int(length):]
		case 5: // 32-bit fixed
			if len(b) < 4 {
				return nil
			}
			b = b[4:]
		default:
			return nil
		}
	}
	return nil
}

// parseWireFormat iterates protobuf wire format bytes, calling handler per field.
func parseWireFormat(b []byte, handler func(fieldNum uint32, wireType int, data []byte)) {
	for len(b) > 0 {
		num, wt, n := consumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]

		switch wt {
		case 0: // varint
			val, vn := consumeVarint(b)
			if vn < 0 {
				return
			}
			handler(num, 0, []byte{byte(val)})
			b = b[vn:]
		case 2: // length-delimited
			length, vn := consumeVarint(b)
			if vn < 0 || uint64(len(b)-vn) < length {
				return
			}
			handler(num, 2, b[vn:vn+int(length)])
			b = b[vn+int(length):]
		case 1: // 64-bit fixed
			if len(b) < 8 {
				return
			}
			b = b[8:]
		case 5: // 32-bit fixed
			if len(b) < 4 {
				return
			}
			b = b[4:]
		default:
			return
		}
	}
}

func consumeTag(b []byte) (num uint32, wireType int, n int) {
	v, n := consumeVarint(b)
	if n < 0 {
		return 0, 0, -1
	}
	if v > math.MaxUint32 {
		return 0, 0, -1
	}
	return uint32(v >> 3), int(v & 0x7), n
}

func consumeVarint(b []byte) (uint64, int) {
	var val uint64
	for i := 0; i < len(b) && i < 10; i++ {
		val |= uint64(b[i]&0x7f) << (i * 7)
		if b[i]&0x80 == 0 {
			return val, i + 1
		}
	}
	return 0, -1
}
