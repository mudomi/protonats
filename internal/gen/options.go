package gen

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	protonats "github.com/mudomi/protonats"
)

// The protonats option extensions are linked into the plugin binary via the
// root package, so protogen resolves them as typed extensions when parsing
// the CodeGeneratorRequest. These accessors return nil when an option is unset.

func serviceOptions(svc *protogen.Service) *protonats.ServiceOptions {
	opts, ok := svc.Desc.Options().(*descriptorpb.ServiceOptions)
	if !ok || !proto.HasExtension(opts, protonats.E_Service) {
		return nil
	}
	return proto.GetExtension(opts, protonats.E_Service).(*protonats.ServiceOptions)
}

func methodOptions(m *protogen.Method) *protonats.MethodOptions {
	opts, ok := m.Desc.Options().(*descriptorpb.MethodOptions)
	if !ok || !proto.HasExtension(opts, protonats.E_Method) {
		return nil
	}
	return proto.GetExtension(opts, protonats.E_Method).(*protonats.MethodOptions)
}

func fieldOptions(f *protogen.Field) *protonats.FieldOptions {
	opts, ok := f.Desc.Options().(*descriptorpb.FieldOptions)
	if !ok || !proto.HasExtension(opts, protonats.E_Field) {
		return nil
	}
	return proto.GetExtension(opts, protonats.E_Field).(*protonats.FieldOptions)
}

func messageOptions(msg *protogen.Message) *protonats.MessageOptions {
	opts, ok := msg.Desc.Options().(*descriptorpb.MessageOptions)
	if !ok || !proto.HasExtension(opts, protonats.E_Message) {
		return nil
	}
	return proto.GetExtension(opts, protonats.E_Message).(*protonats.MessageOptions)
}
