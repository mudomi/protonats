// Package gentest builds protogen plugins from the checked-in test.proto
// descriptor, for generator tests. The descriptor is recovered from the
// generated testgen package instead of being hand-assembled, so tests always
// exercise exactly what protoc produced.
package gentest

import (
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/pluginpb"

	protonats "github.com/mudomi/protonats"
	testgen "github.com/mudomi/protonats/internal/gen/testdata/gen"
)

// NewPlugin returns a protogen plugin loaded with test.proto and its imports,
// with test.proto marked for generation.
func NewPlugin(t *testing.T) *protogen.Plugin {
	t.Helper()

	// Dependency order matters: each file's imports must precede it.
	deps := []protoreflect.FileDescriptor{
		descriptorpb.File_google_protobuf_descriptor_proto,
		emptypb.File_google_protobuf_empty_proto,
		protonats.File_protonats_options_proto,
		testgen.File_test_proto,
	}
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"test.proto"},
	}
	for _, fd := range deps {
		req.ProtoFile = append(req.ProtoFile, protodesc.ToFileDescriptorProto(fd))
	}

	plugin, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen.New: %v", err)
	}
	return plugin
}

// TestFile returns the protogen file for test.proto.
func TestFile(t *testing.T, plugin *protogen.Plugin) *protogen.File {
	t.Helper()
	for _, f := range plugin.Files {
		if f.Generate {
			return f
		}
	}
	t.Fatal("test.proto not found in plugin files")
	return nil
}
