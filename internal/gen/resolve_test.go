package gen

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	protonats "github.com/mudomi/protonats"
	"github.com/mudomi/protonats/internal/gentest"
)

func testServices(t *testing.T) (file *protogen.File, testSvc, prefixedSvc *protogen.Service) {
	t.Helper()
	plugin := gentest.NewPlugin(t)
	file = gentest.TestFile(t, plugin)
	for _, svc := range file.Services {
		switch svc.GoName {
		case "TestService":
			testSvc = svc
		case "PrefixedService":
			prefixedSvc = svc
		}
	}
	if testSvc == nil || prefixedSvc == nil {
		t.Fatal("expected TestService and PrefixedService in test.proto")
	}
	return file, testSvc, prefixedSvc
}

func methodByName(t *testing.T, svc *protogen.Service, name string) *protogen.Method {
	t.Helper()
	for _, m := range svc.Methods {
		if m.GoName == name {
			return m
		}
	}
	t.Fatalf("method %s not found", name)
	return nil
}

func TestSubjectPrefix(t *testing.T) {
	file, testSvc, prefixedSvc := testServices(t)

	if got := SubjectPrefix(file, testSvc); got != "testpkg" {
		t.Errorf("default prefix = %q, want package name", got)
	}
	if got := SubjectPrefix(file, prefixedSvc); got != "custom.prefix" {
		t.Errorf("option prefix = %q, want custom.prefix", got)
	}
}

func TestMethodTypeOf(t *testing.T) {
	_, svc, _ := testServices(t)

	want := map[string]protonats.MethodType{
		"Echo":         protonats.MethodType_REQUEST_REPLY,
		"GetItem":      protonats.MethodType_REQUEST_REPLY,
		"Notify":       protonats.MethodType_PUBLISH,
		"EmitEvent":    protonats.MethodType_JETSTREAM_PUBLISH,
		"ProcessEvent": protonats.MethodType_JETSTREAM_CONSUME,
	}
	for name, mt := range want {
		if got := MethodTypeOf(methodByName(t, svc, name)); got != mt {
			t.Errorf("%s type = %v, want %v", name, got, mt)
		}
	}
}

func TestMethodSubject(t *testing.T) {
	_, svc, _ := testServices(t)

	if got := MethodSubject("testpkg", methodByName(t, svc, "Echo")); got != "testpkg.Echo" {
		t.Errorf("default subject = %q", got)
	}
	if got := MethodSubject("testpkg", methodByName(t, svc, "GetItem")); got != "items.{item_id}" {
		t.Errorf("override subject = %q", got)
	}
}

func TestMethodStream(t *testing.T) {
	_, svc, _ := testServices(t)

	stream, consumer := MethodStream(methodByName(t, svc, "ProcessEvent"))
	if stream != "EVENTS" || consumer != "event-processor" {
		t.Errorf("stream/consumer = %q/%q", stream, consumer)
	}
}

func TestSubscribeSubject(t *testing.T) {
	cases := map[string]string{
		"orders.{order_id}":          "orders.*",
		"orders.{order_id}.{status}": "orders.*.*",
		"plain.subject":              "plain.subject",
		"":                           "",
	}
	for template, want := range cases {
		if got := SubscribeSubject(template); got != want {
			t.Errorf("SubscribeSubject(%q) = %q, want %q", template, got, want)
		}
	}
}

func TestTokenFields(t *testing.T) {
	_, svc, _ := testServices(t)
	m := methodByName(t, svc, "GetItem")

	fields, err := TokenFields(m, "items.{item_id}")
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields[0].GoName != "ItemId" {
		t.Errorf("fields = %v", fields)
	}

	if fields, err := TokenFields(m, "items.static"); err != nil || fields != nil {
		t.Errorf("no tokens: fields=%v err=%v", fields, err)
	}

	if _, err := TokenFields(m, "items.{nope}"); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("unknown field: err=%v", err)
	}
}

func TestValidateFile_TestProto(t *testing.T) {
	file, _, _ := testServices(t)
	if err := ValidateFile(file); err != nil {
		t.Errorf("test.proto should validate: %v", err)
	}
}

func TestSubjectsOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"items.*", "items.list", true},    // the case that motivated the check
		{"items.*", "items.*", true},       // identical wildcards
		{"orders.get", "orders.get", true}, // identical literals
		{"a.>", "a.b.c", true},             // ">" swallows the remainder
		{"items.*", "items.a.b", false},    // token counts differ
		{"items.*", "orders.list", false},  // different literal prefix
		{"orders.get", "orders.create", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
	}
	for _, tc := range cases {
		if got := subjectsOverlap(tc.a, tc.b); got != tc.want {
			t.Errorf("subjectsOverlap(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := subjectsOverlap(tc.b, tc.a); got != tc.want {
			t.Errorf("subjectsOverlap(%q, %q) = %v, want %v (not symmetric)", tc.b, tc.a, got, tc.want)
		}
	}
}

// buildServiceFile compiles a proto file whose service has one request/reply
// method per given subject, named Do0, Do1, ... Used to exercise file-level
// checks that need more than one method.
func buildServiceFile(t *testing.T, subjects ...string) *protogen.File {
	t.Helper()

	svcDesc := &descriptorpb.ServiceDescriptorProto{Name: proto.String("Svc")}
	for i, subject := range subjects {
		opts := &descriptorpb.MethodOptions{}
		proto.SetExtension(opts, protonats.E_Method, &protonats.MethodOptions{Subject: subject})
		svcDesc.Method = append(svcDesc.Method, &descriptorpb.MethodDescriptorProto{
			Name:       proto.String(fmt.Sprintf("Do%d", i)),
			InputType:  proto.String(".synth.Req"),
			OutputType: proto.String(".synth.Req"),
			Options:    opts,
		})
	}

	tokenField := &descriptorpb.FieldDescriptorProto{
		Name:    proto.String("item_id"),
		Number:  proto.Int32(1),
		Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Options: &descriptorpb.FieldOptions{},
	}
	proto.SetExtension(tokenField.Options, protonats.E_Field, &protonats.FieldOptions{SubjectToken: true})

	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("synth.proto"),
		Package: proto.String("synth"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("example.com/synth;synth")},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Req"),
			Field: []*descriptorpb.FieldDescriptorProto{tokenField},
		}},
		Service: []*descriptorpb.ServiceDescriptorProto{svcDesc},
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"synth.proto"},
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(protonats.File_protonats_options_proto),
			fd,
		},
	}
	plugin, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen.New: %v", err)
	}
	for _, f := range plugin.Files {
		if f.Generate {
			return f
		}
	}
	t.Fatal("synth.proto not built")
	return nil
}

func TestValidateFile_RejectsOverlappingSubjects(t *testing.T) {
	// "items.{item_id}" subscribes "items.*", which also matches "items.list".
	err := ValidateFile(buildServiceFile(t, "items.{item_id}", "items.list"))
	if err == nil || !strings.Contains(err.Error(), "overlapping subjects") {
		t.Fatalf("err = %v, want an overlapping-subjects error", err)
	}
}

func TestValidateFile_AllowsDistinctSubjects(t *testing.T) {
	// A wildcard method alongside unrelated literals must not false-positive.
	if err := ValidateFile(buildServiceFile(t, "items.get.{item_id}", "items.list", "orders.create")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A service whose only method is a JetStream publish has no handler side at
// all, so it gets a client and nothing else — an empty interface and a
// registration that subscribes to nothing would only be noise.
func TestGenerateFile_ClientOnlyService(t *testing.T) {
	got := generateSynth(t, &protonats.MethodOptions{
		Type:   protonats.MethodType_JETSTREAM_PUBLISH,
		Stream: "EVENTS",
	})

	if !strings.Contains(got, "func NewSvcClient(") {
		t.Errorf("the client is still required; got:\n%s", got)
	}
	for _, absent := range []string{"type SvcHandler interface", "func RegisterSvcHandler(", "pn.Subscribe("} {
		if strings.Contains(got, absent) {
			t.Errorf("a publish-only service must not generate %q; got:\n%s", absent, got)
		}
	}
}

// A task generates three roles from one declaration: the client that triggers
// it, the worker that runs it, and the rollback that undoes it. The rollback's
// subject and consumer are derived, which is what keeps the two halves in step.
func TestGenerateFile_TaskGeneratesBothRoles(t *testing.T) {
	got := generateSynth(t, &protonats.MethodOptions{
		Type:     protonats.MethodType_JETSTREAM_TASK,
		Stream:   "EVENTS",
		Consumer: "doer",
	})

	for _, want := range []string{
		"func (c *SvcClient) Do(",       // trigger
		"type SvcWorker interface",      // performs the work
		"func RegisterSvcWorker(",       //
		"pn.ConsumeTask(",               //
		"type SvcRollback interface",    // undoes it
		"func RegisterSvcRollback(",     //
		"pn.ConsumeRollback(",           //
		`Subject:  "synth.Do.rollback"`, // derived, not configured
		`Consumer: "doer-rollback"`,     // derived, not configured
		"cause *protonats.Error",        // the rollback is told why
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generated output missing %q; got:\n%s", want, got)
		}
	}

	// The task must not also appear in the plain handler surface.
	if strings.Contains(got, "type SvcHandler interface") {
		t.Errorf("a task-only service must not generate a plain handler; got:\n%s", got)
	}
}

// generateSynth builds a one-method service with the given options and returns
// the generated Go source.
func generateSynth(t *testing.T, opts *protonats.MethodOptions) string {
	t.Helper()
	plugin, file := buildPluginAndFile(t, nil, opts, false, nil)
	if err := GenerateFile(plugin, file); err != nil {
		t.Fatal(err)
	}
	resp := plugin.Response()
	if resp.Error != nil {
		t.Fatalf("plugin error: %s", resp.GetError())
	}
	return resp.File[0].GetContent()
}

// ── Invalid option combinations, built as synthetic descriptors ──

// buildFile compiles a single-service proto file descriptor: message Req with
// a string field item_id (subject_token controlled by tokenOpt), and service
// Svc with method Do(Req) returns (Req) carrying the given options.
func buildFile(t *testing.T, svcOpts *protonats.ServiceOptions, methodOpts *protonats.MethodOptions, tokenOpt bool, msgOpts *protonats.MessageOptions) *protogen.File {
	t.Helper()
	_, file := buildPluginAndFile(t, svcOpts, methodOpts, tokenOpt, msgOpts)
	return file
}

// buildPluginAndFile also returns the plugin, which GenerateFile needs in order
// to collect its output.
func buildPluginAndFile(t *testing.T, svcOpts *protonats.ServiceOptions, methodOpts *protonats.MethodOptions, tokenOpt bool, msgOpts *protonats.MessageOptions) (*protogen.Plugin, *protogen.File) {
	t.Helper()

	fieldDesc := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String("item_id"),
		Number: proto.Int32(1),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
	}
	if tokenOpt {
		fieldDesc.Options = &descriptorpb.FieldOptions{}
		proto.SetExtension(fieldDesc.Options, protonats.E_Field, &protonats.FieldOptions{SubjectToken: true})
	}

	msgDesc := &descriptorpb.DescriptorProto{
		Name:  proto.String("Req"),
		Field: []*descriptorpb.FieldDescriptorProto{fieldDesc},
	}
	if msgOpts != nil {
		msgDesc.Options = &descriptorpb.MessageOptions{}
		proto.SetExtension(msgDesc.Options, protonats.E_Message, msgOpts)
	}

	methodDesc := &descriptorpb.MethodDescriptorProto{
		Name:       proto.String("Do"),
		InputType:  proto.String(".synth.Req"),
		OutputType: proto.String(".synth.Req"),
	}
	if methodOpts != nil {
		methodDesc.Options = &descriptorpb.MethodOptions{}
		proto.SetExtension(methodDesc.Options, protonats.E_Method, methodOpts)
	}

	svcDesc := &descriptorpb.ServiceDescriptorProto{
		Name:   proto.String("Svc"),
		Method: []*descriptorpb.MethodDescriptorProto{methodDesc},
	}
	if svcOpts != nil {
		svcDesc.Options = &descriptorpb.ServiceOptions{}
		proto.SetExtension(svcDesc.Options, protonats.E_Service, svcOpts)
	}

	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("synth.proto"),
		Package: proto.String("synth"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{
			GoPackage: proto.String("example.com/synth;synth"),
		},
		MessageType: []*descriptorpb.DescriptorProto{msgDesc},
		Service:     []*descriptorpb.ServiceDescriptorProto{svcDesc},
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"synth.proto"},
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(protonats.File_protonats_options_proto),
			fd,
		},
	}
	plugin, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen.New: %v", err)
	}
	for _, f := range plugin.Files {
		if f.Generate {
			return plugin, f
		}
	}
	t.Fatal("synth.proto not built")
	return nil, nil
}

func TestValidateFile_Rejections(t *testing.T) {
	cases := []struct {
		name    string
		svc     *protonats.ServiceOptions
		method  *protonats.MethodOptions
		token   bool
		msg     *protonats.MessageOptions
		wantErr string
	}{
		{
			name:    "micro unsupported",
			svc:     &protonats.ServiceOptions{Micro: true},
			wantErr: "micro option is not yet supported",
		},
		{
			name:    "kv unsupported",
			msg:     &protonats.MessageOptions{Kv: &protonats.KVOptions{Bucket: "b"}},
			wantErr: "kv option is not yet supported",
		},
		{
			name:    "jetstream publish requires stream",
			method:  &protonats.MethodOptions{Type: protonats.MethodType_JETSTREAM_PUBLISH},
			wantErr: "requires the stream option",
		},
		{
			name:    "jetstream consume requires stream",
			method:  &protonats.MethodOptions{Type: protonats.MethodType_JETSTREAM_CONSUME},
			wantErr: "requires the stream option",
		},
		{
			name:    "stream invalid on request/reply",
			method:  &protonats.MethodOptions{Stream: "S"},
			wantErr: "only valid for JetStream methods",
		},
		{
			name:    "task requires stream",
			method:  &protonats.MethodOptions{Type: protonats.MethodType_JETSTREAM_TASK, Consumer: "c"},
			wantErr: "requires the stream option",
		},
		{
			// An ephemeral consumer would hand every instance a copy of the
			// rollback, so each would undo the same work.
			name:    "task requires consumer",
			method:  &protonats.MethodOptions{Type: protonats.MethodType_JETSTREAM_TASK, Stream: "S"},
			wantErr: "requires the consumer option",
		},
		{
			name:   "valid task",
			method: &protonats.MethodOptions{Type: protonats.MethodType_JETSTREAM_TASK, Stream: "S", Consumer: "c"},
		},
		{
			name:    "unknown token field",
			method:  &protonats.MethodOptions{Subject: "x.{missing}"},
			wantErr: "unknown field",
		},
		{
			name:    "token field without subject_token option",
			method:  &protonats.MethodOptions{Subject: "x.{item_id}"},
			token:   false,
			wantErr: "not marked",
		},
		{
			name:   "valid token field",
			method: &protonats.MethodOptions{Subject: "x.{item_id}"},
			token:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := buildFile(t, tc.svc, tc.method, tc.token, tc.msg)
			err := ValidateFile(file)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}
