package gen

import (
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	protonats "github.com/mudomi/protonats"
)

var templatePattern = regexp.MustCompile(`\{(\w+)\}`)

// SubjectPrefix returns the subject prefix for a service: the subject_prefix
// option if set, otherwise the proto package name.
func SubjectPrefix(file *protogen.File, svc *protogen.Service) string {
	if opts := serviceOptions(svc); opts != nil && opts.SubjectPrefix != "" {
		return opts.SubjectPrefix
	}
	return string(file.Desc.Package())
}

// MethodTypeOf returns the communication pattern for a method.
// Default: REQUEST_REPLY.
func MethodTypeOf(m *protogen.Method) protonats.MethodType {
	if opts := methodOptions(m); opts != nil {
		return opts.Type
	}
	return protonats.MethodType_REQUEST_REPLY
}

// MethodSubject returns the NATS subject template for a method: the subject
// option if set, otherwise "{prefix}.{MethodName}".
func MethodSubject(prefix string, m *protogen.Method) string {
	if opts := methodOptions(m); opts != nil && opts.Subject != "" {
		return opts.Subject
	}
	return prefix + "." + string(m.Desc.Name())
}

// IsJetStream reports whether a method type is carried by JetStream rather
// than core NATS, and so requires a stream.
func IsJetStream(mt protonats.MethodType) bool {
	switch mt {
	case protonats.MethodType_JETSTREAM_PUBLISH,
		protonats.MethodType_JETSTREAM_CONSUME,
		protonats.MethodType_JETSTREAM_TASK:
		return true
	}
	return false
}

// MethodStream returns the stream and consumer options for a JetStream method.
func MethodStream(m *protogen.Method) (stream, consumer string) {
	if opts := methodOptions(m); opts != nil {
		return opts.Stream, opts.Consumer
	}
	return "", ""
}

// SubscribeSubject converts a subject template to a NATS wildcard subscription.
// "orders.{order_id}" → "orders.*"
func SubscribeSubject(template string) string {
	return templatePattern.ReplaceAllString(template, "*")
}

// TokenFields resolves each {field_name} token in a subject template to the
// request message field it references. Tokens must name an existing top-level
// scalar field marked with the subject_token option, so the plugin fails at
// generation time instead of emitting code that doesn't compile.
func TokenFields(m *protogen.Method, subject string) ([]*protogen.Field, error) {
	matches := templatePattern.FindAllStringSubmatch(subject, -1)
	if len(matches) == 0 {
		return nil, nil
	}

	fields := make([]*protogen.Field, 0, len(matches))
	for _, match := range matches {
		token := match[1]
		field := fieldByName(m.Input, token)
		if field == nil {
			return nil, fmt.Errorf("%s: subject %q references unknown field %q in %s",
				m.Desc.FullName(), subject, token, m.Input.Desc.FullName())
		}
		if !isSubjectTokenKind(field.Desc) {
			return nil, fmt.Errorf("%s: subject token field %q must be a scalar string, integer, or bool",
				m.Desc.FullName(), token)
		}
		if opts := fieldOptions(field); opts == nil || !opts.SubjectToken {
			return nil, fmt.Errorf("%s: subject %q references field %q which is not marked [(protonats.field).subject_token = true]",
				m.Desc.FullName(), subject, token)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// subjectsOverlap reports whether some concrete subject would match both
// subscription patterns. Patterns are NATS wildcards: "*" matches one token,
// ">" matches all remaining tokens.
func subjectsOverlap(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; ; i++ {
		aEnd, bEnd := i >= len(at), i >= len(bt)
		switch {
		case aEnd && bEnd:
			return true
		case aEnd || bEnd:
			// One pattern ran out of tokens; without a ">" it cannot match the
			// other's remaining tokens.
			return false
		}
		av, bv := at[i], bt[i]
		if av == ">" || bv == ">" {
			return true
		}
		if av != bv && av != "*" && bv != "*" {
			return false
		}
	}
}

// validateSubscriptions rejects methods whose subscription subjects can both
// match one message. NATS delivers such a message to every matching
// subscription, so two request/reply handlers would each answer and the caller
// would take whichever reply arrived first — a coin flip, not a routing rule.
// Overlaps must be resolved in the proto by disambiguating the subjects.
func validateSubscriptions(file *protogen.File) error {
	type subscription struct {
		method  protoreflect.FullName
		pattern string
	}

	var subs []subscription
	for _, svc := range file.Services {
		prefix := SubjectPrefix(file, svc)
		for _, m := range svc.Methods {
			switch MethodTypeOf(m) {
			case protonats.MethodType_REQUEST_REPLY, protonats.MethodType_PUBLISH:
				subs = append(subs, subscription{m.Desc.FullName(), SubscribeSubject(MethodSubject(prefix, m))})
			}
		}
	}

	for i, a := range subs {
		for _, b := range subs[i+1:] {
			if subjectsOverlap(a.pattern, b.pattern) {
				return fmt.Errorf("%s (%s) and %s (%s) subscribe to overlapping subjects: "+
					"NATS would deliver a matching message to both; give them distinct subjects",
					a.method, a.pattern, b.method, b.pattern)
			}
		}
	}
	return nil
}

// validateMessages walks nested messages too: an unsupported option on a
// nested message would otherwise pass and produce silently incomplete output.
func validateMessages(messages []*protogen.Message) error {
	for _, msg := range messages {
		if opts := messageOptions(msg); opts != nil && opts.Kv != nil {
			return fmt.Errorf("%s: the kv option is not yet supported", msg.Desc.FullName())
		}
		if err := validateMessages(msg.Messages); err != nil {
			return err
		}
	}
	return nil
}

func fieldByName(msg *protogen.Message, name string) *protogen.Field {
	for _, f := range msg.Fields {
		if string(f.Desc.Name()) == name {
			return f
		}
	}
	return nil
}

func isSubjectTokenKind(fd protoreflect.FieldDescriptor) bool {
	if fd.IsList() || fd.IsMap() {
		return false
	}
	switch fd.Kind() {
	case protoreflect.StringKind, protoreflect.BoolKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return true
	}
	return false
}

// ValidateFile rejects proto files that use options with no implementation
// behind them, so unsupported features fail at generation time instead of
// silently producing incomplete code.
func ValidateFile(file *protogen.File) error {
	if err := validateMessages(file.Messages); err != nil {
		return err
	}

	for _, svc := range file.Services {
		if opts := serviceOptions(svc); opts != nil && opts.Micro {
			return fmt.Errorf("%s: the micro option is not yet supported", svc.Desc.FullName())
		}

		prefix := SubjectPrefix(file, svc)
		for _, m := range svc.Methods {
			if m.Desc.IsStreamingClient() || m.Desc.IsStreamingServer() {
				return fmt.Errorf("%s: streaming RPCs are not supported", m.Desc.FullName())
			}

			mt := MethodTypeOf(m)
			stream, consumer := MethodStream(m)
			if IsJetStream(mt) && stream == "" {
				return fmt.Errorf("%s: %s requires the stream option", m.Desc.FullName(), mt)
			}
			if !IsJetStream(mt) && stream != "" {
				return fmt.Errorf("%s: the stream option is only valid for JetStream methods", m.Desc.FullName())
			}
			// A task's rollback must reach exactly one instance of the service
			// that owns the undo. An ephemeral consumer would give every
			// instance a copy, so each would undo the same work.
			if mt == protonats.MethodType_JETSTREAM_TASK && consumer == "" {
				return fmt.Errorf("%s: JETSTREAM_TASK requires the consumer option, so its rollback is delivered once", m.Desc.FullName())
			}

			if _, err := TokenFields(m, MethodSubject(prefix, m)); err != nil {
				return err
			}
		}
	}

	return validateSubscriptions(file)
}
