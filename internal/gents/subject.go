package gents

import (
	"regexp"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
)

var templatePattern = regexp.MustCompile(`\{(\w+)\}`)

// pascalToCamel converts PascalCase to camelCase for method names.
// "GetOrder" → "getOrder"
func pascalToCamel(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

// subjectTemplateLiteral converts a subject template to a TS expression.
// Token fields use their protobuf-es property name (the proto JSON name):
// "orders.{order_id}" → "`orders.${req.orderId}`", "testpkg.Echo" → `"testpkg.Echo"`.
func subjectTemplateLiteral(template string, tokens []*protogen.Field) string {
	if len(tokens) == 0 {
		return `"` + template + `"`
	}
	tokenIdx := 0
	result := templatePattern.ReplaceAllStringFunc(template, func(_ string) string {
		prop := tokens[tokenIdx].Desc.JSONName()
		tokenIdx++
		return "${req." + prop + "}"
	})
	return "`" + result + "`"
}
