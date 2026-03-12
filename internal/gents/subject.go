package gents

import (
	"regexp"
	"strings"
	"unicode"
)

var templatePattern = regexp.MustCompile(`\{(\w+)\}`)

// subjectTemplateTokens extracts field names from a subject template.
// "orders.{order_id}.{status}" → ["order_id", "status"]
func subjectTemplateTokens(template string) []string {
	matches := templatePattern.FindAllStringSubmatch(template, -1)
	tokens := make([]string, 0, len(matches))
	for _, m := range matches {
		tokens = append(tokens, m[1])
	}
	return tokens
}

// subscribeSubject converts a template to a NATS wildcard subscription.
// "orders.{order_id}" → "orders.*"
func subscribeSubject(template string) string {
	return templatePattern.ReplaceAllString(template, "*")
}

// snakeToCamel converts snake_case to lowerCamelCase matching protobuf-es output.
// "order_id" → "orderId", "warehouse_id" → "warehouseId"
func snakeToCamel(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for i, p := range parts {
		if p == "" {
			continue
		}
		if i == 0 {
			b.WriteString(p)
		} else {
			runes := []rune(p)
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	return b.String()
}

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

// subjectTemplateLiteral converts a subject template to a TS template literal.
// "orders.{order_id}.{status}" → "`orders.${req.orderId}.${req.status}`"
// "testpkg.Echo" → `"testpkg.Echo"`
func subjectTemplateLiteral(template string, tokens []string) string {
	if len(tokens) == 0 {
		return `"` + template + `"`
	}
	tokenIdx := 0
	result := templatePattern.ReplaceAllStringFunc(template, func(_ string) string {
		camel := snakeToCamel(tokens[tokenIdx])
		tokenIdx++
		return "${req." + camel + "}"
	})
	return "`" + result + "`"
}
