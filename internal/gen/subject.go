package gen

import (
	"regexp"
	"strings"
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

// snakeToCamel converts snake_case to CamelCase Go identifier.
func snakeToCamel(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		switch p {
		case "id":
			b.WriteString("ID")
		case "url":
			b.WriteString("URL")
		case "ip":
			b.WriteString("IP")
		case "http":
			b.WriteString("HTTP")
		case "api":
			b.WriteString("API")
		default:
			b.WriteString(strings.ToUpper(p[:1]))
			b.WriteString(p[1:])
		}
	}
	return b.String()
}
