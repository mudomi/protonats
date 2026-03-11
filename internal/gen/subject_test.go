package gen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubjectTemplateTokens(t *testing.T) {
	tests := []struct {
		template string
		want     []string
	}{
		{"orders.Create", nil},
		{"orders.{order_id}", []string{"order_id"}},
		{"events.{region}.{event_type}", []string{"region", "event_type"}},
		{"{tenant}.orders", []string{"tenant"}},
		{"api.{version}.orders.{order_id}.items", []string{"version", "order_id"}},
	}
	for _, tt := range tests {
		t.Run(tt.template, func(t *testing.T) {
			got := subjectTemplateTokens(tt.template)
			if tt.want == nil {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSubscribeSubject(t *testing.T) {
	tests := []struct {
		template, want string
	}{
		{"orders.Create", "orders.Create"},
		{"orders.{order_id}", "orders.*"},
		{"events.{region}.{event_type}", "events.*.*"},
		{"{tenant}.orders.{order_id}", "*.orders.*"},
	}
	for _, tt := range tests {
		t.Run(tt.template, func(t *testing.T) {
			assert.Equal(t, tt.want, subscribeSubject(tt.template))
		})
	}
}

func TestSnakeToCamel(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"order_id", "OrderID"},
		{"customer_id", "CustomerID"},
		{"event_type", "EventType"},
		{"name", "Name"},
		{"display_name", "DisplayName"},
		{"url", "URL"},
		{"api_url", "APIURL"},
		{"ip", "IP"},
		{"http", "HTTP"},
		{"api", "API"},
		{"", ""},
		{"a", "A"},
		{"user_api_url", "UserAPIURL"},
		{"ip_address", "IPAddress"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, snakeToCamel(tt.input))
		})
	}
}

func TestSubjectTemplateTokens_NoTokens(t *testing.T) {
	assert.Empty(t, subjectTemplateTokens("simple.subject"))
}

func TestSubjectTemplateTokens_Empty(t *testing.T) {
	assert.Empty(t, subjectTemplateTokens(""))
}

func TestSubscribeSubject_NoWildcards(t *testing.T) {
	assert.Equal(t, "orders.Create", subscribeSubject("orders.Create"))
}

func TestSubscribeSubject_Empty(t *testing.T) {
	assert.Equal(t, "", subscribeSubject(""))
}
