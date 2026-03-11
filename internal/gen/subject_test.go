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
		// Simple single-word fields
		{"name", "Name"},
		{"status", "Status"},
		{"a", "A"},
		{"", ""},

		// Common _id suffix fields — proto uses "Id" not "ID"
		{"order_id", "OrderId"},
		{"customer_id", "CustomerId"},
		{"warehouse_id", "WarehouseId"},
		{"product_id", "ProductId"},
		{"user_id", "UserId"},
		{"tenant_id", "TenantId"},
		{"reservation_id", "ReservationId"},
		{"id", "Id"},

		// Multi-word fields
		{"event_type", "EventType"},
		{"display_name", "DisplayName"},
		{"created_at", "CreatedAt"},
		{"updated_at", "UpdatedAt"},
		{"price_cents", "PriceCents"},
		{"max_retries", "MaxRetries"},
		{"is_active", "IsActive"},

		// Acronym-like words — proto does NOT uppercase them
		{"url", "Url"},
		{"api_url", "ApiUrl"},
		{"ip", "Ip"},
		{"ip_address", "IpAddress"},
		{"http", "Http"},
		{"http_method", "HttpMethod"},
		{"api", "Api"},
		{"api_key", "ApiKey"},
		{"user_api_url", "UserApiUrl"},
		{"ssl_enabled", "SslEnabled"},
		{"tcp_port", "TcpPort"},

		// Three or more segments
		{"first_middle_last", "FirstMiddleLast"},
		{"max_ack_pending", "MaxAckPending"},
		{"x_request_id", "XRequestId"},
		{"content_type_json", "ContentTypeJson"},

		// Leading/trailing/double underscores
		{"_leading", "Leading"},
		{"trailing_", "Trailing"},
		{"double__underscore", "DoubleUnderscore"},
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
