package gents

import "testing"

func TestSnakeToCamel(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"order_id", "orderId"},
		{"warehouse_id", "warehouseId"},
		{"status", "status"},
		{"event_id", "eventId"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := snakeToCamel(tt.in); got != tt.want {
			t.Errorf("snakeToCamel(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPascalToCamel(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"GetOrder", "getOrder"},
		{"Echo", "echo"},
		{"ProcessEvent", "processEvent"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := pascalToCamel(tt.in); got != tt.want {
			t.Errorf("pascalToCamel(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSubjectTemplateLiteral(t *testing.T) {
	tests := []struct {
		template string
		tokens   []string
		want     string
	}{
		{"testpkg.Echo", nil, `"testpkg.Echo"`},
		{"items.{item_id}", []string{"item_id"}, "`items.${req.itemId}`"},
		{"orders.{order_id}.{status}", []string{"order_id", "status"}, "`orders.${req.orderId}.${req.status}`"},
	}
	for _, tt := range tests {
		if got := subjectTemplateLiteral(tt.template, tt.tokens); got != tt.want {
			t.Errorf("subjectTemplateLiteral(%q, %v) = %q, want %q", tt.template, tt.tokens, got, tt.want)
		}
	}
}

func TestSubscribeSubject(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"orders.*", "orders.*"},
		{"items.{item_id}", "items.*"},
		{"orders.{order_id}.{status}", "orders.*.*"},
		{"testpkg.Echo", "testpkg.Echo"},
	}
	for _, tt := range tests {
		if got := subscribeSubject(tt.in); got != tt.want {
			t.Errorf("subscribeSubject(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
