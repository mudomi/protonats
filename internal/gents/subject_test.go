package gents

import "testing"

func TestPascalToCamel(t *testing.T) {
	cases := map[string]string{
		"GetOrder": "getOrder",
		"Echo":     "echo",
		"A":        "a",
		"":         "",
	}
	for in, want := range cases {
		if got := pascalToCamel(in); got != want {
			t.Errorf("pascalToCamel(%q) = %q, want %q", in, got, want)
		}
	}
}
