package gen_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mudomi/protonats/internal/gen"
	"github.com/mudomi/protonats/internal/gentest"
)

// TestGenerateFile_Golden checks the plugin output against the checked-in
// generated code. On mismatch, regenerate with protoc (see ci.yml) and commit.
func TestGenerateFile_Golden(t *testing.T) {
	plugin := gentest.NewPlugin(t)
	file := gentest.TestFile(t, plugin)

	if err := gen.GenerateFile(plugin, file); err != nil {
		t.Fatal(err)
	}

	resp := plugin.Response()
	if resp.Error != nil {
		t.Fatalf("plugin error: %s", resp.GetError())
	}
	if len(resp.File) != 1 {
		t.Fatalf("generated %d files, want 1", len(resp.File))
	}

	goldenPath := filepath.Join("testdata", "gen", "test_protonats.pb.go")
	golden, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if got := resp.File[0].GetContent(); got != string(golden) {
		t.Errorf("output differs from %s — regenerate it with protoc and commit.\n\nGot:\n%s", goldenPath, got)
	}
}
