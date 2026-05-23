package logger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootAndSystemLogsHaveBaseFields(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	if err := Init(cfg, "session-test"); err != nil {
		t.Fatalf("init logger: %v", err)
	}
	defer Close()

	Root().Info("event=root_started")
	System().Warn("event=system_warning")

	rootEntry := readLastEntry(t, filepath.Join(tmpDir, "root.log"))
	systemEntry := readLastEntry(t, filepath.Join(tmpDir, "system.log"))

	assertBaseFields(t, rootEntry, "root")
	assertBaseFields(t, systemEntry, "system")
}

func TestSQLHelperWritesStructuredEntry(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	if err := Init(cfg, "session-sql"); err != nil {
		t.Fatalf("init logger: %v", err)
	}
	defer Close()

	NewSQLHelper().Statement("migration:0001_enable_timescaledb.sql", 12, 0, SessionID())
	entry := readLastEntry(t, filepath.Join(tmpDir, "sql.log"))

	assertBaseFields(t, entry, "sql")
	if entry["statement"] != "migration:0001_enable_timescaledb.sql" {
		t.Fatalf("unexpected statement field: %v", entry["statement"])
	}
}

func readLastEntry(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		t.Fatalf("empty log file: %s", path)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	return entry
}

func assertBaseFields(t *testing.T, entry map[string]any, typ string) {
	t.Helper()
	if entry["type"] != typ {
		t.Fatalf("expected type=%s got %v", typ, entry["type"])
	}
	required := []string{"type", "level", "message", "session_id", "timestamp", "log_time"}
	for _, key := range required {
		if _, ok := entry[key]; !ok {
			t.Fatalf("expected field %s in entry", key)
		}
	}
}
