package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the .sqlboundarycheck.yaml configuration.
type Config struct {
	SchemaDir      string              `yaml:"schema_dir"`
	QueriesDir     string              `yaml:"queries_dir"`
	TableOwnership map[string][]string `yaml:"table_ownership"`
}

// LoadConfig reads and parses the YAML config file.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config %s: %w", path, err)
	}

	if cfg.SchemaDir == "" {
		return Config{}, fmt.Errorf("config %s: schema_dir is required", path)
	}
	if cfg.QueriesDir == "" {
		return Config{}, fmt.Errorf("config %s: queries_dir is required", path)
	}
	if len(cfg.TableOwnership) == 0 {
		return Config{}, fmt.Errorf("config %s: table_ownership must define at least one context", path)
	}

	return cfg, nil
}

// TableIndex maps every declared table to its owning context.
type TableIndex struct {
	owners map[string]string // table_name -> context
}

// BuildTableIndex constructs a reverse lookup from the config.
func BuildTableIndex(cfg Config) (TableIndex, error) {
	idx := TableIndex{owners: make(map[string]string)}
	for ctx, tables := range cfg.TableOwnership {
		for _, t := range tables {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if existing, ok := idx.owners[t]; ok {
				return TableIndex{}, fmt.Errorf("table %q claimed by both %q and %q", t, existing, ctx)
			}
			idx.owners[t] = ctx
		}
	}
	return idx, nil
}

// OwnerOf returns the owning context of a table and whether it was found.
func (idx TableIndex) OwnerOf(table string) (string, bool) {
	ctx, ok := idx.owners[table]
	return ctx, ok
}

// IsShared returns true if the table belongs to the "shared" context.
func (idx TableIndex) IsShared(table string) bool {
	return idx.owners[table] == "shared"
}

// ContextFromFile derives the bounded-context name from a query filename.
// "auth.sql" -> "auth", "clinic.sql" -> "clinic".
func ContextFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}
