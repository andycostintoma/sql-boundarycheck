package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ContextConfig describes the schema and query locations for a single bounded context.
type ContextConfig struct {
	Schema  string `yaml:"schema"`
	Queries string `yaml:"queries"`
}

// Config represents the .sqlboundarycheck.yaml configuration.
type Config struct {
	Contexts map[string]ContextConfig `yaml:"contexts"`
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

	if len(cfg.Contexts) == 0 {
		return Config{}, fmt.Errorf("config %s: contexts must define at least one entry", path)
	}

	for name, ctx := range cfg.Contexts {
		if ctx.Schema == "" {
			return Config{}, fmt.Errorf("config %s: context %q must have a schema path", path, name)
		}
	}

	return cfg, nil
}

// ResolveSQLFiles resolves a path that may be a single .sql file or a directory
// containing .sql files. Returns the list of absolute file paths.
func ResolveSQLFiles(root, path string) ([]string, error) {
	abs := filepath.Join(root, path)

	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("resolving %s: %w", path, err)
	}

	if !info.IsDir() {
		return []string{abs}, nil
	}

	files, err := filepath.Glob(filepath.Join(abs, "*.sql"))
	if err != nil {
		return nil, fmt.Errorf("listing .sql files in %s: %w", path, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .sql files found in %s", path)
	}

	return files, nil
}

// TableIndex maps every discovered table to its owning context.
type TableIndex struct {
	owners map[string]string
}

// NewTableIndex creates an empty table index.
func NewTableIndex() TableIndex {
	return TableIndex{owners: make(map[string]string)}
}

// Register adds a table to the index. Returns an error if the table is already owned.
func (idx TableIndex) Register(table, context string) error {
	if existing, ok := idx.owners[table]; ok {
		return fmt.Errorf("table %q claimed by both %q and %q", table, existing, context)
	}
	idx.owners[table] = context
	return nil
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
