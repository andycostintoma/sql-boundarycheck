package internal

import (
	"fmt"
	"path/filepath"
)

// Result holds the combined output of all checks.
type Result struct {
	SchemaViolations []SchemaViolation
	QueryViolations  []QueryViolation
	Errors           []error
}

// HasViolations returns true if any violations were found.
func (r Result) HasViolations() bool {
	return len(r.SchemaViolations) > 0 || len(r.QueryViolations) > 0
}

// HasErrors returns true if any non-violation errors occurred.
func (r Result) HasErrors() bool {
	return len(r.Errors) > 0
}

// Run executes both schema and query boundary checks.
func Run(root, configPath string) Result {
	cfg, err := LoadConfig(filepath.Join(root, configPath))
	if err != nil {
		return Result{Errors: []error{err}}
	}

	var result Result

	// Phase 1: Discover all tables from schema files to build the index.
	idx := NewTableIndex()
	for ctxName, ctxCfg := range cfg.Contexts {
		files, err := ResolveSQLFiles(root, ctxCfg.Schema)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("context %q schema: %w", ctxName, err))
			continue
		}
		errs := DiscoverTables(files, ctxName, idx)
		result.Errors = append(result.Errors, errs...)
	}

	// If there were table discovery errors, still proceed with what we have.

	// Phase 2: Check schema files for FK violations (inline + ALTER TABLE).
	for ctxName, ctxCfg := range cfg.Contexts {
		files, err := ResolveSQLFiles(root, ctxCfg.Schema)
		if err != nil {
			// Already reported in phase 1.
			continue
		}
		sv, se := CheckSchemaFiles(files, ctxName, idx)
		result.SchemaViolations = append(result.SchemaViolations, sv...)
		result.Errors = append(result.Errors, se...)
	}

	// Phase 3: Check query files for cross-BC table references.
	for ctxName, ctxCfg := range cfg.Contexts {
		if ctxCfg.Queries == "" {
			continue
		}
		files, err := ResolveSQLFiles(root, ctxCfg.Queries)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("context %q queries: %w", ctxName, err))
			continue
		}
		qv, qe := CheckQueryFiles(files, ctxName, idx)
		result.QueryViolations = append(result.QueryViolations, qv...)
		result.Errors = append(result.Errors, qe...)
	}

	return result
}

// FormatResult produces a human-readable summary of violations and errors.
func FormatResult(r Result) string {
	var out string

	if len(r.SchemaViolations) > 0 {
		out += fmt.Sprintf("Schema FK violations (%d):\n", len(r.SchemaViolations))
		for _, v := range r.SchemaViolations {
			out += fmt.Sprintf("  %s\n", v)
		}
	}

	if len(r.QueryViolations) > 0 {
		out += fmt.Sprintf("Query boundary violations (%d):\n", len(r.QueryViolations))
		for _, v := range r.QueryViolations {
			out += fmt.Sprintf("  %s\n", v)
		}
	}

	if len(r.Errors) > 0 {
		out += fmt.Sprintf("Errors (%d):\n", len(r.Errors))
		for _, e := range r.Errors {
			out += fmt.Sprintf("  %s\n", e)
		}
	}

	if !r.HasViolations() && !r.HasErrors() {
		out = "No boundary violations found.\n"
	}

	return out
}
