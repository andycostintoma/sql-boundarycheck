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

	idx, err := BuildTableIndex(cfg)
	if err != nil {
		return Result{Errors: []error{err}}
	}

	var result Result

	schemaDir := filepath.Join(root, cfg.SchemaDir)
	sv, se := CheckSchema(schemaDir, idx)
	result.SchemaViolations = sv
	result.Errors = append(result.Errors, se...)

	queriesDir := filepath.Join(root, cfg.QueriesDir)
	qv, qe := CheckQueries(queriesDir, idx)
	result.QueryViolations = qv
	result.Errors = append(result.Errors, qe...)

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
