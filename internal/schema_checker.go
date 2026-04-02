package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// SchemaViolation describes a cross-BC foreign key found in a schema file.
type SchemaViolation struct {
	File           string
	SourceTable    string
	SourceContext  string
	TargetTable    string
	TargetContext  string
	ConstraintName string
}

func (v SchemaViolation) String() string {
	name := v.ConstraintName
	if name == "" {
		name = "(unnamed)"
	}
	return fmt.Sprintf(
		"%s: FK %s on %s (%s) -> %s (%s)",
		v.File, name, v.SourceTable, v.SourceContext, v.TargetTable, v.TargetContext,
	)
}

// CheckSchema parses all .sql files in schemaDir and reports cross-BC foreign keys.
func CheckSchema(schemaDir string, idx TableIndex) ([]SchemaViolation, []error) {
	files, err := filepath.Glob(filepath.Join(schemaDir, "*.sql"))
	if err != nil {
		return nil, []error{fmt.Errorf("listing schema files: %w", err)}
	}
	if len(files) == 0 {
		return nil, []error{fmt.Errorf("no .sql files found in %s", schemaDir)}
	}

	var violations []SchemaViolation
	var errs []error

	for _, file := range files {
		v, e := checkSchemaFile(file, idx)
		violations = append(violations, v...)
		errs = append(errs, e...)
	}

	return violations, errs
}

func checkSchemaFile(file string, idx TableIndex) ([]SchemaViolation, []error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, []error{fmt.Errorf("reading %s: %w", file, err)}
	}

	result, err := pg_query.Parse(string(data))
	if err != nil {
		return nil, []error{fmt.Errorf("parsing %s: %w", file, err)}
	}

	relFile := filepath.Base(file)

	var violations []SchemaViolation
	var errs []error

	for _, stmt := range result.Stmts {
		node := stmt.GetStmt()
		if node == nil {
			continue
		}

		create := node.GetCreateStmt()
		if create == nil {
			continue
		}

		sourceTable := ""
		if create.Relation != nil {
			sourceTable = create.Relation.Relname
		}
		if sourceTable == "" {
			continue
		}

		sourceCtx, sourceOk := idx.OwnerOf(sourceTable)
		if !sourceOk {
			errs = append(errs, fmt.Errorf("%s: table %q is not declared in table_ownership", relFile, sourceTable))
			continue
		}

		// Marshal the entire CREATE TABLE to JSON so we can recursively
		// find all Constraint nodes with contype=CONSTR_FOREIGN.
		jsonBytes, err := json.Marshal(create)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: marshaling %s AST: %w", relFile, sourceTable, err))
			continue
		}

		var tree interface{}
		if err := json.Unmarshal(jsonBytes, &tree); err != nil {
			errs = append(errs, fmt.Errorf("%s: unmarshaling %s AST: %w", relFile, sourceTable, err))
			continue
		}

		fks := collectForeignKeys(tree)
		for _, fk := range fks {
			targetCtx, targetOk := idx.OwnerOf(fk.targetTable)
			if !targetOk {
				errs = append(errs, fmt.Errorf(
					"%s: FK target table %q (from %s) is not declared in table_ownership",
					relFile, fk.targetTable, sourceTable,
				))
				continue
			}

			if sourceCtx == targetCtx || idx.IsShared(fk.targetTable) {
				continue
			}

			if strings.EqualFold(sourceCtx, "shared") {
				continue
			}

			violations = append(violations, SchemaViolation{
				File:           relFile,
				SourceTable:    sourceTable,
				SourceContext:  sourceCtx,
				TargetTable:    fk.targetTable,
				TargetContext:  targetCtx,
				ConstraintName: fk.name,
			})
		}
	}

	return violations, errs
}

type foreignKeyRef struct {
	name        string
	targetTable string
}

// collectForeignKeys walks a JSON tree looking for Constraint objects
// with contype == 10 (CONSTR_FOREIGN) and extracts their pktable.relname.
func collectForeignKeys(node interface{}) []foreignKeyRef {
	var refs []foreignKeyRef

	switch v := node.(type) {
	case map[string]interface{}:
		// Check if this is a Constraint node with CONSTR_FOREIGN (contype=10).
		if contype, ok := v["contype"]; ok {
			if ct, ok := toFloat(contype); ok && ct == 10 {
				fk := foreignKeyRef{}
				if name, ok := v["conname"].(string); ok {
					fk.name = name
				}
				if pktable, ok := v["pktable"].(map[string]interface{}); ok {
					if relname, ok := pktable["relname"].(string); ok {
						fk.targetTable = relname
					}
				}
				if fk.targetTable != "" {
					refs = append(refs, fk)
				}
			}
		}
		// Recurse into all values.
		for _, val := range v {
			refs = append(refs, collectForeignKeys(val)...)
		}
	case []interface{}:
		for _, elem := range v {
			refs = append(refs, collectForeignKeys(elem)...)
		}
	}

	return refs
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
