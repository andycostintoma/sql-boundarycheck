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

// DiscoverTables parses schema files and registers all CREATE TABLE declarations
// in the table index under the given context name.
func DiscoverTables(files []string, context string, idx TableIndex) []error {
	var errs []error
	for _, file := range files {
		e := discoverTablesInFile(file, context, idx)
		errs = append(errs, e...)
	}
	return errs
}

func discoverTablesInFile(file string, context string, idx TableIndex) []error {
	data, err := os.ReadFile(file)
	if err != nil {
		return []error{fmt.Errorf("reading %s: %w", file, err)}
	}

	result, err := pg_query.Parse(string(data))
	if err != nil {
		return []error{fmt.Errorf("parsing %s: %w", file, err)}
	}

	var errs []error
	for _, stmt := range result.Stmts {
		node := stmt.GetStmt()
		if node == nil {
			continue
		}

		create := node.GetCreateStmt()
		if create == nil || create.Relation == nil {
			continue
		}

		tableName := create.Relation.Relname
		if tableName == "" {
			continue
		}

		if err := idx.Register(tableName, context); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", filepath.Base(file), err))
		}
	}

	return errs
}

// CheckSchemaFiles parses the given schema files and reports cross-BC foreign keys.
// It checks both inline FK constraints in CREATE TABLE and ALTER TABLE ... ADD CONSTRAINT/FOREIGN KEY.
func CheckSchemaFiles(files []string, context string, idx TableIndex) ([]SchemaViolation, []error) {
	var violations []SchemaViolation
	var errs []error

	for _, file := range files {
		v, e := checkSchemaFile(file, context, idx)
		violations = append(violations, v...)
		errs = append(errs, e...)
	}

	return violations, errs
}

func checkSchemaFile(file string, context string, idx TableIndex) ([]SchemaViolation, []error) {
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

		// Check CREATE TABLE inline FKs.
		if create := node.GetCreateStmt(); create != nil {
			v, e := checkCreateStmt(create, relFile, context, idx)
			violations = append(violations, v...)
			errs = append(errs, e...)
		}

		// Check ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY / ADD FOREIGN KEY.
		if alter := node.GetAlterTableStmt(); alter != nil {
			v, e := checkAlterTableStmt(alter, relFile, idx)
			violations = append(violations, v...)
			errs = append(errs, e...)
		}
	}

	return violations, errs
}

func checkCreateStmt(create *pg_query.CreateStmt, relFile, context string, idx TableIndex) ([]SchemaViolation, []error) {
	sourceTable := ""
	if create.Relation != nil {
		sourceTable = create.Relation.Relname
	}
	if sourceTable == "" {
		return nil, nil
	}

	sourceCtx, sourceOk := idx.OwnerOf(sourceTable)
	if !sourceOk {
		return nil, []error{fmt.Errorf("%s: table %q not found in any context schema", relFile, sourceTable)}
	}

	jsonBytes, err := json.Marshal(create)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: marshaling %s AST: %w", relFile, sourceTable, err)}
	}

	var tree interface{}
	if err := json.Unmarshal(jsonBytes, &tree); err != nil {
		return nil, []error{fmt.Errorf("%s: unmarshaling %s AST: %w", relFile, sourceTable, err)}
	}

	fks := collectForeignKeys(tree)
	return evaluateFKs(fks, sourceTable, sourceCtx, relFile, idx)
}

func checkAlterTableStmt(alter *pg_query.AlterTableStmt, relFile string, idx TableIndex) ([]SchemaViolation, []error) {
	if alter.Relation == nil {
		return nil, nil
	}

	sourceTable := alter.Relation.Relname
	if sourceTable == "" {
		return nil, nil
	}

	sourceCtx, sourceOk := idx.OwnerOf(sourceTable)
	if !sourceOk {
		// Table might not be in this project's ownership — report as error.
		return nil, []error{fmt.Errorf("%s: ALTER TABLE on %q which is not found in any context schema", relFile, sourceTable)}
	}

	// Marshal each ALTER TABLE command to JSON and look for FK constraints.
	jsonBytes, err := json.Marshal(alter)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: marshaling ALTER TABLE %s AST: %w", relFile, sourceTable, err)}
	}

	var tree interface{}
	if err := json.Unmarshal(jsonBytes, &tree); err != nil {
		return nil, []error{fmt.Errorf("%s: unmarshaling ALTER TABLE %s AST: %w", relFile, sourceTable, err)}
	}

	fks := collectForeignKeys(tree)
	return evaluateFKs(fks, sourceTable, sourceCtx, relFile, idx)
}

func evaluateFKs(fks []foreignKeyRef, sourceTable, sourceCtx, relFile string, idx TableIndex) ([]SchemaViolation, []error) {
	var violations []SchemaViolation
	var errs []error

	for _, fk := range fks {
		targetCtx, targetOk := idx.OwnerOf(fk.targetTable)
		if !targetOk {
			errs = append(errs, fmt.Errorf(
				"%s: FK target table %q (from %s) not found in any context schema",
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
