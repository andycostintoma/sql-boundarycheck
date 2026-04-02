package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// QueryViolation describes a query that touches a table owned by another context.
type QueryViolation struct {
	File         string
	QueryContext string
	Table        string
	TableContext string
}

func (v QueryViolation) String() string {
	return fmt.Sprintf(
		"%s (context %q): references table %q owned by %q",
		v.File, v.QueryContext, v.Table, v.TableContext,
	)
}

// CheckQueryFiles parses the given query files and reports cross-BC table references.
// The context is the owning bounded context for all queries in these files.
func CheckQueryFiles(files []string, context string, idx TableIndex) ([]QueryViolation, []error) {
	var violations []QueryViolation
	var errs []error

	for _, file := range files {
		v, e := checkQueryFile(file, context, idx)
		violations = append(violations, v...)
		errs = append(errs, e...)
	}

	return violations, errs
}

func checkQueryFile(file string, queryCtx string, idx TableIndex) ([]QueryViolation, []error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, []error{fmt.Errorf("reading %s: %w", file, err)}
	}

	result, err := pg_query.Parse(string(data))
	if err != nil {
		return nil, []error{fmt.Errorf("parsing %s: %w", file, err)}
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return nil, []error{fmt.Errorf("marshaling AST for %s: %w", file, err)}
	}

	var tree interface{}
	if err := json.Unmarshal(jsonBytes, &tree); err != nil {
		return nil, []error{fmt.Errorf("unmarshaling AST for %s: %w", file, err)}
	}

	relFile := filepath.Base(file)

	cteNames := make(map[string]bool)
	collectCTENames(tree, cteNames)

	tables := make(map[string]bool)
	collectRangeVarNames(tree, tables)

	for name := range cteNames {
		delete(tables, name)
	}

	var violations []QueryViolation
	var errs []error

	for table := range tables {
		owner, ok := idx.OwnerOf(table)
		if !ok {
			errs = append(errs, fmt.Errorf("%s: table %q is not found in any context schema", relFile, table))
			continue
		}

		if owner == queryCtx || idx.IsShared(table) {
			continue
		}

		violations = append(violations, QueryViolation{
			File:         relFile,
			QueryContext: queryCtx,
			Table:        table,
			TableContext: owner,
		})
	}

	return violations, errs
}

// collectCTENames recursively walks a JSON tree and collects all
// "ctename" values found inside "CommonTableExpr" objects.
func collectCTENames(node interface{}, names map[string]bool) {
	switch v := node.(type) {
	case map[string]interface{}:
		if cte, ok := v["CommonTableExpr"]; ok {
			if cteMap, ok := cte.(map[string]interface{}); ok {
				if name, ok := cteMap["ctename"].(string); ok && name != "" {
					names[name] = true
				}
			}
		}
		for _, val := range v {
			collectCTENames(val, names)
		}
	case []interface{}:
		for _, elem := range v {
			collectCTENames(elem, names)
		}
	}
}

// collectRangeVarNames recursively walks a JSON tree and collects all
// "relname" values found inside "RangeVar" objects.
func collectRangeVarNames(node interface{}, tables map[string]bool) {
	switch v := node.(type) {
	case map[string]interface{}:
		if rv, ok := v["RangeVar"]; ok {
			if rvMap, ok := rv.(map[string]interface{}); ok {
				if relname, ok := rvMap["relname"].(string); ok && relname != "" {
					tables[relname] = true
				}
			}
		}
		for _, val := range v {
			collectRangeVarNames(val, tables)
		}
	case []interface{}:
		for _, elem := range v {
			collectRangeVarNames(elem, tables)
		}
	}
}
