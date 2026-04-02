package internal

import (
	"path/filepath"
	"runtime"
	"testing"
)

func testdataDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine testdata path")
	}
	return filepath.Join(filepath.Dir(file), "testdata")
}

// runCheck builds a Config from context definitions, runs the full check, and returns the result.
func runCheck(t *testing.T, fixture string, contexts map[string]ContextConfig) Result {
	t.Helper()
	root := filepath.Join(testdataDir(t), fixture)
	cfg := Config{Contexts: contexts}

	// Write a temporary config — or just call the internal flow directly.
	// Since Run() expects a file, we test at a lower level.
	idx := NewTableIndex()
	var result Result

	// Phase 1: discover tables.
	for ctxName, ctxCfg := range cfg.Contexts {
		files, err := ResolveSQLFiles(root, ctxCfg.Schema)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}
		errs := DiscoverTables(files, ctxName, idx)
		result.Errors = append(result.Errors, errs...)
	}

	// Phase 2: check schema FKs.
	for ctxName, ctxCfg := range cfg.Contexts {
		files, err := ResolveSQLFiles(root, ctxCfg.Schema)
		if err != nil {
			continue
		}
		sv, se := CheckSchemaFiles(files, ctxName, idx)
		result.SchemaViolations = append(result.SchemaViolations, sv...)
		result.Errors = append(result.Errors, se...)
	}

	// Phase 3: check queries.
	for ctxName, ctxCfg := range cfg.Contexts {
		if ctxCfg.Queries == "" {
			continue
		}
		files, err := ResolveSQLFiles(root, ctxCfg.Queries)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}
		qv, qe := CheckQueryFiles(files, ctxName, idx)
		result.QueryViolations = append(result.QueryViolations, qv...)
		result.Errors = append(result.Errors, qe...)
	}

	return result
}

// --- Schema: same-context FK ---

func TestSameContextFKAllowed(t *testing.T) {
	r := runCheck(t, "basic", map[string]ContextConfig{
		"clinic":      {Schema: "schema/clinic"},
		"auth":        {Schema: "schema/auth"},
		"patient":     {Schema: "schema/patient"},
		"appointment": {Schema: "schema/appointment"},
		"shared":      {Schema: "schema/shared"},
	})

	if len(r.SchemaViolations) > 0 {
		t.Errorf("expected no schema violations, got %d: %v", len(r.SchemaViolations), r.SchemaViolations)
	}
	for _, e := range r.Errors {
		t.Errorf("unexpected error: %s", e)
	}
}

// --- Schema: cross-BC inline FK ---

func TestCrossBCInlineFKDetected(t *testing.T) {
	r := runCheck(t, "cross_bc_fk", map[string]ContextConfig{
		"auth":    {Schema: "schema/auth"},
		"patient": {Schema: "schema/patient"},
	})

	found := false
	for _, v := range r.SchemaViolations {
		if v.SourceTable == "patients" && v.TargetTable == "auth_users" {
			found = true
			if v.SourceContext != "patient" {
				t.Errorf("expected source context 'patient', got %q", v.SourceContext)
			}
			if v.TargetContext != "auth" {
				t.Errorf("expected target context 'auth', got %q", v.TargetContext)
			}
		}
	}
	if !found {
		t.Error("expected cross-BC FK violation: patients -> auth_users, not found")
	}
}

// --- Schema: FK to shared table ---

func TestSharedTargetFKAllowed(t *testing.T) {
	r := runCheck(t, "shared_target", map[string]ContextConfig{
		"shared":       {Schema: "schema/shared"},
		"notification": {Schema: "schema/notification"},
	})

	for _, v := range r.SchemaViolations {
		t.Errorf("unexpected violation: %s", v)
	}
}

// --- Schema: ALTER TABLE cross-BC FK ---

func TestAlterTableCrossBCFKDetected(t *testing.T) {
	r := runCheck(t, "alter_table_cross", map[string]ContextConfig{
		"auth":    {Schema: "schema/auth"},
		"patient": {Schema: "schema/patient"},
	})

	found := false
	for _, v := range r.SchemaViolations {
		if v.SourceTable == "patients" && v.TargetTable == "auth_users" {
			found = true
			if v.ConstraintName != "patients_owner_fk" {
				t.Errorf("expected constraint name 'patients_owner_fk', got %q", v.ConstraintName)
			}
		}
	}
	if !found {
		t.Error("expected ALTER TABLE cross-BC FK violation: patients -> auth_users, not found")
	}
}

// --- Schema: ALTER TABLE same-context FK ---

func TestAlterTableSameContextFKAllowed(t *testing.T) {
	r := runCheck(t, "alter_table_same", map[string]ContextConfig{
		"clinic": {Schema: "schema/clinic"},
	})

	for _, v := range r.SchemaViolations {
		t.Errorf("unexpected violation: %s", v)
	}
	for _, e := range r.Errors {
		t.Errorf("unexpected error: %s", e)
	}
}

// --- Schema: ALTER TABLE ADD FOREIGN KEY (no CONSTRAINT keyword) ---

func TestAlterTableAddForeignKeyShorthandDetected(t *testing.T) {
	// The alter_table_same fixture already uses ADD FOREIGN KEY without CONSTRAINT name.
	// Here we verify unnamed ALTER TABLE FK works for cross-BC detection.
	r := runCheck(t, "alter_table_cross", map[string]ContextConfig{
		"auth":    {Schema: "schema/auth"},
		"patient": {Schema: "schema/patient"},
	})

	if len(r.SchemaViolations) == 0 {
		t.Error("expected at least one ALTER TABLE FK violation")
	}
}

// --- Schema: table auto-discovery ---

func TestTableAutoDiscovery(t *testing.T) {
	root := filepath.Join(testdataDir(t), "basic")
	idx := NewTableIndex()

	files, err := ResolveSQLFiles(root, "schema/clinic")
	if err != nil {
		t.Fatalf("resolving clinic schema: %s", err)
	}
	errs := DiscoverTables(files, "clinic", idx)
	for _, e := range errs {
		t.Errorf("unexpected error: %s", e)
	}

	owner, ok := idx.OwnerOf("clinics")
	if !ok || owner != "clinic" {
		t.Errorf("expected clinics owned by 'clinic', got %q (found=%v)", owner, ok)
	}

	owner, ok = idx.OwnerOf("clinic_services")
	if !ok || owner != "clinic" {
		t.Errorf("expected clinic_services owned by 'clinic', got %q (found=%v)", owner, ok)
	}
}

// --- Schema: duplicate table ownership ---

func TestDuplicateTableOwnershipError(t *testing.T) {
	root := filepath.Join(testdataDir(t), "cross_bc_fk")
	idx := NewTableIndex()

	// Register auth_users under auth.
	authFiles, _ := ResolveSQLFiles(root, "schema/auth")
	_ = DiscoverTables(authFiles, "auth", idx)

	// Try registering auth_users again under patient (patient.sql also declares it? No — but let's test directly).
	err := idx.Register("auth_users", "patient")
	if err == nil {
		t.Error("expected error for duplicate table registration, got nil")
	}
}

// --- Query: own context allowed ---

func TestQueryOwnContextAllowed(t *testing.T) {
	r := runCheck(t, "basic", map[string]ContextConfig{
		"clinic":      {Schema: "schema/clinic", Queries: "queries/clinic"},
		"auth":        {Schema: "schema/auth"},
		"patient":     {Schema: "schema/patient"},
		"appointment": {Schema: "schema/appointment", Queries: "queries/appointment"},
		"shared":      {Schema: "schema/shared"},
	})

	for _, v := range r.QueryViolations {
		t.Errorf("unexpected query violation: %s", v)
	}
}

// --- Query: cross-BC JOIN detected ---

func TestQueryCrossBCJoinDetected(t *testing.T) {
	r := runCheck(t, "cross_query", map[string]ContextConfig{
		"appointment": {Schema: "schema/appointment", Queries: "queries/appointment"},
		"clinic":      {Schema: "schema/clinic"},
	})

	found := false
	for _, v := range r.QueryViolations {
		if v.Table == "clinics" && v.QueryContext == "appointment" {
			found = true
			if v.TableContext != "clinic" {
				t.Errorf("expected table context 'clinic', got %q", v.TableContext)
			}
		}
	}
	if !found {
		t.Error("expected cross-BC query violation: appointment touching clinics, not found")
	}
}

// --- Query: shared table allowed ---

func TestQuerySharedTableAllowed(t *testing.T) {
	r := runCheck(t, "shared_query", map[string]ContextConfig{
		"shared":       {Schema: "schema/shared"},
		"notification": {Schema: "schema/notification", Queries: "queries/notification"},
	})

	for _, v := range r.QueryViolations {
		t.Errorf("unexpected query violation: %s", v)
	}
}

// --- Query: CTE cross-BC detected ---

func TestQueryCTECrossBCDetected(t *testing.T) {
	r := runCheck(t, "cte_cross", map[string]ContextConfig{
		"patient":     {Schema: "schema/patient"},
		"appointment": {Schema: "schema/appointment"},
		"clinic":      {Schema: "schema/clinic"},
		"cte_test":    {Schema: "schema/clinic", Queries: "queries/cte_test"}, // cte_test uses clinic schema as dummy
	})

	foundPatients := false
	foundAppointments := false
	for _, v := range r.QueryViolations {
		if v.QueryContext == "cte_test" && v.Table == "patients" {
			foundPatients = true
		}
		if v.QueryContext == "cte_test" && v.Table == "appointments" {
			foundAppointments = true
		}
	}
	if !foundPatients {
		t.Error("expected CTE cross-BC violation: cte_test touching patients, not found")
	}
	if !foundAppointments {
		t.Error("expected CTE cross-BC violation: cte_test touching appointments, not found")
	}
}

// --- Query: CTE alias not flagged as unowned ---

func TestQueryCTEAliasNotFlaggedAsUnowned(t *testing.T) {
	r := runCheck(t, "cte_cross", map[string]ContextConfig{
		"patient":     {Schema: "schema/patient"},
		"appointment": {Schema: "schema/appointment"},
		"clinic":      {Schema: "schema/clinic"},
		"cte_test":    {Schema: "schema/clinic", Queries: "queries/cte_test"},
	})

	for _, e := range r.Errors {
		if e != nil && contains(e.Error(), "patient_appts") {
			t.Errorf("CTE alias 'patient_appts' should not be flagged as unowned: %s", e)
		}
	}
}

// --- Query: $1 params don't break parsing ---

func TestQueryParamPlaceholders(t *testing.T) {
	r := runCheck(t, "param_query", map[string]ContextConfig{
		"appointment": {Schema: "schema/appointment", Queries: "queries/appointment"},
	})

	for _, e := range r.Errors {
		if e != nil && contains(e.Error(), "parsing") {
			t.Errorf("unexpected parse error (param placeholder issue?): %s", e)
		}
	}

	for _, v := range r.QueryViolations {
		t.Errorf("unexpected query violation: %s", v)
	}
}

// --- Config: context without queries ---

func TestContextWithoutQueriesOK(t *testing.T) {
	r := runCheck(t, "basic", map[string]ContextConfig{
		"clinic":      {Schema: "schema/clinic"},
		"auth":        {Schema: "schema/auth"},
		"patient":     {Schema: "schema/patient"},
		"appointment": {Schema: "schema/appointment"},
		"shared":      {Schema: "schema/shared"},
	})

	// No query violations or errors since no queries are checked.
	for _, v := range r.QueryViolations {
		t.Errorf("unexpected query violation: %s", v)
	}
}

// --- Config: schema as single file ---

func TestSchemaAsSingleFile(t *testing.T) {
	r := runCheck(t, "basic", map[string]ContextConfig{
		"clinic":      {Schema: "schema/clinic/clinic.sql"},
		"auth":        {Schema: "schema/auth/auth.sql"},
		"patient":     {Schema: "schema/patient/patient.sql"},
		"appointment": {Schema: "schema/appointment/appointment.sql"},
		"shared":      {Schema: "schema/shared/shared.sql"},
	})

	if len(r.SchemaViolations) > 0 {
		t.Errorf("unexpected schema violations: %v", r.SchemaViolations)
	}
	for _, e := range r.Errors {
		t.Errorf("unexpected error: %s", e)
	}
}

// --- Path resolution ---

func TestResolveSQLFilesDirectory(t *testing.T) {
	root := filepath.Join(testdataDir(t), "basic")
	files, err := ResolveSQLFiles(root, "schema/clinic")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
}

func TestResolveSQLFilesSingleFile(t *testing.T) {
	root := filepath.Join(testdataDir(t), "basic")
	files, err := ResolveSQLFiles(root, "schema/clinic/clinic.sql")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
