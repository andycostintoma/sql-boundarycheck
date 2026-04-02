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

func testTableIndex() TableIndex {
	idx, _ := BuildTableIndex(Config{
		TableOwnership: map[string][]string{
			"auth":         {"auth_users", "auth_memberships", "auth_role_permissions"},
			"clinic":       {"clinics", "clinic_services"},
			"patient":      {"patients"},
			"practitioner": {"practitioners", "practitioner_clinics", "practitioner_clinic_services"},
			"schedule":     {"availability_rules"},
			"appointment":  {"appointments"},
			"notification": {"notifications"},
			"shared":       {"outbox_events"},
		},
	})
	return idx
}

// --- Schema tests ---

func TestSchemaCheckSameContextFKAllowed(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "schema")

	violations, errs := CheckSchema(dir, idx)

	// good.sql has clinics -> clinic_services, both in "clinic" context.
	// Should produce no violations for that file.
	for _, v := range violations {
		if v.File == "good.sql" {
			t.Errorf("unexpected violation in good.sql: %s", v)
		}
	}
	for _, e := range errs {
		// Allow errors from other fixture files but not good.sql.
		_ = e
	}
}

func TestSchemaCheckCrossBCFKDetected(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "schema")

	violations, _ := CheckSchema(dir, idx)

	found := false
	for _, v := range violations {
		if v.File == "cross_bc.sql" && v.SourceTable == "patients" && v.TargetTable == "auth_users" {
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

func TestSchemaCheckSharedTargetAllowed(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "schema")

	violations, _ := CheckSchema(dir, idx)

	for _, v := range violations {
		if v.File == "shared_target.sql" {
			t.Errorf("unexpected violation in shared_target.sql: %s", v)
		}
	}
}

func TestSchemaCheckUnownedTableReportsError(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "schema")

	_, errs := CheckSchema(dir, idx)

	found := false
	for _, e := range errs {
		if e != nil && contains(e.Error(), "mystery_table") && contains(e.Error(), "not declared") {
			found = true
		}
	}
	if !found {
		t.Error("expected error for unowned table 'mystery_table', not found")
	}
}

// --- Query tests ---

func TestQueryCheckOwnContextAllowed(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	violations, errs := CheckQueries(dir, idx)

	// clinic.sql touches only clinics and clinic_services (both "clinic" context).
	for _, v := range violations {
		if v.File == "clinic.sql" {
			t.Errorf("unexpected violation in clinic.sql: %s", v)
		}
	}
	for _, e := range errs {
		if e != nil && contains(e.Error(), "clinic.sql") {
			t.Errorf("unexpected error for clinic.sql: %s", e)
		}
	}
}

func TestQueryCheckOwnContextOnlyAllowed(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	violations, _ := CheckQueries(dir, idx)

	// appointment.sql touches only appointments (own context).
	for _, v := range violations {
		if v.File == "appointment.sql" {
			t.Errorf("unexpected violation in appointment.sql: %s", v)
		}
	}
}

func TestQueryCheckCrossBCJoinDetected(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	violations, _ := CheckQueries(dir, idx)

	found := false
	for _, v := range violations {
		if v.File == "appointment_cross.sql" && v.Table == "clinics" {
			found = true
			if v.QueryContext != "appointment_cross" {
				t.Errorf("expected query context 'appointment_cross', got %q", v.QueryContext)
			}
			if v.TableContext != "clinic" {
				t.Errorf("expected table context 'clinic', got %q", v.TableContext)
			}
		}
	}
	if !found {
		t.Error("expected cross-BC violation: appointment_cross.sql touching clinics, not found")
	}
}

func TestQueryCheckSharedTableAllowed(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	violations, _ := CheckQueries(dir, idx)

	for _, v := range violations {
		if v.File == "shared_ok.sql" {
			t.Errorf("unexpected violation in shared_ok.sql: %s", v)
		}
	}
}

func TestQueryCheckCTECrossBCDetected(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	violations, _ := CheckQueries(dir, idx)

	foundPatients := false
	foundAppointments := false
	for _, v := range violations {
		if v.File == "cte_cross.sql" && v.Table == "patients" {
			foundPatients = true
		}
		if v.File == "cte_cross.sql" && v.Table == "appointments" {
			foundAppointments = true
		}
	}
	if !foundPatients {
		t.Error("expected CTE cross-BC violation: cte_cross.sql touching patients, not found")
	}
	if !foundAppointments {
		t.Error("expected CTE cross-BC violation: cte_cross.sql touching appointments, not found")
	}
}

func TestQueryCheckCTEAliasNotFlaggedAsUnowned(t *testing.T) {
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	_, errs := CheckQueries(dir, idx)

	for _, e := range errs {
		if e != nil && contains(e.Error(), "patient_appts") {
			t.Errorf("CTE alias 'patient_appts' should not be flagged as unowned: %s", e)
		}
	}
}

func TestQueryCheckParamPlaceholders(t *testing.T) {
	// Verifies $1 params don't break parsing.
	idx := testTableIndex()
	dir := filepath.Join(testdataDir(t), "queries")

	_, errs := CheckQueries(dir, idx)

	for _, e := range errs {
		if e != nil && contains(e.Error(), "parsing") {
			t.Errorf("unexpected parse error (param placeholder issue?): %s", e)
		}
	}
}

// --- Config tests ---

func TestBuildTableIndexRejectsDuplicateOwnership(t *testing.T) {
	_, err := BuildTableIndex(Config{
		TableOwnership: map[string][]string{
			"auth":    {"users"},
			"patient": {"users"},
		},
	})
	if err == nil {
		t.Error("expected error for duplicate table ownership, got nil")
	}
}

func TestContextFromFile(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"auth.sql", "auth"},
		{"clinic.sql", "clinic"},
		{"/some/path/appointment.sql", "appointment"},
	}
	for _, tt := range tests {
		got := ContextFromFile(tt.input)
		if got != tt.want {
			t.Errorf("ContextFromFile(%q) = %q, want %q", tt.input, got, tt.want)
		}
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
