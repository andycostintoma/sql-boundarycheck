-- Cross-BC FK via ALTER TABLE.
CREATE TABLE patients (
    patient_id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL
);

ALTER TABLE patients
    ADD CONSTRAINT patients_owner_fk
    FOREIGN KEY (owner_user_id) REFERENCES auth_users (user_id);
