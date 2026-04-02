-- Cross-BC FK: patients (patient) -> auth_users (auth).
CREATE TABLE patients (
    patient_id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES auth_users (user_id)
);
