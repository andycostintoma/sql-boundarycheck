-- Cross-BC FK: patients (patient context) -> auth_users (auth context).
CREATE TABLE auth_users (
    user_id UUID PRIMARY KEY
);

CREATE TABLE patients (
    patient_id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES auth_users (user_id)
);
