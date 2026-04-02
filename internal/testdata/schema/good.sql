-- Same-context FK: clinic_services -> clinics (both "clinic" context).
CREATE TABLE clinics (
    clinic_id UUID PRIMARY KEY
);

CREATE TABLE clinic_services (
    service_id UUID PRIMARY KEY,
    clinic_id UUID NOT NULL REFERENCES clinics (clinic_id)
);
