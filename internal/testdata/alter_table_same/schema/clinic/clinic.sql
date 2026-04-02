-- Same-context FK via ALTER TABLE: allowed.
CREATE TABLE clinics (
    clinic_id UUID PRIMARY KEY
);

CREATE TABLE clinic_services (
    service_id UUID PRIMARY KEY,
    clinic_id UUID NOT NULL
);

ALTER TABLE clinic_services
    ADD FOREIGN KEY (clinic_id) REFERENCES clinics (clinic_id);
