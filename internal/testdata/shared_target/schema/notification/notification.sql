-- FK to a shared table: allowed.
CREATE TABLE notifications (
    notification_id UUID PRIMARY KEY,
    outbox_event_id UUID REFERENCES outbox_events (outbox_event_id)
);
