-- FK to a shared table: notifications -> outbox_events (shared context).
CREATE TABLE outbox_events (
    outbox_event_id UUID PRIMARY KEY
);

CREATE TABLE notifications (
    notification_id UUID PRIMARY KEY,
    outbox_event_id UUID REFERENCES outbox_events (outbox_event_id)
);
