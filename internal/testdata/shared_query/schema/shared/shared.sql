CREATE TABLE outbox_events (
    outbox_event_id UUID PRIMARY KEY,
    event_name TEXT NOT NULL
);
