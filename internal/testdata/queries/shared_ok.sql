-- name: InsertOutboxEvent :exec
-- Context "shared_ok" can touch outbox_events because it is shared.
INSERT INTO outbox_events (outbox_event_id, event_name) VALUES ($1, $2);
