SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_uid TEXT NOT NULL UNIQUE,
    protocol TEXT NOT NULL,
    source_ip TEXT,
    raw_message TEXT NOT NULL,
    parsed_data TEXT,
    delivery_status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    api_status INTEGER,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_protocol_created
ON messages(protocol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_delivery_status
ON messages(delivery_status, attempts, updated_at);
"""
