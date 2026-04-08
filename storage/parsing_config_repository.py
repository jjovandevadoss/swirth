"""
Parser Configuration Repository
Handles CRUD operations for parser configuration profiles.
"""

import json
import sqlite3
from datetime import UTC, datetime
from typing import Dict, Any, List, Optional


class ParsingConfigRepository:
    """Repository for managing parser configuration profiles."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with Row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def create_config(
        self,
        name: str,
        protocol: str,
        description: Optional[str] = None,
        sender_filter: Optional[str] = None,
        is_active: bool = False,
        enable_escape_decoding: bool = True,
        custom_loinc_codes: Optional[Dict[str, str]] = None,
        field_mappings: Optional[Dict[str, Any]] = None,
        delimiter_overrides: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a new parser configuration profile."""
        now = datetime.now(UTC).isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO parser_configs 
                (name, description, protocol, sender_filter, is_active, 
                 enable_escape_decoding, custom_loinc_codes, field_mappings, 
                 delimiter_overrides, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    description,
                    protocol,
                    sender_filter,
                    1 if is_active else 0,
                    1 if enable_escape_decoding else 0,
                    json.dumps(custom_loinc_codes or {}),
                    json.dumps(field_mappings or {}),
                    json.dumps(delimiter_overrides or {}),
                    now,
                    now,
                ),
            )
            config_id = cursor.lastrowid
            conn.commit()

        return self.get_config(config_id)

    def get_config(self, config_id: int) -> Optional[Dict[str, Any]]:
        """Get a parser configuration by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM parser_configs WHERE id = ?", (config_id,)
            ).fetchone()

        if not row:
            return None

        return self._row_to_dict(row)

    def get_config_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a parser configuration by name."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM parser_configs WHERE name = ?", (name,)
            ).fetchone()

        if not row:
            return None

        return self._row_to_dict(row)

    def get_active_config(
        self, protocol: str, sender_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the active parser configuration for a protocol and optional sender.
        
        Precedence:
        1. Active config with matching sender_filter
        2. Active config with NULL sender_filter (default for protocol)
        3. Active config with protocol='ALL'
        """
        with self._get_connection() as conn:
            # First try to find sender-specific config
            if sender_name:
                row = conn.execute(
                    """
                    SELECT * FROM parser_configs 
                    WHERE is_active = 1 
                      AND (protocol = ? OR protocol = 'ALL')
                      AND sender_filter = ?
                    ORDER BY 
                      CASE WHEN protocol = ? THEN 1 ELSE 2 END,
                      created_at DESC
                    LIMIT 1
                    """,
                    (protocol, sender_name, protocol),
                ).fetchone()
                
                if row:
                    return self._row_to_dict(row)
            
            # Fallback to default config for protocol
            row = conn.execute(
                """
                SELECT * FROM parser_configs 
                WHERE is_active = 1 
                  AND (protocol = ? OR protocol = 'ALL')
                  AND sender_filter IS NULL
                ORDER BY 
                  CASE WHEN protocol = ? THEN 1 ELSE 2 END,
                  created_at DESC
                LIMIT 1
                """,
                (protocol, protocol),
            ).fetchone()

        if not row:
            return None

        return self._row_to_dict(row)

    def list_configs(self, protocol: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all parser configurations, optionally filtered by protocol."""
        with self._get_connection() as conn:
            if protocol:
                rows = conn.execute(
                    """
                    SELECT * FROM parser_configs 
                    WHERE protocol = ? OR protocol = 'ALL'
                    ORDER BY is_active DESC, updated_at DESC
                    """,
                    (protocol,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM parser_configs ORDER BY is_active DESC, updated_at DESC"
                ).fetchall()

        return [self._row_to_dict(row) for row in rows]

    def update_config(
        self,
        config_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        protocol: Optional[str] = None,
        sender_filter: Optional[str] = None,
        is_active: Optional[bool] = None,
        enable_escape_decoding: Optional[bool] = None,
        custom_loinc_codes: Optional[Dict[str, str]] = None,
        field_mappings: Optional[Dict[str, Any]] = None,
        delimiter_overrides: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update an existing parser configuration."""
        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if protocol is not None:
            updates.append("protocol = ?")
            params.append(protocol)
        if sender_filter is not None:
            updates.append("sender_filter = ?")
            params.append(sender_filter)
        if is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)
        if enable_escape_decoding is not None:
            updates.append("enable_escape_decoding = ?")
            params.append(1 if enable_escape_decoding else 0)
        if custom_loinc_codes is not None:
            updates.append("custom_loinc_codes = ?")
            params.append(json.dumps(custom_loinc_codes))
        if field_mappings is not None:
            updates.append("field_mappings = ?")
            params.append(json.dumps(field_mappings))
        if delimiter_overrides is not None:
            updates.append("delimiter_overrides = ?")
            params.append(json.dumps(delimiter_overrides))

        if not updates:
            return self.get_config(config_id)

        updates.append("updated_at = ?")
        params.append(datetime.now(UTC).isoformat())
        params.append(config_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE parser_configs SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            conn.commit()

        return self.get_config(config_id)

    def delete_config(self, config_id: int) -> bool:
        """Delete a parser configuration."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM parser_configs WHERE id = ?", (config_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Convert a database row to a dictionary with JSON parsing."""
        data = dict(row)
        
        # Parse JSON fields
        if data.get('custom_loinc_codes'):
            data['custom_loinc_codes'] = json.loads(data['custom_loinc_codes'])
        if data.get('field_mappings'):
            data['field_mappings'] = json.loads(data['field_mappings'])
        if data.get('delimiter_overrides'):
            data['delimiter_overrides'] = json.loads(data['delimiter_overrides'])
        
        # Convert integer booleans
        data['is_active'] = bool(data.get('is_active'))
        data['enable_escape_decoding'] = bool(data.get('enable_escape_decoding'))
        
        return data

    def export_config(self, config_id: int) -> Optional[str]:
        """Export a parser configuration as JSON string."""
        config = self.get_config(config_id)
        if not config:
            return None
        
        # Remove DB-specific fields
        export_data = {k: v for k, v in config.items() 
                      if k not in ['id', 'created_at', 'updated_at']}
        
        return json.dumps(export_data, indent=2)

    def import_config(self, config_json: str) -> Dict[str, Any]:
        """Import a parser configuration from JSON string."""
        config_data = json.loads(config_json)
        
        return self.create_config(
            name=config_data['name'],
            protocol=config_data['protocol'],
            description=config_data.get('description'),
            sender_filter=config_data.get('sender_filter'),
            is_active=config_data.get('is_active', False),
            enable_escape_decoding=config_data.get('enable_escape_decoding', True),
            custom_loinc_codes=config_data.get('custom_loinc_codes'),
            field_mappings=config_data.get('field_mappings'),
            delimiter_overrides=config_data.get('delimiter_overrides'),
        )


__all__ = ['ParsingConfigRepository']
