"""
Repository for managing field mapping profiles
"""

import sqlite3
import json
import logging
from datetime import UTC, datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)
from .schema import SCHEMA_SQL


DEFAULT_MAPPING_TEMPLATE_NAME = "Default Template"
DEFAULT_MAPPING_TEMPLATE_JSON = json.dumps({
    "displayNumber": "string",
    "testName": "string",
    "result": [
        {
            "fieldName": "string",
            "testResult": "string",
        }
    ],
}, indent=2)


class MappingRepository:
    """
    Repository for storing and retrieving field mapping configurations.
    Handles CRUD operations for mapping profiles in SQLite database.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize the mapping repository.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._initialize()
        logger.info(f"Mapping repository initialized with database: {db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection with Row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _initialize(self) -> None:
        """Initialize database schema if not already created."""
        with self._get_connection() as conn:
            conn.executescript(SCHEMA_SQL)
            self._ensure_mapping_profile_columns(conn)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_mapping_profiles_match
                ON mapping_profiles(is_active, protocol_filter, instrument_model, instrument_serial, test_profile)
            """)
            self._ensure_default_template(conn)
            conn.commit()

    def _ensure_default_template(self, conn: sqlite3.Connection) -> None:
        """Ensure one immutable default template exists server-side."""
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO mapping_templates (name, template_json, is_default, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(name)
            DO UPDATE SET
                template_json = excluded.template_json,
                is_default = 1,
                updated_at = excluded.updated_at
            """,
            (DEFAULT_MAPPING_TEMPLATE_NAME, DEFAULT_MAPPING_TEMPLATE_JSON, now, now),
        )

    def _ensure_mapping_profile_columns(self, conn: sqlite3.Connection) -> None:
        """Backfill selector columns for older databases."""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(mapping_profiles)")
        existing_columns = {
            row["name"] if isinstance(row, sqlite3.Row) else row[1]
            for row in cursor.fetchall()
        }

        required_columns = {
            "instrument_model": "TEXT",
            "instrument_serial": "TEXT",
            "test_profile": "TEXT",
        }

        for column_name, column_type in required_columns.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE mapping_profiles ADD COLUMN {column_name} {column_type}")
    
    def create_profile(self, name: str, description: str = "", 
                      protocol_filter: str = "ALL", instrument_model: str = None,
                      instrument_serial: str = None, test_profile: str = None,
                      config: List[Dict[str, Any]] = None) -> int:
        """
        Create a new mapping profile.
        
        Args:
            name: Unique name for the mapping profile
            description: Optional description of the mapping
            protocol_filter: Filter by protocol ('ALL', 'HL7', 'ASTM')
            config: List of mapping rules
            
        Returns:
            ID of created profile
            
        Raises:
            ValueError: If name is empty or config is invalid
            sqlite3.IntegrityError: If name already exists
        """
        if not name or not name.strip():
            raise ValueError("Profile name cannot be empty")
        
        if config is None:
            config = []
        
        # Validate config is serializable
        try:
            config_json = json.dumps(config)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid config format: {str(e)}")
        
        now = datetime.now(UTC).isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO mapping_profiles (
                    name, description, protocol_filter, instrument_model,
                    instrument_serial, test_profile, config, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                name.strip(),
                description,
                protocol_filter,
                instrument_model or None,
                instrument_serial or None,
                test_profile or None,
                config_json,
                now,
                now,
            ))
            conn.commit()
            profile_id = cursor.lastrowid
            
        logger.info(f"Created mapping profile: {name} (ID: {profile_id})")
        return profile_id
    
    def get_profile(self, profile_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve a mapping profile by ID.
        
        Args:
            profile_id: ID of the profile
            
        Returns:
            Profile dict or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles WHERE id = ?", (profile_id,))
            row = cursor.fetchone()
            
        if row:
            return self._row_to_dict(row)
        return None
    
    def get_profile_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a mapping profile by name.
        
        Args:
            name: Name of the profile
            
        Returns:
            Profile dict or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles WHERE name = ?", (name,))
            row = cursor.fetchone()
            
        if row:
            return self._row_to_dict(row)
        return None
    
    def get_active_profile(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recently activated mapping profile.
        
        Returns:
            Active profile dict or None if no active profile
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles WHERE is_active = 1 ORDER BY updated_at DESC, id DESC LIMIT 1")
            row = cursor.fetchone()
            
        if row:
            return self._row_to_dict(row)
        return None

    def get_active_profiles(self) -> List[Dict[str, Any]]:
        """Return all enabled mapping profiles ordered by recency."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles WHERE is_active = 1 ORDER BY updated_at DESC, id DESC")
            rows = cursor.fetchall()

        return [self._row_to_dict(row) for row in rows]

    def get_matching_profile(
        self,
        protocol: str = None,
        instrument_model: str = None,
        instrument_serial: str = None,
        test_profile: str = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the most specific active profile for the incoming message."""
        requested_protocol = (protocol or "ALL").upper()
        best_profile = None
        best_score = -1

        for profile in self.get_active_profiles():
            protocol_filter = (profile.get("protocol_filter") or "ALL").upper()
            if protocol_filter not in {"ALL", requested_protocol}:
                continue

            selectors = [
                ("instrument_model", instrument_model, 2),
                ("instrument_serial", instrument_serial, 3),
                ("test_profile", test_profile, 2),
            ]

            score = 1 if protocol_filter != "ALL" else 0
            matched = True

            for field_name, actual_value, weight in selectors:
                expected_value = profile.get(field_name)
                if expected_value in (None, ""):
                    continue
                if actual_value is None or str(expected_value).strip().upper() != str(actual_value).strip().upper():
                    matched = False
                    break
                score += weight

            if matched and score > best_score:
                best_profile = profile
                best_score = score

        return best_profile
    
    def get_all_profiles(self) -> List[Dict[str, Any]]:
        """
        Get all mapping profiles.
        
        Returns:
            List of profile dicts
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles ORDER BY created_at DESC")
            rows = cursor.fetchall()
            
        return [self._row_to_dict(row) for row in rows]
    
    def update_profile(self, profile_id: int, name: str = None, 
                      description: str = None, protocol_filter: str = None,
                      instrument_model: str = None, instrument_serial: str = None,
                      test_profile: str = None, config: List[Dict[str, Any]] = None) -> bool:
        """
        Update an existing mapping profile.
        
        Args:
            profile_id: ID of profile to update
            name: New name (optional)
            description: New description (optional)
            protocol_filter: New protocol filter (optional)
            config: New config (optional)
            
        Returns:
            True if updated, False if profile not found
        """
        # Check if profile exists
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        
        updates = []
        values = []
        
        if name is not None:
            if not name.strip():
                raise ValueError("Profile name cannot be empty")
            updates.append("name = ?")
            values.append(name.strip())
        
        if description is not None:
            updates.append("description = ?")
            values.append(description)
        
        if protocol_filter is not None:
            updates.append("protocol_filter = ?")
            values.append(protocol_filter)

        if instrument_model is not None:
            updates.append("instrument_model = ?")
            values.append(instrument_model or None)

        if instrument_serial is not None:
            updates.append("instrument_serial = ?")
            values.append(instrument_serial or None)

        if test_profile is not None:
            updates.append("test_profile = ?")
            values.append(test_profile or None)
        
        if config is not None:
            try:
                config_json = json.dumps(config)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Invalid config format: {str(e)}")
            updates.append("config = ?")
            values.append(config_json)
        
        if not updates:
            return True  # Nothing to update
        
        updates.append("updated_at = ?")
        values.append(datetime.now(UTC).isoformat())
        values.append(profile_id)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE mapping_profiles 
                SET {', '.join(updates)}
                WHERE id = ?
            """, values)
            conn.commit()
        
        logger.info(f"Updated mapping profile ID: {profile_id}")
        return True
    
    def delete_profile(self, profile_id: int) -> bool:
        """
        Delete a mapping profile.
        
        Args:
            profile_id: ID of profile to delete
            
        Returns:
            True if deleted, False if not found
            
        Raises:
            ValueError: If trying to delete active profile
        """
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        
        if profile['is_active']:
            raise ValueError("Cannot delete active profile. Deactivate it first.")
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM mapping_profiles WHERE id = ?", (profile_id,))
            conn.commit()
        
        logger.info(f"Deleted mapping profile ID: {profile_id}")
        return True
    
    def set_active_profile(self, profile_id: int) -> bool:
        """
        Enable a profile for runtime matching.
        
        Multiple profiles may be active at once; the most specific match wins.
        """
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE mapping_profiles 
                SET is_active = 1, updated_at = ?
                WHERE id = ?
            """, (datetime.now(UTC).isoformat(), profile_id))
            conn.commit()
        
        logger.info(f"Activated mapping profile: {profile['name']} (ID: {profile_id})")
        return True

    def deactivate_profile(self, profile_id: int) -> bool:
        """Deactivate a specific mapping profile."""
        profile = self.get_profile(profile_id)
        if not profile:
            return False

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE mapping_profiles SET is_active = 0, updated_at = ? WHERE id = ?",
                (datetime.now(UTC).isoformat(), profile_id),
            )
            conn.commit()

        logger.info(f"Deactivated mapping profile: {profile['name']} (ID: {profile_id})")
        return True
    
    def deactivate_all_profiles(self) -> None:
        """Deactivate all mapping profiles"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE mapping_profiles SET is_active = 0")
            conn.commit()
        
        logger.info("Deactivated all mapping profiles")
    
    # ------------------------------------------------------------------
    # Machine assignments
    # ------------------------------------------------------------------

    def get_unique_instruments(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return unique instruments seen in recent messages."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT protocol, source_ip, parsed_data, created_at FROM messages "
                "WHERE parsed_data IS NOT NULL ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            rows = cursor.fetchall()

        seen: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            try:
                data = json.loads(row["parsed_data"])
            except (json.JSONDecodeError, TypeError):
                continue
            instrument = data.get("instrument") or {}
            model = (instrument.get("model") or "").strip() if isinstance(instrument, dict) else ""
            serial = (instrument.get("serial") or "").strip() if isinstance(instrument, dict) else ""
            protocol = (row["protocol"] or "ALL").upper()
            key = f"{protocol}|{model}|{serial}"
            if key not in seen:
                display_name = (
                    instrument.get("display_name") or model
                    if isinstance(instrument, dict) else model
                ) or f"{protocol} device"
                seen[key] = {
                    "key": key,
                    "protocol": protocol,
                    "instrument_model": model,
                    "instrument_serial": serial,
                    "display_name": display_name,
                    "source_ip": row["source_ip"] or "",
                    "last_seen": row["created_at"],
                }
        return list(seen.values())

    def get_machine_assignment(
        self,
        protocol: str,
        instrument_model: str,
        instrument_serial: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the explicit profile assignment for a machine, or None if none set."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM machine_assignments WHERE protocol=? AND instrument_model=? AND instrument_serial=?",
                (protocol or "ALL", instrument_model or "", instrument_serial or ""),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def set_machine_assignment(
        self,
        protocol: str,
        instrument_model: str,
        instrument_serial: str,
        profile_id: Optional[int],
    ) -> None:
        """Upsert a machine → profile assignment. profile_id=None means 'use default'."""
        now = datetime.now(UTC).isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """INSERT INTO machine_assignments (protocol, instrument_model, instrument_serial, profile_id, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(protocol, instrument_model, instrument_serial)
                   DO UPDATE SET profile_id=excluded.profile_id, updated_at=excluded.updated_at""",
                (protocol or "ALL", instrument_model or "", instrument_serial or "", profile_id, now),
            )
            conn.commit()
        logger.info("Set machine assignment %s|%s|%s -> profile_id=%s", protocol, instrument_model, instrument_serial, profile_id)

    def delete_machine_assignment(
        self,
        protocol: str,
        instrument_model: str,
        instrument_serial: str,
    ) -> None:
        """Remove an explicit machine assignment (revert to auto-matching)."""
        with self._get_connection() as conn:
            conn.execute(
                "DELETE FROM machine_assignments WHERE protocol=? AND instrument_model=? AND instrument_serial=?",
                (protocol or "ALL", instrument_model or "", instrument_serial or ""),
            )
            conn.commit()

    def get_all_machine_assignments(self) -> List[Dict[str, Any]]:
        """Return all machine assignments."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM machine_assignments ORDER BY updated_at DESC")
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Mapping templates
    # ------------------------------------------------------------------

    def get_all_templates(self) -> List[Dict[str, Any]]:
        """Return all mapping templates with default first."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM mapping_templates
                ORDER BY is_default DESC, name COLLATE NOCASE ASC
                """
            )
            rows = cursor.fetchall()

        templates: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["is_default"] = bool(item.get("is_default"))
            templates.append(item)
        return templates

    def create_template(self, name: str, template_json: str) -> int:
        """Create a custom mapping template."""
        if not name or not name.strip():
            raise ValueError("Template name is required")

        try:
            parsed = json.loads(template_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Template JSON is invalid: {exc}") from exc

        normalized = json.dumps(parsed, indent=2)
        now = datetime.now(UTC).isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO mapping_templates (name, template_json, is_default, created_at, updated_at)
                    VALUES (?, ?, 0, ?, ?)
                    """,
                    (name.strip(), normalized, now, now),
                )
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError as exc:
                raise ValueError("Template name already exists") from exc

    def update_template(self, template_id: int, template_json: str, name: Optional[str] = None) -> bool:
        """Update a custom template by id."""
        existing = self.get_template(template_id)
        if not existing:
            return False
        if existing.get("is_default"):
            raise ValueError("Default template cannot be edited")

        try:
            parsed = json.loads(template_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Template JSON is invalid: {exc}") from exc

        normalized = json.dumps(parsed, indent=2)
        next_name = name.strip() if isinstance(name, str) else existing["name"]
        if not next_name:
            raise ValueError("Template name is required")

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE mapping_templates
                    SET name = ?, template_json = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (next_name, normalized, datetime.now(UTC).isoformat(), template_id),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise ValueError("Template name already exists") from exc

        return True

    def get_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        """Get template by id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_templates WHERE id = ?", (template_id,))
            row = cursor.fetchone()

        if not row:
            return None

        template = dict(row)
        template["is_default"] = bool(template.get("is_default"))
        return template

    def delete_template(self, template_id: int) -> bool:
        """Delete template by id (except default)."""
        existing = self.get_template(template_id)
        if not existing:
            return False
        if existing.get("is_default"):
            raise ValueError("Default template cannot be deleted")

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM mapping_templates WHERE id = ?", (template_id,))
            conn.commit()

        return True

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """
        Convert database row to dictionary with parsed JSON.
        
        Args:
            row: sqlite3.Row object
            
        Returns:
            Dictionary with parsed config
        """
        profile = dict(row)
        
        # Parse JSON config
        try:
            profile['config'] = json.loads(profile['config'])
        except (json.JSONDecodeError, TypeError):
            logger.error(f"Failed to parse config for profile ID: {profile['id']}")
            profile['config'] = []
        
        # Convert is_active to boolean
        profile['is_active'] = bool(profile['is_active'])
        
        return profile
