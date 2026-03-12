"""
Repository for managing field mapping profiles
"""

import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)
from .schema import SCHEMA_SQL


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
        """Initialize database schema if not already created"""
        with self._get_connection() as conn:
            conn.executescript(SCHEMA_SQL)
    
    def create_profile(self, name: str, description: str = "", 
                      protocol_filter: str = "ALL", config: List[Dict[str, Any]] = None) -> int:
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
        
        now = datetime.utcnow().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO mapping_profiles (name, description, protocol_filter, config, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name.strip(), description, protocol_filter, config_json, now, now))
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
        Get the currently active mapping profile.
        
        Returns:
            Active profile dict or None if no active profile
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mapping_profiles WHERE is_active = 1 LIMIT 1")
            row = cursor.fetchone()
            
        if row:
            return self._row_to_dict(row)
        return None
    
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
                      config: List[Dict[str, Any]] = None) -> bool:
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
        values.append(datetime.utcnow().isoformat())
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
        Set a profile as active. Deactivates all other profiles.
        
        Args:
            profile_id: ID of profile to activate
            
        Returns:
            True if activated, False if profile not found
        """
        # Check if profile exists
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Deactivate all profiles
            cursor.execute("UPDATE mapping_profiles SET is_active = 0")
            
            # Activate the specified profile
            cursor.execute("""
                UPDATE mapping_profiles 
                SET is_active = 1, updated_at = ?
                WHERE id = ?
            """, (datetime.utcnow().isoformat(), profile_id))
            
            conn.commit()
        
        logger.info(f"Activated mapping profile: {profile['name']} (ID: {profile_id})")
        return True
    
    def deactivate_all_profiles(self) -> None:
        """Deactivate all mapping profiles"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE mapping_profiles SET is_active = 0")
            conn.commit()
        
        logger.info("Deactivated all mapping profiles")
    
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
