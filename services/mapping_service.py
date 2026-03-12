"""
Service for applying field mapping transformations to parsed HL7/ASTM data
"""

import logging
from typing import Dict, Any, List, Optional
import re

logger = logging.getLogger(__name__)


class MappingService:
    """
    Service for applying custom field mappings to parsed message data.
    Transforms data based on mapping rules stored in profiles.
    """
    
    def __init__(self, mapping_repository):
        """
        Initialize the mapping service.
        
        Args:
            mapping_repository: MappingRepository instance
        """
        self.mapping_repository = mapping_repository
        logger.info("Mapping service initialized")
    
    def apply_mapping(self, parsed_data: Dict[str, Any], protocol: str = None) -> Dict[str, Any]:
        """
        Apply the active mapping profile to parsed data.
        
        Args:
            parsed_data: Parsed HL7 or ASTM data dictionary
            protocol: Protocol type ('HL7' or 'ASTM'), optional
            
        Returns:
            Transformed data dictionary
        """
        # Get active profile
        try:
            active_profile = self.mapping_repository.get_active_profile()
        except Exception as e:
            logger.error(f"Failed to get active mapping profile: {str(e)}")
            return parsed_data
        
        if not active_profile:
            logger.debug("No active mapping profile, returning original data")
            return parsed_data
        
        # Check protocol filter
        protocol_filter = active_profile.get('protocol_filter', 'ALL')
        if protocol_filter != 'ALL' and protocol and protocol.upper() != protocol_filter.upper():
            logger.debug(f"Protocol {protocol} does not match filter {protocol_filter}, skipping mapping")
            return parsed_data
        
        config = active_profile.get('config', [])
        if not config:
            logger.warning(f"Active profile '{active_profile['name']}' has no mapping rules")
            return parsed_data
        
        logger.info(f"Applying mapping profile: {active_profile['name']} with {len(config)} rules")
        
        # Apply mapping rules
        try:
            result = self._apply_rules(parsed_data, config)
            logger.debug(f"Successfully applied mapping, output has {len(result)} top-level fields")
            return result
        except Exception as e:
            logger.error(f"Failed to apply mapping: {str(e)}")
            return parsed_data
    
    def _apply_rules(self, data: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply mapping rules to transform data.
        
        Args:
            data: Source data dictionary
            rules: List of mapping rules
            
        Returns:
            Transformed data dictionary
        """
        result = {}
        
        for rule in rules:
            source_path = rule.get('source_path', '')
            target_path = rule.get('target_path', '')
            default_value = rule.get('default_value')
            transform = rule.get('transform')
            
            if not source_path or not target_path:
                logger.warning(f"Skipping invalid rule: {rule}")
                continue
            
            try:
                # Extract value from source
                values = self._extract_value(data, source_path)
                
                # Handle array iteration
                if isinstance(values, list) and '[*]' in source_path:
                    # Array mapping - preserve array structure
                    transformed_values = []
                    for value in values:
                        if value is not None:
                            transformed_value = self._apply_transform(value, transform)
                            transformed_values.append(transformed_value)
                    
                    if transformed_values or default_value is None:
                        self._set_value(result, target_path, transformed_values)
                    else:
                        self._set_value(result, target_path, default_value)
                else:
                    # Single value mapping
                    if values is not None:
                        transformed_value = self._apply_transform(values, transform)
                        self._set_value(result, target_path, transformed_value)
                    elif default_value is not None:
                        self._set_value(result, target_path, default_value)
                
            except Exception as e:
                logger.warning(f"Failed to apply rule {source_path} -> {target_path}: {str(e)}")
                if default_value is not None:
                    self._set_value(result, target_path, default_value)
        
        return result
    
    def _extract_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Extract value from nested data structure using dot notation and array indexing.
        
        Supports:
        - Dot notation: 'patient.name.family_name'
        - Array indexing: 'observations[0].value'
        - Array iteration: 'observations[*].value'
        
        Args:
            data: Source data dictionary
            path: Path to value (e.g., 'patient.id' or 'observations[0].value')
            
        Returns:
            Extracted value or None if not found
        """
        if not path:
            return None
        
        current = data
        parts = self._parse_path(path)
        
        for part in parts:
            if current is None:
                return None
            
            if part['type'] == 'key':
                # Dictionary key access
                if isinstance(current, dict):
                    current = current.get(part['value'])
                else:
                    return None
            
            elif part['type'] == 'index':
                # Array index access
                if isinstance(current, list):
                    try:
                        index = int(part['value'])
                        current = current[index] if 0 <= index < len(current) else None
                    except (ValueError, IndexError):
                        return None
                else:
                    return None
            
            elif part['type'] == 'iterate':
                # Array iteration - return list of values
                if isinstance(current, list):
                    # Get remaining path after [*]
                    remaining_parts = parts[parts.index(part) + 1:]
                    if remaining_parts:
                        # Extract value from each item
                        results = []
                        for item in current:
                            value = self._extract_from_parts(item, remaining_parts)
                            if value is not None:
                                results.append(value)
                        return results
                    else:
                        return current
                else:
                    return None
        
        return current
    
    def _extract_from_parts(self, data: Any, parts: List[Dict[str, str]]) -> Any:
        """
        Extract value from data using parsed path parts.
        
        Args:
            data: Source data
            parts: List of path parts
            
        Returns:
            Extracted value or None
        """
        current = data
        
        for part in parts:
            if current is None:
                return None
            
            if part['type'] == 'key':
                if isinstance(current, dict):
                    current = current.get(part['value'])
                else:
                    return None
            elif part['type'] == 'index':
                if isinstance(current, list):
                    try:
                        index = int(part['value'])
                        current = current[index] if 0 <= index < len(current) else None
                    except (ValueError, IndexError):
                        return None
                else:
                    return None
        
        return current
    
    def _parse_path(self, path: str) -> List[Dict[str, str]]:
        """
        Parse path string into components.
        
        Examples:
        - 'patient.name' -> [{'type': 'key', 'value': 'patient'}, {'type': 'key', 'value': 'name'}]
        - 'observations[0]' -> [{'type': 'key', 'value': 'observations'}, {'type': 'index', 'value': '0'}]
        - 'observations[*]' -> [{'type': 'key', 'value': 'observations'}, {'type': 'iterate', 'value': '*'}]
        
        Args:
            path: Path string
            
        Returns:
            List of path components
        """
        parts = []
        
        # Split by dots, but preserve array notation
        tokens = re.split(r'\.', path)
        
        for token in tokens:
            # Check for array notation
            match = re.match(r'^([^\[]+)\[([^\]]+)\]$', token)
            if match:
                key = match.group(1)
                index = match.group(2)
                
                parts.append({'type': 'key', 'value': key})
                
                if index == '*':
                    parts.append({'type': 'iterate', 'value': '*'})
                else:
                    parts.append({'type': 'index', 'value': index})
            else:
                parts.append({'type': 'key', 'value': token})
        
        return parts
    
    def _set_value(self, data: Dict[str, Any], path: str, value: Any) -> None:
        """
        Set value in nested data structure using dot notation.
        Creates intermediate dictionaries as needed.
        
        Args:
            data: Target data dictionary (modified in place)
            path: Path to set (e.g., 'patient.id' or 'result[0].value')
            value: Value to set
        """
        if not path:
            return
        
        parts = self._parse_path(path)
        current = data
        
        for i, part in enumerate(parts[:-1]):
            key = part['value']
            
            if part['type'] == 'key':
                if key not in current:
                    # Determine what to create based on next part
                    next_part = parts[i + 1]
                    if next_part['type'] in ['index', 'iterate']:
                        current[key] = []
                    else:
                        current[key] = {}
                
                current = current[key]
            
            elif part['type'] == 'index':
                # Extend array if needed
                index = int(part['value'])
                while len(current) <= index:
                    current.append({})
                current = current[index]
        
        # Set final value
        last_part = parts[-1]
        if last_part['type'] == 'key':
            current[last_part['value']] = value
        elif last_part['type'] == 'index':
            index = int(last_part['value'])
            while len(current) <= index:
                current.append(None)
            current[index] = value
    
    def _apply_transform(self, value: Any, transform: Optional[str]) -> Any:
        """
        Apply transformation function to value.
        
        Supported transforms:
        - 'uppercase': Convert string to uppercase
        - 'lowercase': Convert string to lowercase
        - 'trim': Remove leading/trailing whitespace
        - 'string': Convert to string
        
        Args:
            value: Value to transform
            transform: Transform name or None
            
        Returns:
            Transformed value
        """
        if transform is None or value is None:
            return value
        
        try:
            if transform == 'uppercase':
                return str(value).upper()
            elif transform == 'lowercase':
                return str(value).lower()
            elif transform == 'trim':
                return str(value).strip()
            elif transform == 'string':
                return str(value)
            else:
                logger.warning(f"Unknown transform: {transform}")
                return value
        except Exception as e:
            logger.warning(f"Failed to apply transform '{transform}': {str(e)}")
            return value
    
    def validate_config(self, config: List[Dict[str, Any]]) -> tuple[bool, Optional[str]]:
        """
        Validate mapping configuration.
        
        Args:
            config: List of mapping rules
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(config, list):
            return False, "Config must be a list"
        
        for i, rule in enumerate(config):
            if not isinstance(rule, dict):
                return False, f"Rule {i} must be a dictionary"
            
            if 'source_path' not in rule or not rule['source_path']:
                return False, f"Rule {i} missing 'source_path'"
            
            if 'target_path' not in rule or not rule['target_path']:
                return False, f"Rule {i} missing 'target_path'"
            
            # Validate path syntax
            try:
                self._parse_path(rule['source_path'])
                self._parse_path(rule['target_path'])
            except Exception as e:
                return False, f"Rule {i} has invalid path syntax: {str(e)}"
        
        return True, None
    
    def preview_mapping(self, data: Dict[str, Any], config: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Preview mapping without using a saved profile.
        
        Args:
            data: Source data to transform
            config: Mapping rules to apply
            
        Returns:
            Transformed data
        """
        try:
            return self._apply_rules(data, config)
        except Exception as e:
            logger.error(f"Preview mapping failed: {str(e)}")
            raise
