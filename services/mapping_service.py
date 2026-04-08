"""
Service for applying field mapping transformations to parsed HL7/ASTM data
"""

import json
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
        Apply the best matching active mapping profile to parsed data.
        
        Args:
            parsed_data: Parsed HL7 or ASTM data dictionary
            protocol: Protocol type ('HL7' or 'ASTM'), optional
            
        Returns:
            Transformed data dictionary
        """
        selectors = self._extract_profile_selectors(parsed_data, protocol)

        try:
            # Check for an explicit machine assignment first
            assignment = self.mapping_repository.get_machine_assignment(
                selectors.get('protocol', 'ALL'),
                selectors.get('instrument_model') or '',
                selectors.get('instrument_serial') or '',
            )
            if assignment is not None:
                profile_id = assignment.get('profile_id')
                if profile_id is None:
                    # Explicitly set to "Default" — skip mapping entirely
                    logger.debug("Machine assignment set to default; skipping custom mapping")
                    return parsed_data
                active_profile = self.mapping_repository.get_profile(profile_id)
            else:
                active_profile = self.mapping_repository.get_matching_profile(**selectors)
        except AttributeError:
            active_profile = self.mapping_repository.get_active_profile()
        except Exception as e:
            logger.error(f"Failed to get active mapping profile: {str(e)}")
            return parsed_data
        
        if not active_profile:
            logger.debug("No active mapping profile, returning original data")
            return parsed_data
        
        config = active_profile.get('config', [])
        if not config:
            logger.warning(f"Active profile '{active_profile['name']}' has no mapping rules")
            return parsed_data
        
        logger.info(
            "Applying mapping profile '%s' with %d rules (model=%s, serial=%s, test_profile=%s)",
            active_profile['name'],
            len(config),
            selectors.get('instrument_model'),
            selectors.get('instrument_serial'),
            selectors.get('test_profile'),
        )
        
        try:
            result = self._apply_rules(parsed_data, config)
            logger.debug(f"Successfully applied mapping, output has {len(result)} top-level fields")
            return result
        except Exception as e:
            logger.error(f"Failed to apply mapping: {str(e)}")
            return parsed_data

    def _extract_profile_selectors(self, parsed_data: Dict[str, Any], protocol: str = None) -> Dict[str, Any]:
        """Extract runtime selectors used to find the best mapping profile."""
        if not isinstance(parsed_data, dict):
            return {
                'protocol': protocol or 'ALL',
                'instrument_model': None,
                'instrument_serial': None,
                'test_profile': None,
            }

        instrument = parsed_data.get('instrument') or {}
        first_order = (parsed_data.get('orders') or [{}])[0]
        if not isinstance(first_order, dict):
            first_order = {}

        return {
            'protocol': protocol or parsed_data.get('protocol') or 'ALL',
            'instrument_model': instrument.get('model') if isinstance(instrument, dict) else None,
            'instrument_serial': instrument.get('serial') if isinstance(instrument, dict) else None,
            'test_profile': parsed_data.get('message_profile') or first_order.get('test_profile'),
        }
    
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
            
            if not target_path or (not source_path and default_value is None):
                logger.warning(f"Skipping invalid rule: {rule}")
                continue
            
            try:
                # Extract value from source when provided; otherwise use static/default value.
                values = self._extract_value(data, source_path) if source_path else None
                
                # Handle array iteration
                if source_path and isinstance(values, list) and '[*]' in source_path:
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
                        transformed_default = self._apply_transform(default_value, transform)
                        self._set_value(result, target_path, transformed_default)
                
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
        - 'result_pairs': Convert an HL7/ASTM observation/result to {fieldName, testResult}
        
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
            elif transform == 'result_pairs':
                if isinstance(value, dict):
                    identifier = value.get('identifier')
                    if not identifier:
                        test_id = value.get('universal_test_id') or {}
                        if isinstance(test_id, dict):
                            identifier = (
                                test_id.get('display_name')
                                or test_id.get('mnemonic')
                                or test_id.get('test_name')
                                or test_id.get('test_id')
                                or test_id.get('raw')
                            )
                    return {
                        'fieldName': str(identifier or ''),
                        'testResult': str(value.get('value') or ''),
                    }
                return {
                    'fieldName': '',
                    'testResult': str(value),
                }
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
            
            source_path = rule.get('source_path')
            target_path = rule.get('target_path')
            default_value = rule.get('default_value')

            if not target_path:
                return False, f"Rule {i} missing 'target_path'"

            if not source_path and default_value is None:
                return False, f"Rule {i} needs either 'source_path' or 'default_value'"
            
            # Validate path syntax
            try:
                if source_path:
                    self._parse_path(source_path)
                self._parse_path(target_path)
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

    def get_default_json_template(self) -> Dict[str, Any]:
        """Return the default outbound JSON schema requested by the user."""
        return {
            "displayNumber": "string",
            "testName": "string",
            "result": [
                {
                    "fieldName": "string",
                    "testResult": "string",
                }
            ],
        }

    def get_default_mapping_config(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build the default mapping config for the requested outbound layout."""
        display_source = self._first_existing_path(data, [
            'patient.id',
            'patient.practice_patient_id',
            'patient.lab_patient_id',
            'orders[0].specimen_id_parsed.accession_number',
            'orders[0].filler_order_number',
            'orders[0].placer_order_number',
            'orders[0].specimen_id',
        ])
        test_name_source = self._first_existing_path(data, [
            'message_profile',
            'orders[0].universal_service_id',
            'orders[0].test_profile',
            'orders[0].universal_test_id.display_name',
            'orders[0].universal_test_id.raw',
        ])
        result_source = self._first_existing_path(data, ['observations[*]', 'results[*]'])

        rules: List[Dict[str, Any]] = [
            {
                'source_path': display_source,
                'target_path': 'displayNumber',
                'default_value': '',
            },
            {
                'source_path': test_name_source,
                'target_path': 'testName',
                'default_value': '',
            },
        ]

        if result_source:
            rules.append({
                'source_path': result_source,
                'target_path': 'result',
                'transform': 'result_pairs',
                'default_value': [],
            })
        else:
            rules.append({
                'target_path': 'result',
                'default_value': [],
            })

        return rules

    def generate_template_rules(self, data: Dict[str, Any], template: Any) -> List[Dict[str, Any]]:
        """Generate mapping rules from a pasted JSON template and sample message data."""
        if isinstance(template, str):
            try:
                template = json.loads(template)
            except json.JSONDecodeError as exc:
                raise ValueError(f'Invalid JSON template: {exc.msg}') from exc

        if not isinstance(template, (dict, list)):
            raise ValueError('Template must be a JSON object or array')

        if template == self.get_default_json_template():
            return self.get_default_mapping_config(data)

        extracted_fields = self._extract_template_fields(template)
        rules: List[Dict[str, Any]] = []
        handled_result_arrays = set()

        for field in extracted_fields:
            target_path = field['path']
            value = field['value']
            normalized_target = re.sub(r'\[\d+\]', '[0]', target_path)
            key_name = normalized_target.split('.')[-1].replace('[0]', '')
            array_match = re.match(r'^(.*)\[0\]\.(fieldName|testResult)$', normalized_target)

            if key_name in {'fieldName', 'testResult'} and array_match:
                array_target = array_match.group(1)
                if array_target not in handled_result_arrays:
                    handled_result_arrays.add(array_target)
                    result_source = self._first_existing_path(data, ['observations[*]', 'results[*]'])
                    rule = {
                        'target_path': array_target,
                        'transform': 'result_pairs',
                        'default_value': [],
                    }
                    if result_source:
                        rule['source_path'] = result_source
                    rules.append(rule)
                continue

            source_path = self._suggest_source_path(data, key_name)
            rule: Dict[str, Any] = {'target_path': normalized_target}

            if source_path:
                rule['source_path'] = source_path

            default_value = self._coerce_template_value(value)
            if default_value is not None:
                rule['default_value'] = default_value

            if 'source_path' in rule or 'default_value' in rule:
                rules.append(rule)

        return rules or self.get_default_mapping_config(data)

    def _first_existing_path(self, data: Dict[str, Any], candidates: List[str]) -> Optional[str]:
        """Return the first candidate path that exists in the sample data."""
        for candidate in candidates:
            value = self._extract_value(data, candidate)
            if value not in (None, '', [], {}):
                return candidate
        return None

    def _extract_template_fields(self, template: Any, path: str = '') -> List[Dict[str, Any]]:
        """Flatten a JSON template into leaf paths and values."""
        fields: List[Dict[str, Any]] = []

        if isinstance(template, dict):
            for key, value in template.items():
                next_path = f'{path}.{key}' if path else key
                fields.extend(self._extract_template_fields(value, next_path))
        elif isinstance(template, list):
            if not template:
                fields.append({'path': path, 'value': []})
            else:
                fields.extend(self._extract_template_fields(template[0], f'{path}[0]'))
        else:
            fields.append({'path': path, 'value': template})

        return fields

    def _suggest_source_path(self, data: Dict[str, Any], key_name: str) -> Optional[str]:
        """Suggest a likely source path for a target key based on the selected message."""
        normalized_key = self._normalize_key(key_name)
        suggestion_map = {
            'displaynumber': [
                'patient.id', 'patient.practice_patient_id', 'patient.lab_patient_id',
                'orders[0].specimen_id_parsed.accession_number', 'orders[0].filler_order_number',
                'orders[0].placer_order_number', 'orders[0].specimen_id'
            ],
            'testname': [
                'message_profile', 'orders[0].universal_service_id', 'orders[0].test_profile',
                'orders[0].universal_test_id.display_name', 'orders[0].universal_test_id.raw'
            ],
            'fieldname': ['observations[*]', 'results[*]'],
            'testresult': ['observations[*]', 'results[*]'],
            'patientid': ['patient.id', 'patient.practice_patient_id', 'patient.lab_patient_id'],
            'patientname': ['patient.name.given_name', 'patient.name.first', 'patient.name.family_name', 'patient.name.last'],
            'instrumentmodel': ['instrument.model'],
            'instrumentserial': ['instrument.serial'],
        }

        candidates = suggestion_map.get(normalized_key, [])
        candidate_match = self._first_existing_path(data, candidates)
        if candidate_match:
            return candidate_match

        discovered_paths: List[str] = []
        self._find_matching_paths(data, normalized_key, '', discovered_paths)
        return discovered_paths[0] if discovered_paths else None

    def _find_matching_paths(self, data: Any, normalized_key: str, path: str, results: List[str]) -> None:
        """Recursively search parsed message data for matching field names."""
        if isinstance(data, dict):
            for key, value in data.items():
                next_path = f'{path}.{key}' if path else key
                if self._normalize_key(key) == normalized_key:
                    results.append(next_path)
                self._find_matching_paths(value, normalized_key, next_path, results)
        elif isinstance(data, list):
            for index, item in enumerate(data):
                next_path = f'{path}[{index}]' if path else f'[{index}]'
                self._find_matching_paths(item, normalized_key, next_path, results)

    def _normalize_key(self, key: str) -> str:
        """Normalize a key for loose matching across naming styles."""
        return re.sub(r'[^a-z0-9]', '', str(key).lower())

    def _coerce_template_value(self, value: Any) -> Any:
        """Convert template placeholders into safe default values for generated rules."""
        if isinstance(value, str):
            placeholder = value.strip().lower()
            if placeholder == 'string':
                return ''
            if placeholder == 'number':
                return 0
            if placeholder == 'boolean':
                return False
            if placeholder in {'null', 'none'}:
                return None
        return value
