"""
ASTM Message Parser
Parses ASTM E1394 / LIS2-A2 messages from laboratory instruments.

Record types:
  H  - Header
  P  - Patient
  O  - Order
  R  - Result
  C  - Comment
  Q  - Query
  M  - Manufacturer Info
  L  - Message Terminator
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ASTM low-level framing control characters
_STX = '\x02'
_ETX = '\x03'
_ETB = '\x17'
_CR  = '\r'
_LF  = '\n'


class ASTMParser:
    """
    Parser for ASTM E1394 / LIS2-A2 messages from laboratory instruments.

    Handles:
    - ASTM record separator (CR) and optional LF line endings
    - Framing byte stripping (STX, ETX/ETB, frame numbers, checksums)
    - Custom delimiter sets declared in the H record
    - All standard record types: H P O R C Q M L
    - Multiple orders and multiple results per message
    - Component sub-field parsing (^ separator)
    """

    # Default ASTM delimiters when H record is absent or malformed
    _DEFAULT_FIELD_SEP = '|'
    _DEFAULT_COMPONENT_SEP = '^'
    _DEFAULT_REPEAT_SEP = '\\'
    _DEFAULT_ESCAPE = '&'

    # Common LOINC code mappings for clinical test identification
    _LOINC_CODES = {
        '6690-2': 'White Blood Cell Count',
        '789-8': 'Red Blood Cell Count',
        '718-7': 'Hemoglobin',
        '4544-3': 'Hematocrit',
        '787-2': 'Mean Corpuscular Volume',
        '785-6': 'Mean Corpuscular Hemoglobin',
        '786-4': 'Mean Corpuscular Hemoglobin Concentration',
        '788-0': 'RBC Distribution Width (CV)',
        '21000-5': 'RBC Distribution Width (SD)',
        '777-3': 'Platelet Count',
        '51631-0': 'Platelet Distribution Width',
        '51637-7': 'Plateletcrit',
        '32623-1': 'Mean Platelet Volume',
        '96354-6': 'Platelet Large Cell Count',
        '48386-7': 'Platelet Large Cell Ratio',
        '731-0': 'Lymphocyte Count',
        '736-9': 'Lymphocyte %',
        '742-7': 'Monocyte Count',
        '5905-5': 'Monocyte %',
        '751-8': 'Neutrophil Count',
        '770-8': 'Neutrophil %',
        '711-2': 'Eosinophil Count',
        '713-8': 'Eosinophil %',
        '704-7': 'Basophil Count',
        '706-2': 'Basophil %',
        '55432-9': 'Immature Cell Count',
        '55433-7': 'Immature Cell %',
        '43743-4': 'Atypical Lymphocyte Count',
        '42250-1': 'Atypical Lymphocyte %',
        '53115-2': 'Immature Granulocyte Count',
        '71695-1': 'Immature Granulocyte %',
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info("ASTM Parser initialized")
        self.config = config or {}
        # Merge user LOINC codes with defaults
        self.loinc_codes = {**self._LOINC_CODES, **self.config.get('loinc_codes', {})}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def parse(self, astm_message: str) -> Dict[str, Any]:
        """
        Parse a complete ASTM E1394 message string.

        Returns a dict with keys:
          protocol, timestamp, header, patient, orders (list),
          results (list), comments (list), query, terminator
        """
        try:
            cleaned = self._strip_framing(astm_message)
            records = self._split_records(cleaned)

            # Peek at the H record first so we know what delimiters to use
            field_sep, component_sep, repeat_sep, escape_char = \
                self._detect_delimiters(records)

            data: Dict[str, Any] = {
                'protocol': 'ASTM',
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }

            for record in records:
                if not record:
                    continue

                record_type = record[0].upper()
                fields = record.split(field_sep)

                if record_type == 'H':
                    data['header'] = self._parse_header(
                        fields, field_sep, component_sep, repeat_sep, escape_char)
                elif record_type == 'P':
                    data['patient'] = self._parse_patient(
                        fields, field_sep, component_sep, repeat_sep, escape_char)
                elif record_type == 'O':
                    data.setdefault('orders', []).append(
                        self._parse_order(fields, field_sep, component_sep, repeat_sep, escape_char))
                elif record_type == 'R':
                    data.setdefault('results', []).append(
                        self._parse_result(fields, field_sep, component_sep, repeat_sep, escape_char))
                elif record_type == 'C':
                    data.setdefault('comments', []).append(
                        self._parse_comment(fields, field_sep, component_sep, repeat_sep, escape_char))
                elif record_type == 'Q':
                    data.setdefault('queries', []).append(
                        self._parse_query(fields, field_sep, component_sep, repeat_sep, escape_char))
                elif record_type == 'M':
                    data.setdefault('manufacturer_records', []).append(
                        self._parse_manufacturer(fields, field_sep, component_sep, repeat_sep, escape_char))
                elif record_type == 'L':
                    data['terminator'] = self._parse_terminator(fields, field_sep, component_sep, repeat_sep, escape_char)
                else:
                    # Unknown record type — preserve raw
                    data.setdefault('unknown_records', []).append(
                        {'record_type': record_type, 'raw': record})

            # Post-processing: enrich sparse Patient record from Order data
            # (Sysmex instruments embed patient info in O record specimen_id)
            patient = data.get('patient', {})
            for order in data.get('orders', []):
                sid = order.get('specimen_id_parsed', {})
                if sid:
                    if not patient.get('name') and sid.get('patient_name'):
                        patient['name'] = {'last': sid['patient_name'], 'first': None,
                                           'middle': None, 'suffix': None, 'prefix': None}
                    if not patient.get('practice_patient_id') and sid.get('accession_number'):
                        patient['practice_patient_id'] = sid['accession_number']
                    break  # use first order only
            if patient:
                data['patient'] = patient

            return data

        except Exception as e:
            logger.error(f"Error parsing ASTM message: {e}")
            return {
                'protocol': 'ASTM',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
                'raw_message': astm_message,
            }

    # ------------------------------------------------------------------
    # Pre-processing helpers
    # ------------------------------------------------------------------

    def _strip_framing(self, message: str) -> str:
        """
        Remove ASTM low-level framing bytes that may be present when the
        TCP layer has not already unwrapped them.

        A framed transmission looks like:
          <STX> <frame_number_digit> <data> <ETX|ETB> <2-hex checksum> <CR> <LF>

        After stripping we are left with concatenated record data which can
        then be split on CR.
        """
        result_parts: List[str] = []
        i = 0
        while i < len(message):
            if message[i] == _STX:
                i += 1  # skip STX
                # Skip single-digit frame number if present
                if i < len(message) and message[i].isdigit():
                    i += 1
                # Collect data until ETX or ETB
                data_start = i
                while i < len(message) and message[i] not in (_ETX, _ETB):
                    i += 1
                result_parts.append(message[data_start:i])
                if i < len(message):
                    i += 1  # skip ETX / ETB
                # Skip 2-byte checksum if present
                if i + 2 <= len(message) and all(
                        c in '0123456789ABCDEFabcdef'
                        for c in message[i:i + 2]):
                    i += 2
                # Skip trailing CR LF
                while i < len(message) and message[i] in (_CR, _LF):
                    i += 1
            else:
                result_parts.append(message[i])
                i += 1

        cleaned = ''.join(result_parts)
        return cleaned

    def _split_records(self, message: str) -> List[str]:
        """
        Split on CR (ASTM record separator).
        Also tolerates CR+LF and bare LF line endings.
        Discards empty lines.
        """
        # Normalise CRLF → CR, then bare LF → CR
        message = message.replace('\r\n', '\r').replace('\n', '\r')
        return [r.strip() for r in message.split('\r') if r.strip()]

    def _detect_delimiters(
            self, records: List[str]
    ) -> Tuple[str, str, str, str]:
        """
        Read delimiter definitions from the H record.

        ASTM H record format (field-separated by the very first delimiter):
          H|<field_sep_chars>|...
          where <field_sep_chars> is usually ^\\&  (component^repeat\\escape)

        Returns (field_sep, component_sep, repeat_sep, escape_char).
        """
        for record in records:
            if record and record[0].upper() == 'H':
                # H record: H|delims|...
                # The character immediately after H is the field separator.
                if len(record) < 2:
                    break
                field_sep = record[1]  # typically '|'
                # The next few characters are component/repeat/escape delimiters
                if len(record) > 2:
                    delim_field = record[2:].split(field_sep)[0]

                    # Instruments vary between `^\\&` and `\\^&` ordering.
                    # Prefer the standard ASTM characters when present so both
                    # declarations normalize to component=`^`, repeat=`\\`, escape=`&`.
                    component_sep = '^' if '^' in delim_field else (
                        delim_field[0] if len(delim_field) > 0 else self._DEFAULT_COMPONENT_SEP
                    )
                    repeat_sep = '\\' if '\\' in delim_field else (
                        delim_field[1] if len(delim_field) > 1 else self._DEFAULT_REPEAT_SEP
                    )
                    escape_char = '&' if '&' in delim_field else (
                        delim_field[2] if len(delim_field) > 2 else self._DEFAULT_ESCAPE
                    )
                    return field_sep, component_sep, repeat_sep, escape_char

        return (self._DEFAULT_FIELD_SEP, self._DEFAULT_COMPONENT_SEP,
                self._DEFAULT_REPEAT_SEP, self._DEFAULT_ESCAPE)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _unescape_field(self, value: str, component_sep: str, repeat_sep: str, 
                        escape_char: str, field_sep: str) -> str:
        r"""Decode ASTM E1394 escape sequences in field values.
        
        ASTM escape sequences (per E1394-97 §5.5.2):
        \S\ - component separator (typically ^)
        \R\ - repeat delimiter (typically \)
        \E\ - escape character (typically &)
        \F\ - field delimiter (typically |)
        \T\ - subcomponent separator
        \X##\ - hexadecimal character (## = 2 hex digits)
        """
        if not value or repeat_sep not in value:
            return value
        
        result = []
        i = 0
        while i < len(value):
            # Look for escape sequences starting with repeat_sep
            if i < len(value) - 2 and value[i] == repeat_sep:
                seq = value[i:i+3] if i+3 <= len(value) else value[i:]
                
                # Check for standard escape sequences
                if seq == repeat_sep + 'S' + repeat_sep:
                    result.append(component_sep)
                    i += 3
                    continue
                elif seq == repeat_sep + 'R' + repeat_sep:
                    result.append(repeat_sep)
                    i += 3
                    continue
                elif seq == repeat_sep + 'E' + repeat_sep:
                    result.append(escape_char)
                    i += 3
                    continue
                elif seq == repeat_sep + 'F' + repeat_sep:
                    result.append(field_sep)
                    i += 3
                    continue
                elif seq == repeat_sep + 'T' + repeat_sep:
                    result.append('&')  # subcomponent separator
                    i += 3
                    continue
                # Check for hex escape \X##\
                elif i < len(value) - 4 and value[i:i+2] == repeat_sep + 'X':
                    hex_seq = value[i:i+5] if i+5 <= len(value) else None
                    if hex_seq and len(hex_seq) == 5 and hex_seq[4] == repeat_sep:
                        try:
                            hex_code = hex_seq[2:4]
                            char = chr(int(hex_code, 16))
                            result.append(char)
                            i += 5
                            continue
                        except ValueError:
                            pass
            
            result.append(value[i])
            i += 1
        
        return ''.join(result)

    def _f(self, fields: List[str], index: int, default: Optional[str] = None,
           unescape: bool = True, component_sep: str = '^', repeat_sep: str = '\\',
           escape_char: str = '&', field_sep: str = '|') -> Optional[str]:
        """Return fields[index] stripped and optionally unescaped, or default if absent/empty."""
        try:
            val = fields[index].strip()
            if not val:
                return default
            if unescape and self.config.get('enable_escape_decoding', True):
                val = self._unescape_field(val, component_sep, repeat_sep, escape_char, field_sep)
            return val
        except IndexError:
            return default

    def _components(self, value: Optional[str],
                    component_sep: str, unescape: bool = True,
                    repeat_sep: str = '\\', escape_char: str = '&', 
                    field_sep: str = '|') -> List[Optional[str]]:
        """Split a field value into components, returning None for empty parts."""
        if not value:
            return []
        # First unescape the entire value if requested
        if unescape and self.config.get('enable_escape_decoding', True):
            value = self._unescape_field(value, component_sep, repeat_sep, escape_char, field_sep)
        parts = value.split(component_sep)
        return [p.strip() if p.strip() else None for p in parts]
    
    def _parse_reference_range(self, value: Optional[str]) -> Optional[Dict[str, Any]]:
        """Parse reference range strings like '4.00 - 11.00^REFERENCE_RANGE' or '4.5-11.0'."""
        if not value or not value.strip():
            return None
        
        # Remove any trailing descriptive component (e.g., ^REFERENCE_RANGE)
        range_str = value.split('^')[0].strip()
        
        # Try to parse 'min - max' format
        if ' - ' in range_str:
            parts = range_str.split(' - ')
            if len(parts) == 2:
                try:
                    return {
                        'min': float(parts[0].strip()),
                        'max': float(parts[1].strip()),
                        'raw': value
                    }
                except ValueError:
                    pass
        # Try 'min-max' format (no spaces)
        elif '-' in range_str and not range_str.startswith('-'):
            # Split on last hyphen to handle negative numbers
            parts = range_str.rsplit('-', 1)
            if len(parts) == 2:
                try:
                    return {
                        'min': float(parts[0].strip()),
                        'max': float(parts[1].strip()),
                        'raw': value
                    }
                except ValueError:
                    pass
        
        # Return raw if can't parse
        return {'raw': value}
    
    def _get_test_name(self, test_id_dict: Optional[Dict[str, Any]]) -> Optional[str]:
        """Get human-readable test name from test ID dict, checking LOINC codes."""
        if not test_id_dict:
            return None
        
        # First check if test_name is already provided
        if test_id_dict.get('test_name'):
            return test_id_dict['test_name']
        
        # Check all components for LOINC code (instruments vary in format)
        # Look for a component that matches a LOINC pattern (digits-digits)
        for key in ['loinc_code', 'test_code', 'test_id', 'mnemonic']:
            value = test_id_dict.get(key)
            if value and value in self.loinc_codes:
                return self.loinc_codes[value]
        
        # Also check the raw value for LOINC patterns
        raw = test_id_dict.get('raw', '')
        if raw:
            # Check if any part matches a known LOINC code
            for loinc_code, name in self.loinc_codes.items():
                if loinc_code in raw:
                    return name
        
        # Fallback to test_id or mnemonic
        return test_id_dict.get('mnemonic') or test_id_dict.get('test_id')

    def _parse_name(self, value: Optional[str],
                    component_sep: str, repeat_sep: str = '\\',
                    escape_char: str = '&', field_sep: str = '|') -> Optional[Dict[str, Optional[str]]]:
        """
        Parse a Name field (Last^First^Middle^Suffix^Prefix).
        Returns None when the value is absent.
        """
        if not value or not value.strip():
            return None
        parts = self._components(value, component_sep, unescape=True, 
                                repeat_sep=repeat_sep, escape_char=escape_char, field_sep=field_sep)
        return {
            'last':   parts[0] if len(parts) > 0 else None,
            'first':  parts[1] if len(parts) > 1 else None,
            'middle': parts[2] if len(parts) > 2 else None,
            'suffix': parts[3] if len(parts) > 3 else None,
            'prefix': parts[4] if len(parts) > 4 else None,
        }

    def _parse_universal_test_id(
            self, value: Optional[str],
            component_sep: str, repeat_sep: str = '\\',
            escape_char: str = '&', field_sep: str = '|') -> Optional[Dict[str, Optional[str]]]:
        """
        Parse a Universal Test ID field
        (TestID^TestName^LOINC^Manufacturer^SpecimenType^TestCode^Mnemonic).
        """
        if not value or not value.strip():
            return None
        parts = self._components(value, component_sep, unescape=True,
                                repeat_sep=repeat_sep, escape_char=escape_char, field_sep=field_sep)
        result: Dict[str, Optional[str]] = {
            'test_id':       parts[0] if len(parts) > 0 else None,
            'test_name':     parts[1] if len(parts) > 1 else None,
            'loinc_code':    parts[2] if len(parts) > 2 else None,
            'manufacturer':  parts[3] if len(parts) > 3 else None,
            'specimen_type': parts[4] if len(parts) > 4 else None,
            'test_code':     parts[5] if len(parts) > 5 else None,
            'mnemonic':      parts[6] if len(parts) > 6 else None,
        }
        # Keep the raw string as well so callers can use it directly
        result['raw'] = value.strip()
        # Add human-readable name from LOINC if available
        result['display_name'] = self._get_test_name(result)
        return result

    # ------------------------------------------------------------------
    # Record parsers
    # ------------------------------------------------------------------

    def _parse_header(self, fields: List[str], field_sep: str, component_sep: str,
                      repeat_sep: str, escape_char: str) -> Dict[str, Any]:
        """
        H record — Message Header (LIS2-A2 §5.6)

        Index  Field
        -----  -----
        0      H
        1      Field Delimiter + Delimiters (e.g. ^\\&)
        2      Message Control ID
        3      Access Password
        4      Sender Name
        5      Sender Street Address
        6      Reserved
        7      Sender Telephone Number
        8      Sender Characteristics
        9      Receiver ID
        10     Comments
        11     Processing ID  (P=Production, T=Training, D=Debugging)
        12     Version Number  (LIS2-A2 or numeric e.g. E 1394-97)
        13     Date and Time of Message
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        return {
            'record_type':          'Header',
            'field_delimiter':      f(1),
            'message_control_id':   f(2),
            'access_password':      f(3),
            'sender_name':          f(4),
            'sender_address':       f(5),
            'sender_telephone':     f(7),
            'sender_characteristics': f(8),
            'receiver_id':          f(9),
            'comments':             f(10),
            'processing_id':        f(11),
            'version':              f(12),
            'timestamp':            f(13),
        }

    def _parse_patient(self, fields: List[str], field_sep: str,
                       component_sep: str, repeat_sep: str = '\\',
                       escape_char: str = '&') -> Dict[str, Any]:
        """
        P record — Patient Information (LIS2-A2 §5.7)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Practice Assigned Patient ID
        3      Laboratory Assigned Patient ID
        4      Patient ID No. 3
        5      Patient Name  (Last^First^Middle)
        6      Mother's Maiden Name
        7      Birthdate
        8      Patient Sex  (M/F/U)
        9      Patient Race / Ethnic Origin
        10     Patient Address
        11     Reserved
        12     Patient Telephone Number
        13     Attending Physician  (Last^First)
        14     Special Field 1
        15     Special Field 2
        16     Patient Height
        17     Patient Weight
        18     Known/Suspected Diagnosis
        19     Active Medications
        20     Patient Diet
        21     Practice Field 1
        22     Practice Field 2
        23     Admission/Discharge Dates
        24     Admission Status
        25     Location
        26     Diagnostic Code Type
        27     Diagnostic Code
        28     Patient Religion
        29     Marital Status
        30     Isolation Status
        31     Language
        32     Hospital Service
        33     Hospital Institution
        34     Dosage Category
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        return {
            'record_type':              'Patient',
            'sequence':                 f(1),
            'practice_patient_id':      f(2),
            'lab_patient_id':           f(3),
            'patient_id_3':             f(4),
            'name':                     self._parse_name(f(5), component_sep, repeat_sep, escape_char),
            'mothers_maiden_name':      f(6),
            'date_of_birth':            f(7),
            'sex':                      f(8),
            'race_ethnic_origin':       f(9),
            'address':                  f(10),
            'telephone':                f(12),
            'attending_physician':      self._parse_name(f(13), component_sep, repeat_sep, escape_char),
            'special_field_1':          f(14),
            'special_field_2':          f(15),
            'height':                   f(16),
            'weight':                   f(17),
            'diagnosis':                f(18),
            'active_medications':       f(19),
            'diet':                     f(20),
            'practice_field_1':         f(21),
            'practice_field_2':         f(22),
            'admission_discharge_dates': f(23),
            'admission_status':         f(24),
            'location':                 f(25),
            'diagnostic_code_type':     f(26),
            'diagnostic_code':          f(27),
            'religion':                 f(28),
            'marital_status':           f(29),
            'isolation_status':         f(30),
            'language':                 f(31),
            'hospital_service':         f(32),
            'hospital_institution':     f(33),
            'dosage_category':          f(34),
        }

    def _parse_order(self, fields: List[str], field_sep: str,
                     component_sep: str, repeat_sep: str = '\\',
                     escape_char: str = '&') -> Dict[str, Any]:
        """
        O record — Test Order Record (LIS2-A2 §5.8)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Specimen ID
        3      Instrument Specimen ID
        4      Universal Test ID
        5      Priority  (S=Stat, R=Routine, A=ASAP, C=Callback, P=PreOp)
        6      Requested/Ordered Date and Time
        7      Specimen Collection Date and Time
        8      Collection End Time
        9      Collection Volume
        10     Collector ID
        11     Action Code  (A=Add, C=Cancel, D=Delete, G=NG...)
        12     Danger Code
        13     Relevant Clinical Information
        14     Date/Time Specimen Received
        15     Specimen Descriptor
        16     Ordering Physician
        17     Physician Telephone Number
        18     User Field 1
        19     User Field 2
        20     Laboratory Field 1
        21     Laboratory Field 2
        22     Date/Time Results Reported
        23     Instrument Charge
        24     Instrument Section ID
        25     Report Type  (O=Preliminary, F=Final, X=Not done...)
        26     Reserved
        27     Location of Specimen Collector
        28     Nosocomial Infection Flag
        29     Specimen Service
        30     Specimen Institution
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        record = {
            'record_type':              'Order',
            'sequence':                 f(1),
            'specimen_id':              f(2),
            'instrument_specimen_id':   f(3),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(4), component_sep, repeat_sep, escape_char, field_sep),
            'priority':                 f(5),
            'ordered_datetime':         f(6),
            'collection_datetime':      f(7),
            'collection_end_time':      f(8),
            'collection_volume':        f(9),
            'collector_id':             f(10),
            'action_code':              f(11),
            'danger_code':              f(12),
            'clinical_information':     f(13),
            'specimen_received_datetime': f(14),
            'specimen_descriptor':      f(15),
            'ordering_physician':       self._parse_name(f(16), component_sep, repeat_sep, escape_char),
            'physician_telephone':      f(17),
            'user_field_1':             f(18),
            'user_field_2':             f(19),
            'lab_field_1':              f(20),
            'lab_field_2':              f(21),
            'results_reported_datetime': f(22),
            'instrument_charge':        f(23),
            'instrument_section_id':    f(24),
            'report_type':              f(25),
            'specimen_collector_location': f(27),
            'nosocomial_infection_flag': f(28),
            'specimen_service':         f(29),
            'specimen_institution':     f(30),
        }
        # Sysmex convention: specimen_id encodes PatientName^^AccessionNo^SeqNo
        specimen_id_raw = record.get('specimen_id')
        if specimen_id_raw and component_sep in specimen_id_raw:
            sid_parts = specimen_id_raw.split(component_sep)
            record['specimen_id_parsed'] = {
                'patient_name':    sid_parts[0] if len(sid_parts) > 0 and sid_parts[0] else None,
                'field_2':         sid_parts[1] if len(sid_parts) > 1 and sid_parts[1] else None,
                'accession_number': sid_parts[2] if len(sid_parts) > 2 and sid_parts[2] else None,
                'sequence':        sid_parts[3] if len(sid_parts) > 3 and sid_parts[3] else None,
            }
        return record

    def _parse_result(self, fields: List[str], field_sep: str,
                      component_sep: str, repeat_sep: str = '\\',
                      escape_char: str = '&') -> Dict[str, Any]:
        """
        R record — Result Record (LIS2-A2 §5.9)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Universal Test ID
        3      Data or Measurement Value
        4      Units
        5      Reference Ranges
        6      Result Abnormal Flags
        7      Nature of Abnormality Testing
        8      Result Status  (C=Correction, F=Final, I=Pending, P=Preliminary, X=No result)
        9      Date of Change in Normative Values
        10     Operator Identification
        11     Date/Time Test Started
        12     Date/Time Test Completed
        13     Instrument Identification
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        
        ref_range_raw = f(5)
        result = {
            'record_type':              'Result',
            'sequence':                 f(1),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(2), component_sep, repeat_sep, escape_char, field_sep),
            'value':                    f(3),
            'units':                    f(4),
            'reference_range':          ref_range_raw,
            'reference_range_parsed':   self._parse_reference_range(ref_range_raw),
            'abnormal_flags':           f(6),
            'abnormality_nature':       f(7),
            'result_status':            f(8),
            'normative_change_date':    f(9),
            'operator_id':              f(10),
            'test_started_datetime':    f(11),
            'test_completed_datetime':  f(12),
            'instrument_id':            f(13),
        }
        return result

    def _parse_comment(self, fields: List[str], field_sep: str,
                       component_sep: str, repeat_sep: str = '\\',
                       escape_char: str = '&') -> Dict[str, Any]:
        """
        C record — Comment Record (LIS2-A2 §5.10)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Comment Source  (L=Lab, I=Instrument, P=Patient...)
        3      Comment Text
        4      Comment Type
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        
        comment_text = f(3)
        # Parse comment text to extract individual flags/codes if separated by delimiters
        parsed_comments = None
        if comment_text:
            # Split on repeat separator if present (e.g., \S for embedded separators)
            parsed_comments = self._components(comment_text, component_sep, unescape=True,
                                              repeat_sep=repeat_sep, escape_char=escape_char)
        
        return {
            'record_type':    'Comment',
            'sequence':       f(1),
            'comment_source': f(2),
            'comment_text':   comment_text,
            'comment_type':   f(4),
            'parsed_comments': parsed_comments,
        }

    def _parse_query(self, fields: List[str], field_sep: str,
                     component_sep: str, repeat_sep: str = '\\',
                     escape_char: str = '&') -> Dict[str, Any]:
        """
        Q record — Request Information Record (LIS2-A2 §5.11)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Starting Range ID Number
        3      Ending Range ID Number
        4      Universal Test ID
        5      Nature of Request Time Limits
        6      Beginning Request Results Date/Time
        7      Ending Request Results Date/Time
        8      Requesting Physician Name
        9      Requesting Physician Telephone
        10     User Field 1
        11     User Field 2
        12     Request Information Status Codes
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        return {
            'record_type':              'Query',
            'sequence':                 f(1),
            'starting_range_id':        f(2),
            'ending_range_id':          f(3),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(4), component_sep, repeat_sep, escape_char, field_sep),
            'time_limits':              f(5),
            'begin_results_datetime':   f(6),
            'end_results_datetime':     f(7),
            'requesting_physician':     self._parse_name(f(8), component_sep, repeat_sep, escape_char),
            'physician_telephone':      f(9),
            'user_field_1':             f(10),
            'user_field_2':             f(11),
            'status_codes':             f(12),
        }

    def _parse_manufacturer(self, fields: List[str], field_sep: str,
                             component_sep: str, repeat_sep: str = '\\',
                             escape_char: str = '&') -> Dict[str, Any]:
        """
        M record — Manufacturer Information Record (LIS2-A2 §5.12)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Definition Scope  (I=Instrument, P=Practice...)
        3      Name of Implementation-Specific Definition
        4-13   Implementation-specific fields
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        record: Dict[str, Any] = {
            'record_type':            'Manufacturer',
            'sequence':               f(1),
            'definition_scope':       f(2),
            'definition_name':        f(3),
        }
        # Capture any extra implementation-specific fields
        extras = [f(i) for i in range(4, min(14, len(fields)))]
        non_null = [v for v in extras if v is not None]
        if non_null:
            record['implementation_fields'] = extras
        return record

    def _parse_terminator(self, fields: List[str], field_sep: str = '|', component_sep: str = '^',
                         repeat_sep: str = '\\', escape_char: str = '&') -> Dict[str, Any]:
        """
        L record — Message Terminator (LIS2-A2 §5.13)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Termination Code  (N=Normal, I=Not asking, P=Process, Q=Query...)
        """
        f = lambda idx, default=None: self._f(fields, idx, default, unescape=True,
                                              component_sep=component_sep, repeat_sep=repeat_sep,
                                              escape_char=escape_char, field_sep=field_sep)
        return {
            'record_type':      'Terminator',
            'sequence':         f(1),
            'termination_code': f(2),
        }


__all__ = ["ASTMParser"]
