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
from datetime import datetime
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

    def __init__(self):
        logger.info("ASTM Parser initialized")

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
                'timestamp': datetime.utcnow().isoformat(),
            }

            for record in records:
                if not record:
                    continue

                record_type = record[0].upper()
                fields = record.split(field_sep)

                if record_type == 'H':
                    data['header'] = self._parse_header(
                        fields, component_sep, repeat_sep, escape_char)
                elif record_type == 'P':
                    data['patient'] = self._parse_patient(
                        fields, component_sep)
                elif record_type == 'O':
                    data.setdefault('orders', []).append(
                        self._parse_order(fields, component_sep))
                elif record_type == 'R':
                    data.setdefault('results', []).append(
                        self._parse_result(fields, component_sep))
                elif record_type == 'C':
                    data.setdefault('comments', []).append(
                        self._parse_comment(fields, component_sep))
                elif record_type == 'Q':
                    data.setdefault('queries', []).append(
                        self._parse_query(fields, component_sep))
                elif record_type == 'M':
                    data.setdefault('manufacturer_records', []).append(
                        self._parse_manufacturer(fields, component_sep))
                elif record_type == 'L':
                    data['terminator'] = self._parse_terminator(fields)
                else:
                    # Unknown record type — preserve raw
                    data.setdefault('unknown_records', []).append(
                        {'record_type': record_type, 'raw': record})

            return data

        except Exception as e:
            logger.error(f"Error parsing ASTM message: {e}")
            return {
                'protocol': 'ASTM',
                'timestamp': datetime.utcnow().isoformat(),
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
                    component_sep = delim_field[0] if len(delim_field) > 0 \
                        else self._DEFAULT_COMPONENT_SEP
                    repeat_sep   = delim_field[1] if len(delim_field) > 1 \
                        else self._DEFAULT_REPEAT_SEP
                    escape_char  = delim_field[2] if len(delim_field) > 2 \
                        else self._DEFAULT_ESCAPE
                    return field_sep, component_sep, repeat_sep, escape_char

        return (self._DEFAULT_FIELD_SEP, self._DEFAULT_COMPONENT_SEP,
                self._DEFAULT_REPEAT_SEP, self._DEFAULT_ESCAPE)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _f(fields: List[str], index: int, default: Optional[str] = None
           ) -> Optional[str]:
        """Return fields[index] stripped, or default if absent/empty."""
        try:
            val = fields[index].strip()
            return val if val else default
        except IndexError:
            return default

    def _components(self, value: Optional[str],
                    component_sep: str) -> List[Optional[str]]:
        """Split a field value into components, returning None for empty parts."""
        if not value:
            return []
        parts = value.split(component_sep)
        return [p.strip() if p.strip() else None for p in parts]

    def _parse_name(self, value: Optional[str],
                    component_sep: str) -> Optional[Dict[str, Optional[str]]]:
        """
        Parse a Name field (Last^First^Middle^Suffix^Prefix).
        Returns None when the value is absent.
        """
        if not value or not value.strip():
            return None
        parts = self._components(value, component_sep)
        return {
            'last':   parts[0] if len(parts) > 0 else None,
            'first':  parts[1] if len(parts) > 1 else None,
            'middle': parts[2] if len(parts) > 2 else None,
            'suffix': parts[3] if len(parts) > 3 else None,
            'prefix': parts[4] if len(parts) > 4 else None,
        }

    def _parse_universal_test_id(
            self, value: Optional[str],
            component_sep: str) -> Optional[Dict[str, Optional[str]]]:
        """
        Parse a Universal Test ID field
        (TestID^TestName^LOINC^Manufacturer^SpecimenType^TestCode^Mnemonic).
        """
        if not value or not value.strip():
            return None
        parts = self._components(value, component_sep)
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
        return result

    # ------------------------------------------------------------------
    # Record parsers
    # ------------------------------------------------------------------

    def _parse_header(self, fields: List[str], component_sep: str,
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
        f = self._f
        return {
            'record_type':          'Header',
            'field_delimiter':      f(fields, 1),
            'message_control_id':   f(fields, 2),
            'access_password':      f(fields, 3),
            'sender_name':          f(fields, 4),
            'sender_address':       f(fields, 5),
            'sender_telephone':     f(fields, 7),
            'sender_characteristics': f(fields, 8),
            'receiver_id':          f(fields, 9),
            'comments':             f(fields, 10),
            'processing_id':        f(fields, 11),
            'version':              f(fields, 12),
            'timestamp':            f(fields, 13),
        }

    def _parse_patient(self, fields: List[str],
                       component_sep: str) -> Dict[str, Any]:
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
        f = self._f
        return {
            'record_type':              'Patient',
            'sequence':                 f(fields, 1),
            'practice_patient_id':      f(fields, 2),
            'lab_patient_id':           f(fields, 3),
            'patient_id_3':             f(fields, 4),
            'name':                     self._parse_name(f(fields, 5), component_sep),
            'mothers_maiden_name':      f(fields, 6),
            'date_of_birth':            f(fields, 7),
            'sex':                      f(fields, 8),
            'race_ethnic_origin':       f(fields, 9),
            'address':                  f(fields, 10),
            'telephone':                f(fields, 12),
            'attending_physician':      self._parse_name(f(fields, 13), component_sep),
            'special_field_1':          f(fields, 14),
            'special_field_2':          f(fields, 15),
            'height':                   f(fields, 16),
            'weight':                   f(fields, 17),
            'diagnosis':                f(fields, 18),
            'active_medications':       f(fields, 19),
            'diet':                     f(fields, 20),
            'practice_field_1':         f(fields, 21),
            'practice_field_2':         f(fields, 22),
            'admission_discharge_dates': f(fields, 23),
            'admission_status':         f(fields, 24),
            'location':                 f(fields, 25),
            'diagnostic_code_type':     f(fields, 26),
            'diagnostic_code':          f(fields, 27),
            'religion':                 f(fields, 28),
            'marital_status':           f(fields, 29),
            'isolation_status':         f(fields, 30),
            'language':                 f(fields, 31),
            'hospital_service':         f(fields, 32),
            'hospital_institution':     f(fields, 33),
            'dosage_category':          f(fields, 34),
        }

    def _parse_order(self, fields: List[str],
                     component_sep: str) -> Dict[str, Any]:
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
        f = self._f
        return {
            'record_type':              'Order',
            'sequence':                 f(fields, 1),
            'specimen_id':              f(fields, 2),
            'instrument_specimen_id':   f(fields, 3),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(fields, 4), component_sep),
            'priority':                 f(fields, 5),
            'ordered_datetime':         f(fields, 6),
            'collection_datetime':      f(fields, 7),
            'collection_end_time':      f(fields, 8),
            'collection_volume':        f(fields, 9),
            'collector_id':             f(fields, 10),
            'action_code':              f(fields, 11),
            'danger_code':              f(fields, 12),
            'clinical_information':     f(fields, 13),
            'specimen_received_datetime': f(fields, 14),
            'specimen_descriptor':      f(fields, 15),
            'ordering_physician':       self._parse_name(f(fields, 16), component_sep),
            'physician_telephone':      f(fields, 17),
            'user_field_1':             f(fields, 18),
            'user_field_2':             f(fields, 19),
            'lab_field_1':              f(fields, 20),
            'lab_field_2':              f(fields, 21),
            'results_reported_datetime': f(fields, 22),
            'instrument_charge':        f(fields, 23),
            'instrument_section_id':    f(fields, 24),
            'report_type':              f(fields, 25),
            'specimen_collector_location': f(fields, 27),
            'nosocomial_infection_flag': f(fields, 28),
            'specimen_service':         f(fields, 29),
            'specimen_institution':     f(fields, 30),
        }

    def _parse_result(self, fields: List[str],
                      component_sep: str) -> Dict[str, Any]:
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
        f = self._f
        return {
            'record_type':              'Result',
            'sequence':                 f(fields, 1),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(fields, 2), component_sep),
            'value':                    f(fields, 3),
            'units':                    f(fields, 4),
            'reference_range':          f(fields, 5),
            'abnormal_flags':           f(fields, 6),
            'abnormality_nature':       f(fields, 7),
            'result_status':            f(fields, 8),
            'normative_change_date':    f(fields, 9),
            'operator_id':              f(fields, 10),
            'test_started_datetime':    f(fields, 11),
            'test_completed_datetime':  f(fields, 12),
            'instrument_id':            f(fields, 13),
        }

    def _parse_comment(self, fields: List[str],
                       component_sep: str) -> Dict[str, Any]:
        """
        C record — Comment Record (LIS2-A2 §5.10)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Comment Source  (L=Lab, I=Instrument, P=Patient...)
        3      Comment Text
        4      Comment Type
        """
        f = self._f
        return {
            'record_type':    'Comment',
            'sequence':       f(fields, 1),
            'comment_source': f(fields, 2),
            'comment_text':   f(fields, 3),
            'comment_type':   f(fields, 4),
        }

    def _parse_query(self, fields: List[str],
                     component_sep: str) -> Dict[str, Any]:
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
        f = self._f
        return {
            'record_type':              'Query',
            'sequence':                 f(fields, 1),
            'starting_range_id':        f(fields, 2),
            'ending_range_id':          f(fields, 3),
            'universal_test_id':        self._parse_universal_test_id(
                                            f(fields, 4), component_sep),
            'time_limits':              f(fields, 5),
            'begin_results_datetime':   f(fields, 6),
            'end_results_datetime':     f(fields, 7),
            'requesting_physician':     self._parse_name(f(fields, 8), component_sep),
            'physician_telephone':      f(fields, 9),
            'user_field_1':             f(fields, 10),
            'user_field_2':             f(fields, 11),
            'status_codes':             f(fields, 12),
        }

    def _parse_manufacturer(self, fields: List[str],
                             component_sep: str) -> Dict[str, Any]:
        """
        M record — Manufacturer Information Record (LIS2-A2 §5.12)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Definition Scope  (I=Instrument, P=Practice...)
        3      Name of Implementation-Specific Definition
        4-13   Implementation-specific fields
        """
        f = self._f
        record: Dict[str, Any] = {
            'record_type':            'Manufacturer',
            'sequence':               f(fields, 1),
            'definition_scope':       f(fields, 2),
            'definition_name':        f(fields, 3),
        }
        # Capture any extra implementation-specific fields
        extras = [f(fields, i) for i in range(4, min(14, len(fields)))]
        non_null = [v for v in extras if v is not None]
        if non_null:
            record['implementation_fields'] = extras
        return record

    def _parse_terminator(self, fields: List[str]) -> Dict[str, Any]:
        """
        L record — Message Terminator (LIS2-A2 §5.13)

        Index  Field
        -----  -----
        1      Sequence Number
        2      Termination Code  (N=Normal, I=Not asking, P=Process, Q=Query...)
        """
        return {
            'record_type':      'Terminator',
            'sequence':         self._f(fields, 1),
            'termination_code': self._f(fields, 2),
        }


__all__ = ["ASTMParser"]
