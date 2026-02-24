"""
HL7 Message Parser
Supports HL7 v2.1-2.8 and handles various message types from lab machines
(ORU, ORM, OUL, ACK, ADT, QRY).
"""

import hl7apy
from hl7apy.parser import parse_message
from hl7apy.exceptions import ParserError, UnsupportedVersion
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# MLLP framing bytes
_MLLP_START = '\x0b'   # VT  – start of block
_MLLP_END   = '\x1c'   # FS  – end of block
_MLLP_CR    = '\r'


class HL7Parser:
    """
    Parser for HL7 v2.x messages.
    Primarily designed for lab machine output (ORU, ORM, OUL messages).

    Handles:
    - MLLP framing byte stripping
    - Auto version detection with sequential fallback
    - Complex ORU_R01 nested group structures
    - Multiple OBR / OBX / NTE / DG1 / SPM segments
    - Structured name, coded-element, and identifier parsing
    - A string-level fallback parser for non-standard messages
    """

    def __init__(self):
        self.supported_versions = [
            '2.1', '2.2', '2.3', '2.3.1',
            '2.4', '2.5', '2.5.1', '2.6', '2.7', '2.8',
        ]
        logger.info("HL7 Parser initialized")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def parse(self, hl7_message: str) -> Dict[str, Any]:
        """
        Parse an HL7 message and return a JSON-friendly dict.

        Args:
            hl7_message: Raw HL7 message string (may include MLLP framing)

        Returns:
            Parsed message data
        """
        try:
            hl7_message = self._clean_message(hl7_message)

            parsed_msg = None
            detected_version = None

            try:
                parsed_msg = parse_message(hl7_message)
                detected_version = parsed_msg.version
                logger.info(f"Parsed HL7 message with version: {detected_version}")
            except (ParserError, UnsupportedVersion) as e:
                logger.warning(
                    f"Auto-detection failed: {e}, trying manual versions")
                for version in self.supported_versions:
                    try:
                        parsed_msg = parse_message(
                            hl7_message, version=version)
                        detected_version = version
                        logger.info(
                            f"Successfully parsed with version: {version}")
                        break
                    except Exception:
                        continue

            if not parsed_msg:
                logger.warning(
                    "Standard parsing failed, using fallback parser")
                return self._fallback_parse(hl7_message)

            return self._extract_message_data(parsed_msg, detected_version)

        except Exception as e:
            logger.error(f"Error parsing HL7 message: {e}")
            raise Exception(f"Failed to parse HL7 message: {e}")

    # ------------------------------------------------------------------
    # Pre-processing helpers
    # ------------------------------------------------------------------

    def _clean_message(self, message: str) -> str:
        """
        Normalise an HL7 message string.

        - Strip MLLP framing bytes (0x0B … 0x1C 0x0D)
        - Normalise line endings to CR (HL7 segment separator)
        - Strip leading / trailing whitespace
        """
        message = message.strip()

        # Strip MLLP wrapper if present
        if message.startswith(_MLLP_START):
            message = message[1:]
        if _MLLP_END in message:
            message = message[:message.index(_MLLP_END)]
        message = message.strip(_MLLP_CR).strip()

        # Normalise line endings
        if '\r\n' in message:
            message = message.replace('\r\n', '\r')
        elif '\n' in message and '\r' not in message:
            message = message.replace('\n', '\r')

        return message

    # ------------------------------------------------------------------
    # Structured data extraction
    # ------------------------------------------------------------------

    def _extract_message_data(self, parsed_msg,
                               version: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            'hl7_version': version,
            'timestamp': datetime.utcnow().isoformat(),
        }

        try:
            msh = parsed_msg.msh
            data['message_type'] = self._extract_message_type(msh)
            data['message_control_id'] = self._safe_get(
                msh, 'message_control_id')
            data['sending_application'] = self._safe_get(
                msh, 'sending_application')
            data['sending_facility'] = self._safe_get(
                msh, 'sending_facility')
            data['receiving_application'] = self._safe_get(
                msh, 'receiving_application')
            data['receiving_facility'] = self._safe_get(
                msh, 'receiving_facility')
            data['message_datetime'] = self._safe_get(
                msh, 'date_time_of_message')
            data['processing_id'] = self._safe_get(
                msh, 'processing_id')
            data['sequence_number'] = self._safe_get(
                msh, 'sequence_number')

            data['patient'] = self._extract_patient_info(parsed_msg)
            data['orders'] = self._extract_orders(parsed_msg)
            data['observations'] = self._extract_observations(parsed_msg)
            data['diagnoses'] = self._extract_diagnoses(parsed_msg)
            data['specimens'] = self._extract_specimens(parsed_msg)
            data['notes'] = self._extract_notes(parsed_msg)

            # Backwards-compatible convenience key
            if data['orders']:
                data['order'] = data['orders'][0]

        except Exception as e:
            logger.error(f"Error extracting message data: {e}")
            data['extraction_error'] = str(e)

        return data

    def _extract_message_type(self, msh) -> Optional[Dict[str, Any]]:
        """Return a structured message type dict from MSH.9."""
        raw = self._safe_get(msh, 'message_type')
        if not raw:
            return None
        parts = raw.split('^')
        return {
            'raw':               raw,
            'message_code':      parts[0] if len(parts) > 0 else None,
            'trigger_event':     parts[1] if len(parts) > 1 else None,
            'message_structure': parts[2] if len(parts) > 2 else None,
        }

    # ------------------------------------------------------------------
    # Segment extraction helpers
    # ------------------------------------------------------------------

    def _extract_patient_info(self,
                               parsed_msg) -> Optional[Dict[str, Any]]:
        try:
            if not hasattr(parsed_msg, 'pid'):
                return None
            pid = parsed_msg.pid
            return {
                'id': self._safe_get(pid, 'patient_id'),
                'identifier_list': self._safe_get(
                    pid, 'patient_identifier_list'),
                'account_number': self._safe_get(
                    pid, 'patient_account_number'),
                'ssn': self._safe_get(
                    pid, 'ssn_number_patient'),
                'name': self._parse_name(
                    self._safe_get(pid, 'patient_name')),
                'mothers_maiden_name': self._safe_get(
                    pid, 'mothers_maiden_name'),
                'date_of_birth': self._safe_get(
                    pid, 'date_time_of_birth'),
                'sex': self._safe_get(pid, 'administrative_sex'),
                'race': self._safe_get(pid, 'race'),
                'address': self._safe_get(pid, 'patient_address'),
                'phone_home': self._safe_get(
                    pid, 'phone_number_home'),
                'phone_business': self._safe_get(
                    pid, 'phone_number_business'),
                'primary_language': self._safe_get(
                    pid, 'primary_language'),
                'marital_status': self._safe_get(
                    pid, 'marital_status'),
                'religion': self._safe_get(pid, 'religion'),
                'ethnic_group': self._safe_get(
                    pid, 'ethnic_group'),
                'nationality': self._safe_get(pid, 'nationality'),
            }
        except Exception as e:
            logger.error(f"Error extracting patient info: {e}")
            return None

    def _extract_orders(self, parsed_msg) -> List[Dict[str, Any]]:
        """
        Extract all ORC + OBR pairs as a list of order dicts.
        Handles messages where OBR appears multiple times.
        """
        orders: List[Dict[str, Any]] = []
        try:
            # Collect OBR segments (may be a list or single object)
            obr_list = []
            if hasattr(parsed_msg, 'obr'):
                raw = parsed_msg.obr
                obr_list = raw if isinstance(raw, list) else [raw]

            # Collect ORC segments (may be fewer than OBRs)
            orc_list = []
            if hasattr(parsed_msg, 'orc'):
                raw = parsed_msg.orc
                orc_list = raw if isinstance(raw, list) else [raw]

            for i, obr in enumerate(obr_list):
                orc = orc_list[i] if i < len(orc_list) else None
                order = self._build_order(orc, obr)
                orders.append(order)

            # Also handle the ORU_R01 nested group structure
            if hasattr(parsed_msg, 'oru_r01_patient_result'):
                seen_ids: set = {
                    o.get('filler_order_number')
                    for o in orders
                    if o.get('filler_order_number')
                }
                for pr in parsed_msg.oru_r01_patient_result:
                    if not hasattr(pr, 'oru_r01_order_observation'):
                        continue
                    for oo in pr.oru_r01_order_observation:
                        obr = getattr(oo, 'obr', None)
                        orc = getattr(oo, 'orc', None)
                        if obr is None:
                            continue
                        filler = self._safe_get(
                            obr, 'filler_order_number')
                        if filler and filler in seen_ids:
                            continue  # already captured above
                        order = self._build_order(orc, obr)
                        orders.append(order)
                        if filler:
                            seen_ids.add(filler)

        except Exception as e:
            logger.error(f"Error extracting orders: {e}")

        return orders

    def _build_order(self, orc, obr) -> Dict[str, Any]:
        order: Dict[str, Any] = {}

        if orc is not None:
            order['order_control'] = self._safe_get(orc, 'order_control')
            order['placer_order_number'] = self._safe_get(
                orc, 'placer_order_number')
            order['filler_order_number'] = self._safe_get(
                orc, 'filler_order_number')
            order['order_status'] = self._safe_get(orc, 'order_status')
            order['ordering_provider'] = self._safe_get(
                orc, 'ordering_provider')
            order['entered_by'] = self._safe_get(orc, 'entered_by')
            order['order_effective_datetime'] = self._safe_get(
                orc, 'datetime_of_transaction')

        if obr is not None:
            order['set_id'] = self._safe_get(obr, 'set_id_obr')
            order['placer_order_number'] = order.get(
                'placer_order_number') or self._safe_get(
                    obr, 'placer_order_number')
            order['filler_order_number'] = order.get(
                'filler_order_number') or self._safe_get(
                    obr, 'filler_order_number')
            order['universal_service_id'] = self._safe_get(
                obr, 'universal_service_identifier')
            order['observation_datetime'] = self._safe_get(
                obr, 'observation_date_time')
            order['observation_end_datetime'] = self._safe_get(
                obr, 'observation_end_date_time')
            order['collection_volume'] = self._safe_get(
                obr, 'collection_volume')
            order['collector_identifier'] = self._safe_get(
                obr, 'collector_identifier')
            order['specimen_received_datetime'] = self._safe_get(
                obr, 'specimen_received_date_time')
            order['specimen_source'] = self._safe_get(
                obr, 'specimen_source')
            order['ordering_provider'] = order.get(
                'ordering_provider') or self._safe_get(
                    obr, 'ordering_provider')
            order['result_status'] = self._safe_get(obr, 'result_status')
            order['results_rpt_status_chng_datetime'] = self._safe_get(
                obr, 'results_rpt_status_chng_date_time')
            order['quantity_timing'] = self._safe_get(
                obr, 'quantity_timing')
            order['number_of_sample_containers'] = self._safe_get(
                obr, 'number_of_sample_containers')
            order['reason_for_study'] = self._safe_get(
                obr, 'reason_for_study')
            order['principal_result_interpreter'] = self._safe_get(
                obr, 'principal_result_interpreter')
            order['parent'] = self._safe_get(obr, 'parent')
            order['transport_arrangement_responsibility'] = self._safe_get(
                obr, 'transport_arrangement_responsibility')

        return order

    def _extract_observations(self,
                               parsed_msg) -> List[Dict[str, Any]]:
        """
        Extract all OBX segments, deduplicating across the two possible
        locations in hl7apy's parsed tree (top-level group vs oru_r01
        nested structure).
        """
        observations: List[Dict[str, Any]] = []
        # Use a set of (set_id, identifier_raw) to track what we've seen
        seen: set = set()

        def _add_obx(obx) -> None:
            obs = self._build_observation(obx)
            key = (obs.get('set_id'), obs.get('identifier'))
            if key in seen and key != (None, None):
                return
            seen.add(key)
            observations.append(obs)

        try:
            # Top-level OBX access
            if hasattr(parsed_msg, 'obx'):
                raw = parsed_msg.obx
                obx_list = raw if isinstance(raw, list) else [raw]
                for obx in obx_list:
                    try:
                        _add_obx(obx)
                    except Exception as e:
                        logger.error(
                            f"Error extracting OBX segment: {e}")

            # ORU_R01 nested group structure
            if hasattr(parsed_msg, 'oru_r01_patient_result'):
                for pr in parsed_msg.oru_r01_patient_result:
                    if not hasattr(pr, 'oru_r01_order_observation'):
                        continue
                    for oo in pr.oru_r01_order_observation:
                        if not hasattr(oo, 'oru_r01_observation'):
                            continue
                        for og in oo.oru_r01_observation:
                            obx = getattr(og, 'obx', None)
                            if obx is None:
                                continue
                            try:
                                _add_obx(obx)
                            except Exception as e:
                                logger.error(
                                    f"Error extracting nested OBX: {e}")

        except Exception as e:
            logger.error(f"Error extracting observations: {e}")

        return observations

    def _build_observation(self, obx) -> Dict[str, Any]:
        return {
            'set_id': self._safe_get(obx, 'set_id_obx'),
            'value_type': self._safe_get(obx, 'value_type'),
            'identifier': self._safe_get(obx, 'observation_identifier'),
            'sub_id': self._safe_get(obx, 'observation_sub_id'),
            'value': self._safe_get(obx, 'observation_value'),
            'units': self._safe_get(obx, 'units'),
            'reference_range': self._safe_get(obx, 'references_range'),
            'abnormal_flags': self._safe_get(obx, 'abnormal_flags'),
            'probability': self._safe_get(obx, 'probability'),
            'nature_of_abnormal_test': self._safe_get(
                obx, 'nature_of_abnormal_test'),
            'observation_status': self._safe_get(
                obx, 'observation_result_status'),
            'effective_date_last_obs_normal_values': self._safe_get(
                obx, 'effective_date_of_reference_range_values'),
            'user_defined_access_checks': self._safe_get(
                obx, 'user_defined_access_checks'),
            'observation_datetime': self._safe_get(
                obx, 'date_time_of_the_observation'),
            'producer_id': self._safe_get(obx, 'producer_id'),
            'responsible_observer': self._safe_get(
                obx, 'responsible_observer'),
            'observation_method': self._safe_get(
                obx, 'observation_method'),
            'equipment_instance_identifier': self._safe_get(
                obx, 'equipment_instance_identifier'),
            'analysis_datetime': self._safe_get(
                obx, 'date_time_of_the_analysis'),
        }

    def _extract_diagnoses(self,
                            parsed_msg) -> List[Dict[str, Any]]:
        """Extract DG1 (Diagnosis) segments."""
        diagnoses: List[Dict[str, Any]] = []
        try:
            if not hasattr(parsed_msg, 'dg1'):
                return diagnoses
            raw = parsed_msg.dg1
            dg1_list = raw if isinstance(raw, list) else [raw]
            for dg1 in dg1_list:
                try:
                    diagnoses.append({
                        'set_id': self._safe_get(
                            dg1, 'set_id_dg1'),
                        'coding_method': self._safe_get(
                            dg1, 'diagnosis_coding_method'),
                        'code': self._safe_get(
                            dg1, 'diagnosis_code_dg1'),
                        'description': self._safe_get(
                            dg1, 'diagnosis_description'),
                        'datetime': self._safe_get(
                            dg1, 'diagnosis_date_time'),
                        'type': self._safe_get(dg1, 'diagnosis_type'),
                        'drg_code': self._safe_get(dg1, 'major_diagnostic_category'),
                        'diagnosis_priority': self._safe_get(
                            dg1, 'diagnosis_priority'),
                        'diagnosing_clinician': self._safe_get(
                            dg1, 'diagnosing_clinician'),
                        'diagnosis_classification': self._safe_get(
                            dg1, 'diagnosis_classification'),
                        'confidential_indicator': self._safe_get(
                            dg1, 'confidential_indicator'),
                    })
                except Exception as e:
                    logger.error(f"Error extracting DG1 segment: {e}")
        except Exception as e:
            logger.error(f"Error extracting diagnoses: {e}")
        return diagnoses

    def _extract_specimens(self,
                            parsed_msg) -> List[Dict[str, Any]]:
        """Extract SPM (Specimen) segments (HL7 v2.5+)."""
        specimens: List[Dict[str, Any]] = []
        try:
            if not hasattr(parsed_msg, 'spm'):
                return specimens
            raw = parsed_msg.spm
            spm_list = raw if isinstance(raw, list) else [raw]
            for spm in spm_list:
                try:
                    specimens.append({
                        'set_id': self._safe_get(spm, 'set_id_spm'),
                        'specimen_id': self._safe_get(
                            spm, 'specimen_id'),
                        'parent_ids': self._safe_get(
                            spm, 'specimen_parent_ids'),
                        'type': self._safe_get(spm, 'specimen_type'),
                        'type_modifier': self._safe_get(
                            spm, 'specimen_type_modifier'),
                        'additives': self._safe_get(
                            spm, 'specimen_additives'),
                        'collection_method': self._safe_get(
                            spm, 'specimen_collection_method'),
                        'source_site': self._safe_get(
                            spm, 'specimen_source_site'),
                        'source_site_modifier': self._safe_get(
                            spm, 'specimen_source_site_modifier'),
                        'collection_site': self._safe_get(
                            spm, 'specimen_collection_site'),
                        'collection_amount': self._safe_get(
                            spm, 'specimen_collection_amount'),
                        'collection_datetime': self._safe_get(
                            spm, 'specimen_collection_date_time'),
                        'received_datetime': self._safe_get(
                            spm, 'specimen_received_date_time'),
                        'expiration_datetime': self._safe_get(
                            spm, 'specimen_expiration_date_time'),
                        'availability': self._safe_get(
                            spm, 'specimen_availability'),
                        'description': self._safe_get(
                            spm, 'specimen_description'),
                        'handling_code': self._safe_get(
                            spm, 'specimen_handling_code'),
                        'number_of_containers': self._safe_get(
                            spm, 'number_of_specimen_containers'),
                        'container_type': self._safe_get(
                            spm, 'container_type'),
                    })
                except Exception as e:
                    logger.error(f"Error extracting SPM segment: {e}")
        except Exception as e:
            logger.error(f"Error extracting specimens: {e}")
        return specimens

    def _extract_notes(self, parsed_msg) -> List[str]:
        notes: List[str] = []
        try:
            if not hasattr(parsed_msg, 'nte'):
                return notes
            raw = parsed_msg.nte
            nte_list = raw if isinstance(raw, list) else [raw]
            for nte in nte_list:
                try:
                    comment = self._safe_get(nte, 'comment')
                    if comment:
                        notes.append(comment)
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Error extracting notes: {e}")
        return notes

    # ------------------------------------------------------------------
    # Low-level field helpers
    # ------------------------------------------------------------------

    def _safe_get(self, segment, attr_name: str,
                   default: Optional[str] = None) -> Optional[str]:
        """
        Safely read a named attribute from an hl7apy segment object,
        returning *default* on any error or when the value is empty.
        """
        try:
            if segment is None or not hasattr(segment, attr_name):
                return default
            field = getattr(segment, attr_name)
            if field is None:
                return default
            if hasattr(field, 'value'):
                val = field.value
                if val is not None and str(val).strip():
                    return str(val).strip()
            field_str = str(field).strip()
            return field_str if field_str else default
        except Exception as e:
            logger.debug(f"Error extracting {attr_name}: {e}")
            return default

    # Keep backwards-compatible alias used in older callers
    def _safe_get_field(self, field, default=None):
        try:
            if field is None:
                return default
            if hasattr(field, 'value'):
                val = field.value
                if val is not None and str(val).strip():
                    return str(val).strip()
            field_str = str(field).strip()
            return field_str if field_str else default
        except Exception as e:
            logger.debug(f"Error extracting field value: {e}")
            return default

    def _parse_name(self,
                    name_str: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Parse an HL7 XPN name string (Family^Given^Middle^Suffix^Prefix^Degree).
        """
        if not name_str or not name_str.strip():
            return None
        parts = name_str.split('^')
        return {
            'family_name': parts[0].strip() if len(parts) > 0 and parts[0].strip() else None,
            'given_name':  parts[1].strip() if len(parts) > 1 and parts[1].strip() else None,
            'middle_name': parts[2].strip() if len(parts) > 2 and parts[2].strip() else None,
            'suffix':      parts[3].strip() if len(parts) > 3 and parts[3].strip() else None,
            'prefix':      parts[4].strip() if len(parts) > 4 and parts[4].strip() else None,
            'degree':      parts[5].strip() if len(parts) > 5 and parts[5].strip() else None,
        }

    # ------------------------------------------------------------------
    # Fallback / string-level parser
    # ------------------------------------------------------------------

    def _fallback_parse(self, hl7_message: str) -> Dict[str, Any]:
        """
        String-level parser for messages that hl7apy cannot handle.
        Extracts key fields from MSH, PID, ORC, OBR, and OBX segments.
        """
        logger.info("Using fallback parser for non-standard HL7 message")

        data: Dict[str, Any] = {
            'hl7_version': 'UNKNOWN',
            'timestamp': datetime.utcnow().isoformat(),
            'parsing_method': 'fallback',
            'raw_segments': [],
            'observations': [],
            'orders': [],
            'notes': [],
        }

        segments = [
            s.strip() for s in hl7_message.split('\r') if s.strip()
        ]

        for segment in segments:
            fields = segment.split('|')
            seg_type = fields[0].upper() if fields else 'UNKNOWN'

            data['raw_segments'].append({
                'type': seg_type,
                'fields': fields[1:],
            })

            try:
                if seg_type == 'MSH':
                    # MSH: |^~\&|SendApp|SendFac|RecvApp|RecvFac|DateTime||MsgType|CtrlID|ProcID|Version
                    data['hl7_version']        = self._fb_field(fields, 12)
                    data['message_type']       = {
                        'raw': self._fb_field(fields, 9),
                        'message_code':  (self._fb_field(fields, 9) or '').split('^')[0] or None,
                        'trigger_event': (self._fb_field(fields, 9) or '').split('^')[1]
                            if '^' in (self._fb_field(fields, 9) or '') else None,
                    }
                    data['message_control_id'] = self._fb_field(fields, 10)
                    data['processing_id']      = self._fb_field(fields, 11)
                    data['sending_application']= self._fb_field(fields, 3)
                    data['sending_facility']   = self._fb_field(fields, 4)
                    data['receiving_application'] = self._fb_field(fields, 5)
                    data['receiving_facility'] = self._fb_field(fields, 6)
                    data['message_datetime']   = self._fb_field(fields, 7)

                elif seg_type == 'PID':
                    name_raw = self._fb_field(fields, 5)
                    data['patient'] = {
                        'id': self._fb_field(fields, 3),
                        'identifier_list': self._fb_field(fields, 3),
                        'name': self._parse_name(name_raw),
                        'date_of_birth': self._fb_field(fields, 7),
                        'sex': self._fb_field(fields, 8),
                        'race': self._fb_field(fields, 10),
                        'address': self._fb_field(fields, 11),
                        'phone_home': self._fb_field(fields, 13),
                        'phone_business': self._fb_field(fields, 14),
                        'primary_language': self._fb_field(fields, 15),
                        'marital_status': self._fb_field(fields, 16),
                        'account_number': self._fb_field(fields, 18),
                        'ssn': self._fb_field(fields, 19),
                        'ethnic_group': self._fb_field(fields, 22),
                    }

                elif seg_type == 'ORC':
                    order = data.get('_current_order', {})
                    order['order_control']      = self._fb_field(fields, 1)
                    order['placer_order_number']= self._fb_field(fields, 2)
                    order['filler_order_number']= self._fb_field(fields, 3)
                    order['order_status']       = self._fb_field(fields, 5)
                    order['ordering_provider']  = self._fb_field(fields, 12)
                    data['_current_order'] = order

                elif seg_type == 'OBR':
                    order = data.get('_current_order', {})
                    order['set_id']               = self._fb_field(fields, 1)
                    order['placer_order_number']  = order.get('placer_order_number') or self._fb_field(fields, 2)
                    order['filler_order_number']  = order.get('filler_order_number') or self._fb_field(fields, 3)
                    order['universal_service_id'] = self._fb_field(fields, 4)
                    order['observation_datetime'] = self._fb_field(fields, 7)
                    order['specimen_received_datetime'] = self._fb_field(fields, 14)
                    order['ordering_provider']    = order.get('ordering_provider') or self._fb_field(fields, 16)
                    order['result_status']        = self._fb_field(fields, 25)
                    data['orders'].append(order)
                    data['_current_order'] = {}  # reset for next OBR

                elif seg_type == 'OBX':
                    observation = {
                        'set_id':             self._fb_field(fields, 1),
                        'value_type':         self._fb_field(fields, 2),
                        'identifier':         self._fb_field(fields, 3),
                        'sub_id':             self._fb_field(fields, 4),
                        'value':              self._fb_field(fields, 5),
                        'units':              self._fb_field(fields, 6),
                        'reference_range':    self._fb_field(fields, 7),
                        'abnormal_flags':     self._fb_field(fields, 8),
                        'probability':        self._fb_field(fields, 9),
                        'nature_of_abnormal_test': self._fb_field(fields, 10),
                        'observation_status': self._fb_field(fields, 11),
                        'observation_datetime': self._fb_field(fields, 14),
                        'producer_id':        self._fb_field(fields, 15),
                        'responsible_observer': self._fb_field(fields, 16),
                    }
                    data['observations'].append(observation)

                elif seg_type == 'NTE':
                    note = self._fb_field(fields, 3)
                    if note:
                        data['notes'].append(note)

                elif seg_type == 'DG1':
                    diag = {
                        'set_id':      self._fb_field(fields, 1),
                        'coding_method': self._fb_field(fields, 2),
                        'code':        self._fb_field(fields, 3),
                        'description': self._fb_field(fields, 4),
                        'datetime':    self._fb_field(fields, 5),
                        'type':        self._fb_field(fields, 6),
                    }
                    data.setdefault('diagnoses', []).append(diag)

                elif seg_type == 'SPM':
                    specimen = {
                        'set_id':       self._fb_field(fields, 1),
                        'specimen_id':  self._fb_field(fields, 2),
                        'type':         self._fb_field(fields, 4),
                        'source_site':  self._fb_field(fields, 8),
                        'collection_datetime': self._fb_field(fields, 17),
                        'received_datetime':   self._fb_field(fields, 18),
                    }
                    data.setdefault('specimens', []).append(specimen)

            except Exception as e:
                logger.error(
                    f"Fallback parser error on {seg_type}: {e}")
                continue

        # Remove internal working key
        data.pop('_current_order', None)

        # Backwards-compatible convenience key
        if data['orders']:
            data['order'] = data['orders'][0]

        return data

    @staticmethod
    def _fb_field(fields: List[str], index: int,
                  default: Optional[str] = None) -> Optional[str]:
        """Return fields[index] stripped, or default if absent/empty."""
        try:
            val = fields[index].strip()
            return val if val else default
        except IndexError:
            return default


__all__ = ["HL7Parser"]
