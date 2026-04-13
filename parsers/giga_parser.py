"""
Dynamic Giga parser for analyzer ASTM/LIS2-A2 payloads.

This parser wraps the existing ASTM parser and enriches its output with
instrument-aware metadata and a raw record view so mappings can be saved per
instrument/model and users can choose exactly which fields to keep.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .astm_parser import ASTMParser

logger = logging.getLogger(__name__)


class GigaParser(ASTMParser):
    """Instrument-aware parser for Giga-style ASTM messages."""

    def parse(self, astm_message: str) -> Dict[str, Any]:
        logger.debug(
            "[GigaParser] Starting parse  input_len=%d chars",
            len(astm_message),
        )

        parsed = super().parse(astm_message)
        if not isinstance(parsed, dict):
            logger.error(
                "[GigaParser] Base parser returned unexpected type: %s", type(parsed)
            )
            return {
                "protocol": "ASTM",
                "parser_name": "GIGA",
                "error": "Parser returned an unexpected payload",
                "raw_message": astm_message,
            }

        parsed["parser_name"] = "GIGA"

        if parsed.get("error"):
            logger.warning(
                "[GigaParser] Base parser reported an error: %s", parsed["error"]
            )
            return parsed

        instrument = self._extract_instrument_metadata(parsed.get("header") or {})
        if instrument:
            parsed["instrument"] = instrument
            logger.debug(
                "[GigaParser] Instrument identified: model=%s  serial=%s  firmware=%s",
                instrument.get("model"),
                instrument.get("serial"),
                instrument.get("firmware"),
            )
        else:
            logger.debug("[GigaParser] No instrument metadata found in H record")

        profiles: List[str] = []
        for order in parsed.get("orders", []):
            test_profile = self._extract_test_profile(order.get("universal_test_id") or {})
            if test_profile:
                order["test_profile"] = test_profile
                profiles.append(test_profile)

        if profiles:
            parsed["message_profile"] = profiles[0]
            logger.debug(
                "[GigaParser] Test profiles found: %s", profiles
            )
        else:
            logger.debug("[GigaParser] No test profile extracted from orders")

        raw_records = self._build_raw_records(astm_message)
        parsed["raw_records"] = raw_records
        parsed["routing_hints"] = {
            "protocol": parsed.get("protocol", "ASTM"),
            "instrument_model": instrument.get("model") if instrument else None,
            "instrument_serial": instrument.get("serial") if instrument else None,
            "test_profile": parsed.get("message_profile"),
        }

        logger.info(
            "[GigaParser] Parse complete  instrument=%s  orders=%d  results=%d  "
            "raw_records=%d  profile=%s",
            (instrument or {}).get("display_name") or "unknown",
            len(parsed.get("orders") or []),
            len(parsed.get("results") or []),
            len(raw_records),
            parsed.get("message_profile") or "none",
        )

        return parsed

    def _extract_instrument_metadata(self, header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        sender_name = (header.get("sender_name") or "").strip()
        if not sender_name:
            return None

        parts = sender_name.split("^")
        model = parts[0].strip() if len(parts) > 0 and parts[0] else None
        serial = parts[1].strip() if len(parts) > 1 and parts[1] else None
        firmware = parts[2].strip() if len(parts) > 2 and parts[2] else None

        return {
            "raw": sender_name,
            "model": model,
            "serial": serial,
            "firmware": firmware,
            "display_name": " ".join(part for part in [model, serial] if part) or sender_name,
        }

    def _extract_test_profile(self, universal_test_id: Dict[str, Any]) -> Optional[str]:
        for key in ("mnemonic", "manufacturer", "test_name", "test_code", "test_id"):
            value = universal_test_id.get(key)
            if value:
                return str(value).strip()
        return None

    def _build_raw_records(self, astm_message: str) -> List[Dict[str, Any]]:
        cleaned = self._strip_framing(astm_message)
        records = self._split_records(cleaned)
        field_sep, component_sep, repeat_sep, escape_char = self._detect_delimiters(records)

        raw_records: List[Dict[str, Any]] = []
        for record in records:
            record_type = record[0].upper() if record else "?"
            fields = record.split(field_sep)
            raw_records.append(
                {
                    "record_type": record_type,
                    "sequence": fields[1] if len(fields) > 1 and fields[1] else None,
                    "raw": record,
                    "field_separator": field_sep,
                    "component_separator": component_sep,
                    "repeat_separator": repeat_sep,
                    "escape_character": escape_char,
                    "fields": [
                        {
                            "position": idx,
                            "value": value,
                            **(
                                {
                                    "components": [
                                        {
                                            "position": cidx,
                                            "value": c.strip() if c.strip() else None,
                                        }
                                        for cidx, c in enumerate(value.split(component_sep))
                                    ]
                                }
                                if component_sep in value else {}
                            ),
                        }
                        for idx, value in enumerate(fields[1:], start=1)
                    ],
                }
            )

        return raw_records


__all__ = ["GigaParser"]
