import logging
from datetime import UTC, datetime
from typing import Any, Dict

from parsers import ASTMParser, HL7Parser
from services.delivery_service import DeliveryService
from storage import MessageRepository

logger = logging.getLogger(__name__)


class IngestService:
    def __init__(
        self,
        repository: MessageRepository,
        delivery_service: DeliveryService,
        hl7_parser: HL7Parser,
        astm_parser: ASTMParser,
    ):
        self.repository = repository
        self.delivery_service = delivery_service
        self.hl7_parser = hl7_parser
        self.astm_parser = astm_parser
        logger.info(
            "IngestService initialised  HL7parser=%s  ASTMparser=%s",
            type(hl7_parser).__name__,
            type(astm_parser).__name__,
        )

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def process_hl7(self, raw_message: str, source_ip: str) -> Dict[str, Any]:
        logger.info(
            "[HL7] Received message from %s  len=%d chars",
            source_ip,
            len(raw_message),
        )
        logger.debug("[HL7] Raw content:\n%s", raw_message[:2000])

        try:
            parsed = self.hl7_parser.parse(raw_message)
        except Exception as exc:
            logger.error("[HL7] Parser raised an exception from %s: %s", source_ip, exc, exc_info=True)
            parsed = {"protocol": "HL7", "error": str(exc), "raw_message": raw_message}

        if parsed.get("error"):
            logger.warning("[HL7] Parser returned error from %s: %s", source_ip, parsed["error"])
        else:
            logger.info(
                "[HL7] Parsed OK from %s  patient=%s  observations=%d",
                source_ip,
                _safe_patient_label(parsed),
                len(parsed.get("observations") or []),
            )

        return self._process("HL7", raw_message, parsed, source_ip)

    def process_astm(self, raw_message: str, source_ip: str) -> Dict[str, Any]:
        logger.info(
            "[ASTM] Received message from %s  len=%d chars  lines=%d",
            source_ip,
            len(raw_message),
            raw_message.count("\n") + 1,
        )
        logger.debug("[ASTM] Raw content:\n%s", raw_message[:2000])

        try:
            parsed = self.astm_parser.parse(raw_message)
        except Exception as exc:
            logger.error("[ASTM] Parser raised an exception from %s: %s", source_ip, exc, exc_info=True)
            parsed = {"protocol": "ASTM", "error": str(exc), "raw_message": raw_message}

        if parsed.get("error"):
            logger.warning("[ASTM] Parser returned error from %s: %s", source_ip, parsed["error"])
        else:
            parser_name = parsed.get("parser_name", type(self.astm_parser).__name__)
            instrument = parsed.get("instrument") or {}
            logger.info(
                "[ASTM] Parsed OK via %s from %s  instrument=%s  orders=%d  results=%d  patient=%s",
                parser_name,
                source_ip,
                instrument.get("display_name") or "unknown",
                len(parsed.get("orders") or []),
                len(parsed.get("results") or []),
                _safe_patient_label(parsed),
            )

        return self._process("ASTM", raw_message, parsed, source_ip)

    # ------------------------------------------------------------------
    # Internal pipeline
    # ------------------------------------------------------------------

    def _process(
        self,
        protocol: str,
        raw_message: str,
        parsed_data: Dict[str, Any],
        source_ip: str,
    ) -> Dict[str, Any]:
        uid = f"{protocol}-{int(datetime.now(UTC).timestamp() * 1000)}"
        logger.debug("[%s] Assigning uid=%s for message from %s", protocol, uid, source_ip)

        try:
            message_entry = self.repository.create_message(
                message_uid=uid,
                protocol=protocol,
                source_ip=source_ip,
                raw_message=raw_message,
                parsed_data=parsed_data,
            )
            logger.debug("[%s] Stored in DB  uid=%s", protocol, uid)
        except Exception as exc:
            logger.error(
                "[%s] DB write failed for uid=%s from %s: %s",
                protocol, uid, source_ip, exc,
                exc_info=True,
            )
            raise

        try:
            delivery = self.delivery_service.deliver_message(uid, parsed_data)
            logger.info(
                "[%s] Delivery result for uid=%s: status=%s",
                protocol, uid, delivery.get("status"),
            )
            if delivery.get("status") == "queued_retry":
                logger.warning(
                    "[%s] Delivery queued for retry  uid=%s  error=%s",
                    protocol, uid, delivery.get("error"),
                )
        except Exception as exc:
            logger.error(
                "[%s] Delivery call raised exception for uid=%s: %s",
                protocol, uid, exc,
                exc_info=True,
            )
            delivery = {"status": "error", "error": str(exc)}

        refreshed = self.repository.get_message(uid) or message_entry

        return {
            "message_uid": uid,
            "protocol": protocol,
            "parsed_data": parsed_data,
            "delivery": delivery,
            "stored": refreshed,
        }


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _safe_patient_label(parsed: Dict[str, Any]) -> str:
    """Return a short human-readable patient label for log lines (never raises)."""
    try:
        patient = parsed.get("patient") or {}
        name = patient.get("name") or {}
        if isinstance(name, dict):
            parts = [name.get("last"), name.get("first")]
            label = " ".join(p for p in parts if p)
            if label:
                return label
        pid = (
            patient.get("practice_patient_id")
            or patient.get("lab_patient_id")
            or patient.get("id")
        )
        if pid:
            return str(pid)
        # ASTM — try accession from first order
        for order in parsed.get("orders") or []:
            sid = order.get("specimen_id_parsed") or {}
            acc = sid.get("accession_number") or order.get("specimen_id")
            if acc:
                return f"accession={acc}"
    except Exception:
        pass
    return "unknown"
