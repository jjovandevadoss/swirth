from datetime import datetime
from typing import Any, Dict

from parsers import ASTMParser, HL7Parser
from services.delivery_service import DeliveryService
from storage import MessageRepository


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

    def process_hl7(self, raw_message: str, source_ip: str) -> Dict[str, Any]:
        parsed = self.hl7_parser.parse(raw_message)
        return self._process("HL7", raw_message, parsed, source_ip)

    def process_astm(self, raw_message: str, source_ip: str) -> Dict[str, Any]:
        parsed = self.astm_parser.parse(raw_message)
        return self._process("ASTM", raw_message, parsed, source_ip)

    def _process(self, protocol: str, raw_message: str, parsed_data: Dict[str, Any], source_ip: str) -> Dict[str, Any]:
        uid = f"{protocol}-{int(datetime.utcnow().timestamp() * 1000)}"
        message_entry = self.repository.create_message(
            message_uid=uid,
            protocol=protocol,
            source_ip=source_ip,
            raw_message=raw_message,
            parsed_data=parsed_data,
        )

        delivery = self.delivery_service.deliver_message(uid, parsed_data)
        refreshed = self.repository.get_message(uid) or message_entry

        return {
            "message_uid": uid,
            "protocol": protocol,
            "parsed_data": parsed_data,
            "delivery": delivery,
            "stored": refreshed,
        }
