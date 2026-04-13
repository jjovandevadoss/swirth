import logging
import threading
import time
from typing import Any, Dict, Optional

from api_client import APIClient
from storage import MessageRepository

logger = logging.getLogger(__name__)


class DeliveryService:
    def __init__(
        self,
        repository: MessageRepository,
        api_client: APIClient,
        max_attempts: int = 5,
        poll_interval_seconds: int = 10,
    ):
        self.repository = repository
        self.api_client = api_client
        self.max_attempts = max_attempts
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            logger.debug("Delivery retry worker already running — skipping start")
            return
        self._worker = threading.Thread(target=self._run_worker, daemon=True)
        self._worker.start()
        logger.info(
            "Delivery retry worker started  max_attempts=%d  poll_interval=%ds",
            self.max_attempts,
            self.poll_interval_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._worker and self._worker.is_alive():
            self._worker.join(timeout=2)

    def deliver_message(self, message_uid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        logger.debug("[Delivery] Attempting outbound delivery  uid=%s", message_uid)
        try:
            response = self.api_client.send_data(payload, retry_count=1)
            self.repository.mark_delivered(message_uid, response.status_code)
            logger.info(
                "[Delivery] Delivered  uid=%s  http=%d",
                message_uid,
                response.status_code,
            )
            return {
                "status": "delivered",
                "status_code": response.status_code,
                "response": self._response_to_json(response),
            }
        except Exception as exc:
            error_text = str(exc)
            self.repository.mark_failed_attempt(message_uid, error_text)
            logger.warning(
                "[Delivery] Failed — queued for retry  uid=%s  error=%s",
                message_uid,
                error_text,
            )
            return {
                "status": "queued_retry",
                "error": error_text,
            }

    def _run_worker(self) -> None:
        logger.debug("[Delivery] Retry worker polling loop started")
        while not self._stop_event.is_set():
            try:
                pending = self.repository.get_pending_retries(max_attempts=self.max_attempts, limit=50)
                if pending:
                    logger.info("[Delivery] Retry worker processing %d pending message(s)", len(pending))
                for entry in pending:
                    if self._stop_event.is_set():
                        break
                    uid = entry["message_uid"]
                    logger.debug("[Delivery] Retrying uid=%s", uid)
                    self.deliver_message(uid, entry.get("parsed_data") or {})
            except Exception as exc:
                logger.error("[Delivery] Retry worker loop error: %s", exc, exc_info=True)
            self._stop_event.wait(self.poll_interval_seconds)

    @staticmethod
    def _response_to_json(response) -> Any:
        if response.headers.get("content-type", "").startswith("application/json"):
            try:
                return response.json()
            except Exception:
                return response.text
        return response.text
