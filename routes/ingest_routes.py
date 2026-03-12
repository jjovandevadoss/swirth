from flask import Blueprint, jsonify, request
import logging
import time

logger = logging.getLogger(__name__)


def create_ingest_blueprint(ingest_service):
    ingest_bp = Blueprint("ingest", __name__)

    @ingest_bp.route("/hl7/receive", methods=["POST"])
    def receive_hl7():
        start_time = time.time()
        source_ip = request.headers.get("X-Original-Source-IP", request.remote_addr)
        content_type = request.headers.get("Content-Type", "unknown")
        content_length = request.headers.get("Content-Length", "0")
        
        logger.info(f"[HTTP] POST /hl7/receive from {source_ip} | {content_type} | {content_length} bytes")

        if request.is_json:
            data = request.get_json(silent=True) or {}
            hl7_message = data.get("message", "")
            logger.debug(f"[HTTP] Received JSON payload with 'message' key")
        elif request.form:
            hl7_message = request.form.get("message", "")
            logger.debug(f"[HTTP] Received form-encoded payload")
        else:
            hl7_message = request.data.decode("utf-8", errors="ignore")
            logger.debug(f"[HTTP] Received raw text body")

        if not hl7_message:
            logger.warning(f"[HTTP] Empty HL7 message from {source_ip}")
            return jsonify({"status": "error", "message": "No HL7 message provided"}), 400

        logger.info(f"[HTTP] Extracted {len(hl7_message)} char HL7 message from {source_ip}")
        
        try:
            result = ingest_service.process_hl7(hl7_message, source_ip)
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(f"[HTTP] HL7 parse error from {source_ip} after {duration:.3f}s: {str(exc)}")
            return jsonify({"status": "error", "message": f"Failed to parse HL7 message: {str(exc)}"}), 400

        duration = time.time() - start_time
        if result["delivery"]["status"] == "delivered":
            logger.info(f"[HTTP] 200 OK to {source_ip} in {duration:.3f}s | message processed and delivered")
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "HL7 message processed and forwarded",
                        "message_uid": result["message_uid"],
                        "parsed_data": result["parsed_data"],
                        "api_response": {
                            "status_code": result["delivery"].get("status_code"),
                            "response": result["delivery"].get("response"),
                        },
                    }
                ),
                200,
            )

        logger.warning(f"[HTTP] 207 Multi-Status to {source_ip} in {duration:.3f}s | parsed OK but delivery failed (queued for retry)")
        return (
            jsonify(
                {
                    "status": "partial_success",
                    "message": "HL7 parsed; outbound failed and queued for retry",
                    "message_uid": result["message_uid"],
                    "parsed_data": result["parsed_data"],
                    "error": result["delivery"].get("error"),
                }
            ),
            207,
        )

    @ingest_bp.route("/astm/receive", methods=["POST"])
    def receive_astm():
        start_time = time.time()
        source_ip = request.headers.get("X-Original-Source-IP", request.remote_addr)
        content_length = request.headers.get("Content-Length", "0")
        
        logger.info(f"[HTTP] POST /astm/receive from {source_ip} | {content_length} bytes")
        
        astm_message = request.data.decode("utf-8", errors="ignore")

        if not astm_message:
            logger.warning(f"[HTTP] Empty ASTM message from {source_ip}")
            return jsonify({"status": "error", "message": "No ASTM message provided"}), 400

        logger.info(f"[HTTP] Extracted {len(astm_message)} char ASTM message from {source_ip}")
        
        try:
            result = ingest_service.process_astm(astm_message, source_ip)
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(f"[HTTP] ASTM parse error from {source_ip} after {duration:.3f}s: {str(exc)}")
            return jsonify({"status": "error", "message": f"Failed to parse ASTM message: {str(exc)}"}), 400

        duration = time.time() - start_time
        if result["delivery"]["status"] == "delivered":
            logger.info(f"[HTTP] 200 OK to {source_ip} in {duration:.3f}s | message processed and delivered")
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "ASTM message processed and forwarded",
                        "message_uid": result["message_uid"],
                        "parsed_data": result["parsed_data"],
                        "api_response": {
                            "status_code": result["delivery"].get("status_code"),
                            "response": result["delivery"].get("response"),
                        },
                    }
                ),
                200,
            )

        logger.warning(f"[HTTP] 207 Multi-Status to {source_ip} in {duration:.3f}s | parsed OK but delivery failed (queued for retry)")
        return (
            jsonify(
                {
                    "status": "partial_success",
                    "message": "ASTM parsed; outbound failed and queued for retry",
                    "message_uid": result["message_uid"],
                    "parsed_data": result["parsed_data"],
                    "error": result["delivery"].get("error"),
                }
            ),
            207,
        )

    @ingest_bp.route("/hl7/batch", methods=["POST"])
    def receive_hl7_batch():
        if not request.is_json:
            return jsonify({"status": "error", "message": "Content-Type must be application/json for batch processing"}), 400

        data = request.get_json(silent=True) or {}
        messages = data.get("messages", [])

        if not messages:
            return jsonify({"status": "error", "message": "No messages provided in batch"}), 400

        results = []
        success_count = 0
        failure_count = 0

        for idx, hl7_message in enumerate(messages):
            try:
                result = ingest_service.process_hl7(hl7_message, request.remote_addr)
                delivery_status = result["delivery"]["status"]
                if delivery_status == "delivered":
                    success_count += 1
                    results.append(
                        {
                            "index": idx,
                            "status": "success",
                            "message_uid": result["message_uid"],
                            "message_type": result["parsed_data"].get("message_type"),
                            "api_status_code": result["delivery"].get("status_code"),
                        }
                    )
                else:
                    failure_count += 1
                    results.append(
                        {
                            "index": idx,
                            "status": "queued_retry",
                            "message_uid": result["message_uid"],
                            "error": result["delivery"].get("error"),
                        }
                    )
            except Exception as exc:
                failure_count += 1
                results.append({"index": idx, "status": "error", "error": str(exc)})

        return (
            jsonify(
                {
                    "status": "batch_complete",
                    "total": len(messages),
                    "successful": success_count,
                    "failed": failure_count,
                    "results": results,
                }
            ),
            200,
        )

    return ingest_bp
