from datetime import datetime

from flask import Blueprint, jsonify

from parsers import ASTMFormatter


def create_results_blueprint(repository):
    results_bp = Blueprint("results", __name__)
    astm_formatter = ASTMFormatter()

    @results_bp.route("/health", methods=["GET"])
    def health_check():
        return (
            jsonify(
                {
                    "status": "healthy",
                    "timestamp": datetime.utcnow().isoformat(),
                    "service": "HL7 Lab Machine Interface",
                }
            ),
            200,
        )

    @results_bp.route("/api/history", methods=["GET"])
    def get_history():
        return jsonify(repository.get_recent_messages(limit=100))

    @results_bp.route("/results/hl7/latest", methods=["GET"])
    def latest_hl7():
        result = repository.get_latest_by_protocol("HL7")
        if not result:
            return jsonify({"status": "not_found", "message": "No HL7 results found"}), 404
        return jsonify({"status": "success", "result": result}), 200

    @results_bp.route("/results/astm/latest", methods=["GET"])
    def latest_astm():
        result = repository.get_latest_by_protocol("ASTM")
        if not result:
            return jsonify({"status": "not_found", "message": "No ASTM results found"}), 404
        return jsonify({"status": "success", "result": result}), 200

    @results_bp.route("/api/messages/<message_uid>/formatted", methods=["GET"])
    def get_formatted_message(message_uid):
        """Get formatted HTML report for a message."""
        message = repository.get_message(message_uid)
        if not message:
            return jsonify({"status": "not_found", "message": "Message not found"}), 404
        
        # Only format ASTM messages for now
        if message.get('protocol') != 'ASTM':
            return jsonify({
                "status": "unsupported",
                "message": "Formatted reports are only available for ASTM messages"
            }), 400
        
        parsed_data = message.get('parsed_data')
        if not parsed_data:
            return jsonify({
                "status": "error",
                "message": "Message has no parsed data"
            }), 400
        
        try:
            html_content = astm_formatter.format_as_html(parsed_data)
            return jsonify({
                "status": "success",
                "html": html_content,
                "message_uid": message_uid
            }), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Failed to format message: {str(e)}"
            }), 500

    return results_bp
