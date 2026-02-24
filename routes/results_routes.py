from datetime import datetime

from flask import Blueprint, jsonify


def create_results_blueprint(repository):
    results_bp = Blueprint("results", __name__)

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

    return results_bp
