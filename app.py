"""
Flask application for receiving HL7/ASTM messages,
parsing them, persisting them, and forwarding to an external API.
"""

import logging
import os
import sys
import collections
import socket
import threading as _threading

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from api_client import APIClient
from config import Config
from parsers import ASTMParser, HL7Parser
from routes import create_ingest_blueprint, create_results_blueprint
from routes.mapping_routes import create_mapping_blueprint
from services import DeliveryService, IngestService
from services.mapping_service import MappingService
from storage import MessageRepository
from storage.mapping_repository import MappingRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory log buffer — last 400 records, exposed via /api/logs
# ---------------------------------------------------------------------------
_log_buffer: collections.deque = collections.deque(maxlen=400)
_log_lock = _threading.Lock()


class _MemoryHandler(logging.Handler):
    _FMT = logging.Formatter(
        '%(asctime)s  %(levelname)-8s  %(name)s  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    def emit(self, record):
        try:
            msg = self._FMT.format(record)
        except Exception:
            msg = record.getMessage()
        entry = {
            'ts': record.created,
            'level': record.levelname,
            'name': record.name,
            'msg': msg,
        }
        with _log_lock:
            _log_buffer.append(entry)


logging.getLogger().addHandler(_MemoryHandler())


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object(Config)

    logger.info('=' * 60)
    logger.info('Swirth Lab Interface Gateway starting...')
    logger.info(f'Database: {app.config["DB_PATH"]}')
    logger.info(f'API destination: {app.config["API_URL"]}')
    logger.info('=' * 60)

    repository = MessageRepository(app.config['DB_PATH'])
    mapping_repository = MappingRepository(app.config['DB_PATH'])
    mapping_service = MappingService(mapping_repository)
    
    hl7_parser = HL7Parser()
    astm_parser = ASTMParser()

    api_client = APIClient(
        api_url=app.config['API_URL'],
        api_key=app.config['API_KEY'],
        timeout=app.config['API_TIMEOUT'],
        mapping_service=mapping_service
    )

    delivery_service = DeliveryService(
        repository=repository,
        api_client=api_client,
        max_attempts=app.config['DELIVERY_MAX_ATTEMPTS'],
        poll_interval_seconds=app.config['DELIVERY_POLL_INTERVAL']
    )
    delivery_service.start()
    logger.info(f'Delivery service started: max_attempts={app.config["DELIVERY_MAX_ATTEMPTS"]}, poll_interval={app.config["DELIVERY_POLL_INTERVAL"]}s')

    ingest_service = IngestService(
        repository=repository,
        delivery_service=delivery_service,
        hl7_parser=hl7_parser,
        astm_parser=astm_parser,
    )

    app.extensions['repository'] = repository
    app.extensions['delivery_service'] = delivery_service
    app.extensions['mapping_service'] = mapping_service

    app.register_blueprint(create_ingest_blueprint(ingest_service))
    app.register_blueprint(create_results_blueprint(repository))
    app.register_blueprint(create_mapping_blueprint(mapping_service))

    @app.route('/')
    def dashboard():
        return render_template(
            'index.html',
            http_port=app.config['PORT'],
            mllp_port=app.config.get('MLLP_PORT', 6000)
        )

    @app.route('/settings')
    def settings():
        return render_template('settings.html')

    @app.route('/mappings')
    def mappings():
        return render_template('mappings.html')

    @app.route('/api/config', methods=['GET'])
    def get_config():
        config = {
            'HOST': app.config['HOST'],
            'PORT': app.config['PORT'],
            'MLLP_HOST': app.config.get('MLLP_HOST', '0.0.0.0'),
            'MLLP_PORT': app.config.get('MLLP_PORT', 6000),
            'ASTM_HOST': app.config.get('ASTM_HOST', '0.0.0.0'),
            'ASTM_PORT': app.config.get('ASTM_PORT', 7000),
            'API_URL': app.config['API_URL'],
            'API_KEY': app.config['API_KEY'],
            'API_TIMEOUT': app.config['API_TIMEOUT'],
            'DEBUG': str(app.config['DEBUG']),
            'LOG_LEVEL': app.config['LOG_LEVEL'],
            'DB_PATH': app.config['DB_PATH'],
            'DELIVERY_MAX_ATTEMPTS': app.config['DELIVERY_MAX_ATTEMPTS'],
            'DELIVERY_POLL_INTERVAL': app.config['DELIVERY_POLL_INTERVAL'],
        }
        return jsonify(config)

    @app.route('/api/config', methods=['POST'])
    def update_config():
        try:
            data = request.get_json() or {}

            env_path = os.path.join(os.path.dirname(__file__), '.env')
            env_content = []
            if os.path.exists(env_path):
                with open(env_path, 'r') as env_file:
                    env_content = env_file.readlines()

            new_host = data.get('HOST', app.config['HOST'])
            new_port = int(data.get('PORT', app.config['PORT']))
            new_mllp_host = data.get('MLLP_HOST', app.config.get('MLLP_HOST', '0.0.0.0'))
            new_mllp_port = int(data.get('MLLP_PORT', app.config.get('MLLP_PORT', 6000)))
            new_astm_host = data.get('ASTM_HOST', app.config.get('ASTM_HOST', '0.0.0.0'))
            new_astm_port = int(data.get('ASTM_PORT', app.config.get('ASTM_PORT', 7000)))

            updates = {
                'HOST': new_host,
                'PORT': str(new_port),
                'MLLP_HOST': new_mllp_host,
                'MLLP_PORT': str(new_mllp_port),
                'ASTM_HOST': data.get('ASTM_HOST', app.config.get('ASTM_HOST', '0.0.0.0')),
                'ASTM_PORT': str(data.get('ASTM_PORT', app.config.get('ASTM_PORT', 7000))),
                'API_URL': data.get('API_URL'),
                'API_KEY': data.get('API_KEY', ''),
                'API_TIMEOUT': str(data.get('API_TIMEOUT')),
                'DEBUG': data.get('DEBUG'),
                'LOG_LEVEL': data.get('LOG_LEVEL'),
                'DB_PATH': data.get('DB_PATH', app.config.get('DB_PATH', 'data/messages.db')),
                'DELIVERY_MAX_ATTEMPTS': str(data.get('DELIVERY_MAX_ATTEMPTS', app.config.get('DELIVERY_MAX_ATTEMPTS', 5))),
                'DELIVERY_POLL_INTERVAL': str(data.get('DELIVERY_POLL_INTERVAL', app.config.get('DELIVERY_POLL_INTERVAL', 10))),
            }

            for key, value in updates.items():
                if value is None:
                    continue
                found = False
                for index, line in enumerate(env_content):
                    if line.strip().startswith(f'{key}='):
                        env_content[index] = f'{key}={value}\n'
                        found = True
                        break
                if not found:
                    env_content.append(f'{key}={value}\n')

            with open(env_path, 'w') as env_file:
                env_file.writelines(env_content)

            load_dotenv(override=True)

            app.config['API_URL'] = data.get('API_URL', app.config['API_URL'])
            app.config['API_KEY'] = data.get('API_KEY', app.config['API_KEY'])
            app.config['API_TIMEOUT'] = int(data.get('API_TIMEOUT', app.config['API_TIMEOUT']))

            app.extensions['delivery_service'].api_client = APIClient(
                api_url=app.config['API_URL'],
                api_key=app.config['API_KEY'],
                timeout=app.config['API_TIMEOUT'],
                mapping_service=app.extensions['mapping_service']
            )

            logger.info('Configuration updated and API client hot-reloaded')
            return jsonify({
                'status': 'success',
                'message': 'Configuration saved and applied',
                'requires_restart': (
                    new_port != app.config['PORT']
                    or new_host != app.config['HOST']
                    or new_mllp_port != app.config.get('MLLP_PORT')
                    or new_mllp_host != app.config.get('MLLP_HOST')
                    or new_astm_port != app.config.get('ASTM_PORT')
                    or new_astm_host != app.config.get('ASTM_HOST')
                )
            })

        except Exception as exc:
            logger.error(f'Failed to update configuration: {str(exc)}')
            return jsonify({'status': 'error', 'message': str(exc)}), 500

    @app.route('/api/restart', methods=['POST'])
    def restart_app():
        try:
            from threading import Thread
            import time

            logger.info('Application restart requested')

            def restart():
                time.sleep(1)
                os.execv(sys.executable, ['python'] + sys.argv)

            Thread(target=restart).start()
            return jsonify({'status': 'success', 'message': 'Application restarting...'})

        except Exception as exc:
            logger.error(f'Failed to restart: {str(exc)}')
            return jsonify({'status': 'error', 'message': str(exc)}), 500

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'status': 'error', 'message': 'Endpoint not found'}), 404

    @app.route('/api/logs', methods=['GET'])
    def get_logs():
        try:
            since = float(request.args.get('since', 0))
        except (TypeError, ValueError):
            since = 0.0
        with _log_lock:
            entries = [e for e in _log_buffer if e['ts'] > since]
        return jsonify({'logs': entries})

    @app.route('/api/listener-status', methods=['GET'])
    def listener_status():
        def _check(port):
            try:
                with socket.create_connection(('127.0.0.1', int(port)), timeout=0.5):
                    return True
            except OSError:
                return False
        return jsonify({
            'http': {'port': app.config.get('PORT', 5001), 'listening': True},
            'mllp': {
                'port': app.config.get('MLLP_PORT', 6000),
                'listening': _check(app.config.get('MLLP_PORT', 6000)),
            },
            'astm': {
                'port': app.config.get('ASTM_PORT', 7000),
                'listening': _check(app.config.get('ASTM_PORT', 7000)),
            },
        })

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f'Internal server error: {str(error)}')
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

    return app


app = create_app()


if __name__ == '__main__':
    logger.info('=' * 60)
    logger.info('Starting Flask HTTP server...')
    logger.info(f'HTTP bound to {app.config["HOST"]}:{app.config["PORT"]}')
    logger.info(f'Expected MLLP listener: {app.config.get("MLLP_HOST","0.0.0.0")}:{app.config.get("MLLP_PORT",6000)}')
    logger.info(f'Expected ASTM listener: {app.config.get("ASTM_HOST","0.0.0.0")}:{app.config.get("ASTM_PORT",7000)}')
    logger.info('=' * 60)
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        debug=app.config['DEBUG']
    )
