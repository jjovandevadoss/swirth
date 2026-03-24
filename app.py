"""
Flask application for receiving HL7/ASTM messages,
parsing them, persisting them, and forwarding to an external API.
"""

import logging
import os
import sys
import collections
import socket
import socketserver
import threading as _threading
import time
import requests

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


# ---------------------------------------------------------------------------
# MLLP Listener Classes (integrated from mllp_server.py)
# ---------------------------------------------------------------------------
class HL7MLLPHandler(socketserver.BaseRequestHandler):
    """Handles incoming TCP connections containing HL7 messages wrapped in MLLP.
    Also detects ASTM-style ENQ/ACK handshaking used by some instruments
    (e.g. Horiba Yumizen) that label their mode as 'MLLP' but still initiate
    with an ASTM handshake before sending HL7 data.
    """

    # MLLP Constants
    SB = b'\x0b'  # Start Block (VT)
    EB = b'\x1c'  # End Block (FS)
    CR = b'\x0d'  # Carriage Return

    # ASTM handshake bytes (some instruments send these before MLLP data)
    ENQ = b'\x05'  # Enquiry
    ACK = b'\x06'  # Acknowledge
    NAK = b'\x15'  # Negative Acknowledge
    EOT = b'\x04'  # End of Transmission
    STX = b'\x02'  # Start of Text
    ETX = b'\x03'  # End of Text

    def setup(self):
        """Configure socket options for instrument compatibility."""
        # Disable Nagle's algorithm — send small segments immediately
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Enable TCP keepalive so the OS probes idle connections
        self.request.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def handle(self):
        client_ip = self.client_address[0]
        client_port = self.client_address[1]
        logger.info(f"[MLLP] New connection established from {client_ip}:{client_port}")

        start_time = time.time()
        total_bytes = 0
        message_count = 0

        try:
            buffer = b''
            astm_session_frames = []  # collect ASTM frames if instrument uses ENQ/ACK

            while True:
                chunk = self.request.recv(4096)
                if not chunk:
                    elapsed = time.time() - start_time
                    if total_bytes == 0:
                        logger.info(
                            f"[MLLP] Connection probe from {client_ip}:{client_port} "
                            f"(connected {elapsed:.2f}s, 0 bytes — instrument may be "
                            f"polling or using ASTM protocol; consider switching to "
                            f"ASTM port if this persists)"
                        )
                    else:
                        logger.info(
                            f"[MLLP] Connection closed by {client_ip} after "
                            f"{elapsed:.2f}s, {total_bytes} bytes, {message_count} messages"
                        )
                    break

                total_bytes += len(chunk)
                logger.info(
                    f"[MLLP] Received {len(chunk)} bytes from {client_ip} "
                    f"(total: {total_bytes}) raw hex: {chunk[:64].hex(' ')}"
                )
                buffer += chunk

                # ----------------------------------------------------------
                # 1) Detect ASTM ENQ — instrument wants to start a session
                # ----------------------------------------------------------
                while self.ENQ in buffer:
                    logger.info(f"[MLLP] ← ASTM ENQ detected from {client_ip}, sending ACK")
                    self.request.sendall(self.ACK)
                    buffer = buffer.replace(self.ENQ, b'', 1)

                # ----------------------------------------------------------
                # 2) Detect ASTM EOT — instrument finished transmission
                # ----------------------------------------------------------
                while self.EOT in buffer:
                    logger.info(f"[MLLP] ← ASTM EOT from {client_ip}")
                    if astm_session_frames:
                        full_message = '\n'.join(astm_session_frames)
                        logger.info(
                            f"[MLLP] Assembled {len(astm_session_frames)} ASTM frames "
                            f"into {len(full_message)} char message"
                        )
                        self._process_astm_message(full_message, client_ip)
                        message_count += 1
                        astm_session_frames = []
                    buffer = buffer.replace(self.EOT, b'', 1)

                # ----------------------------------------------------------
                # 3) Detect ASTM STX…ETX frames
                # ----------------------------------------------------------
                while self.STX in buffer and self.ETX in buffer:
                    stx_pos = buffer.find(self.STX)
                    etx_pos = buffer.find(self.ETX, stx_pos)
                    if stx_pos != -1 and etx_pos != -1:
                        frame_data = buffer[stx_pos + 1:etx_pos]
                        # checksum (2 bytes) + CR+LF after ETX
                        checksum_end = etx_pos + 3
                        if len(buffer) > checksum_end:
                            if len(frame_data) > 0:
                                data = frame_data[1:].decode('utf-8', errors='ignore')
                                astm_session_frames.append(data)
                                logger.info(f"[MLLP] ← ASTM frame: {len(data)} bytes")
                            self.request.sendall(self.ACK)
                            buffer = buffer[checksum_end + 2:]
                        else:
                            break  # wait for more data
                    else:
                        break

                # ----------------------------------------------------------
                # 4) Standard MLLP framing: SB … EB CR
                # ----------------------------------------------------------
                while self.SB in buffer and self.EB + self.CR in buffer:
                    start = buffer.find(self.SB)
                    end = buffer.find(self.EB + self.CR)

                    if start != -1 and end != -1:
                        hl7_data = buffer[start + 1:end]
                        message_count += 1
                        logger.info(
                            f"[MLLP] Extracted HL7 message #{message_count} "
                            f"from {client_ip} ({len(hl7_data)} bytes)"
                        )

                        self._process_message(hl7_data, client_ip)
                        self._send_ack()

                        buffer = buffer[end + 2:]
                    else:
                        break

        except ConnectionResetError:
            logger.warning(
                f"[MLLP] Connection reset by {client_ip}:{client_port} after "
                f"{time.time()-start_time:.2f}s, {total_bytes} bytes"
            )
        except Exception as e:
            logger.error(f"[MLLP] Error handling connection from {client_ip}:{client_port}: {str(e)}")

    def _process_message(self, raw_data, client_ip):
        """Send received HL7 message to the main Flask API"""
        try:
            message = raw_data.decode('utf-8', errors='ignore')
            logger.info(f"[MLLP] Decoded HL7 message: {len(message)} chars from {client_ip}")
            
            port = os.getenv('PORT', 5001)
            api_endpoint = f"http://localhost:{port}/hl7/receive"
            
            try:
                response = requests.post(
                    api_endpoint,
                    data=message,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip
                    },
                    timeout=10
                )
                logger.info(f"[MLLP] HTTP POST → Flask: {response.status_code}")
            except Exception as e:
                logger.error(f"[MLLP] Failed to forward to API: {str(e)}")
                
        except Exception as e:
            logger.error(f"[MLLP] Error processing message: {str(e)}")

    def _process_astm_message(self, raw_data, client_ip):
        """Forward an ASTM message that arrived on the MLLP port."""
        try:
            logger.info(f"[MLLP] Forwarding ASTM message from {client_ip}: {len(raw_data)} chars")
            port = os.getenv('PORT', 5001)
            api_endpoint = f"http://localhost:{port}/astm/receive"
            try:
                response = requests.post(
                    api_endpoint,
                    data=raw_data,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip,
                        'X-Protocol': 'ASTM',
                    },
                    timeout=10,
                )
                logger.info(f"[MLLP] HTTP POST → Flask (ASTM): {response.status_code}")
            except Exception as e:
                logger.error(f"[MLLP] Failed to forward ASTM message to API: {str(e)}")
        except Exception as e:
            logger.error(f"[MLLP] Error processing ASTM message: {str(e)}")

    def _send_ack(self):
        """Send a basic HL7 ACK message back to machine"""
        ack_msg = f"MSH|^~\\&|HL7_LISTENER|LOCAL|LAB_MACHINE|REMOTE|{time.strftime('%Y%m%d%H%M%S')}||ACK|1|P|2.3\rMSA|AA|1\r"
        wrapped_ack = self.SB + ack_msg.encode('utf-8') + self.EB + self.CR
        self.request.sendall(wrapped_ack)


# ---------------------------------------------------------------------------
# ASTM Listener Classes (integrated from astm_server.py)
# ---------------------------------------------------------------------------
class ASTMHandler(socketserver.StreamRequestHandler):
    """Handles incoming TCP connections containing ASTM messages"""
    
    # ASTM Protocol Constants
    STX = b'\x02'  # Start of Text
    ETX = b'\x03'  # End of Text
    EOT = b'\x04'  # End of Transmission
    ENQ = b'\x05'  # Enquiry
    ACK = b'\x06'  # Acknowledge
    NAK = b'\x15'  # Negative Acknowledge
    
    def handle(self):
        client_ip = self.client_address[0]
        client_port = self.client_address[1]
        logger.info(f"[ASTM] New connection established from {client_ip}:{client_port}")
        
        start_time = time.time()
        total_bytes = 0
        frame_count = 0
        
        try:
            buffer = b''
            messages = []
            
            while True:
                chunk = self.request.recv(4096)
                if not chunk:
                    logger.info(f"[ASTM] Connection closed by {client_ip} after {time.time()-start_time:.2f}s, {total_bytes} bytes, {frame_count} frames")
                    break
                
                total_bytes += len(chunk)
                logger.debug(f"[ASTM] Received {len(chunk)} bytes from {client_ip} (total: {total_bytes})")
                buffer += chunk
                
                if self.ENQ in buffer:
                    logger.info(f"[ASTM] ← Received ENQ from {client_ip}")
                    self.request.sendall(self.ACK)
                    logger.info(f"[ASTM] → Sent ACK to {client_ip}")
                    buffer = buffer.replace(self.ENQ, b'')
                    continue
                
                if self.EOT in buffer:
                    logger.info(f"[ASTM] ← Received EOT from {client_ip} — {len(messages)} frames complete")
                    if messages:
                        full_message = '\n'.join(messages)
                        logger.info(f"[ASTM] Assembled {len(messages)} frames into {len(full_message)} char message")
                        self._process_message(full_message, client_ip)
                    messages = []
                    buffer = buffer.replace(self.EOT, b'')
                    continue
                
                while self.STX in buffer and self.ETX in buffer:
                    stx_pos = buffer.find(self.STX)
                    etx_pos = buffer.find(self.ETX, stx_pos)
                    
                    if stx_pos != -1 and etx_pos != -1:
                        frame_data = buffer[stx_pos + 1:etx_pos]
                        checksum_end = etx_pos + 3
                        
                        if len(buffer) > checksum_end:
                            checksum = buffer[etx_pos + 1:etx_pos + 3]
                            frame_count += 1
                            
                            if len(frame_data) > 0:
                                frame_num = frame_data[0:1]
                                data = frame_data[1:].decode('utf-8', errors='ignore')
                                messages.append(data)
                                logger.info(f"[ASTM] ← Frame {frame_num.decode('utf-8','ignore')}: {len(data)} bytes")
                            
                            self.request.sendall(self.ACK)
                            buffer = buffer[checksum_end + 2:]
                        else:
                            break
                    else:
                        break
                        
        except Exception as e:
            logger.error(f"[ASTM] Error handling connection from {client_ip}:{client_port}: {str(e)}")
    
    def _process_message(self, raw_data, client_ip):
        """Send received ASTM message to the main Flask API"""
        try:
            logger.info(f"[ASTM] Parsed ASTM message from {client_ip}: {len(raw_data)} chars")
            
            port = os.getenv('PORT', 5001)
            api_endpoint = f"http://localhost:{port}/astm/receive"
            
            try:
                response = requests.post(
                    api_endpoint,
                    data=raw_data,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip,
                        'X-Protocol': 'ASTM'
                    },
                    timeout=10
                )
                logger.info(f"[ASTM] HTTP POST → Flask: {response.status_code}")
            except Exception as e:
                logger.error(f"[ASTM] Failed to forward to API: {str(e)}")
                
        except Exception as e:
            logger.error(f"[ASTM] Error processing ASTM message: {str(e)}")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Handle requests in a separate thread."""
    allow_reuse_address = True
    daemon_threads = True


# ---------------------------------------------------------------------------
# Listener Startup Functions
# ---------------------------------------------------------------------------
def start_mllp_listener(host, port):
    """Start MLLP listener in background thread"""
    try:
        logger.info(f"[MLLP] Starting listener on {host}:{port}")
        server = ThreadedTCPServer((host, port), HL7MLLPHandler)
        server.serve_forever()
    except Exception as e:
        logger.error(f"[MLLP] Failed to start listener: {str(e)}")


def start_astm_listener(host, port):
    """Start ASTM listener in background thread"""
    try:
        logger.info(f"[ASTM] Starting listener on {host}:{port}")
        server = ThreadedTCPServer((host, port), ASTMHandler)
        server.serve_forever()
    except Exception as e:
        logger.error(f"[ASTM] Failed to start listener: {str(e)}")


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

    # Start MLLP Listener in background thread
    mllp_host = app.config.get('MLLP_HOST', '0.0.0.0')
    mllp_port = app.config.get('MLLP_PORT', 6000)
    mllp_thread = _threading.Thread(
        target=start_mllp_listener,
        args=(mllp_host, mllp_port),
        daemon=True,
        name="MLLP-Listener"
    )
    mllp_thread.start()
    logger.info(f'MLLP listener thread started on {mllp_host}:{mllp_port}')

    # Start ASTM Listener in background thread
    astm_host = app.config.get('ASTM_HOST', '0.0.0.0')
    astm_port = app.config.get('ASTM_PORT', 7000)
    astm_thread = _threading.Thread(
        target=start_astm_listener,
        args=(astm_host, astm_port),
        daemon=True,
        name="ASTM-Listener"
    )
    astm_thread.start()
    logger.info(f'ASTM listener thread started on {astm_host}:{astm_port}')

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
    logger.info(f'MLLP listener: {app.config.get("MLLP_HOST","0.0.0.0")}:{app.config.get("MLLP_PORT",6000)} (auto-started)')
    logger.info(f'ASTM listener: {app.config.get("ASTM_HOST","0.0.0.0")}:{app.config.get("ASTM_PORT",7000)} (auto-started)')
    logger.info('=' * 60)
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        debug=app.config['DEBUG']
    )
