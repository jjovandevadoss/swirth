"""
ASTM (American Society for Testing and Materials) Receiver
This script listens for laboratory instrument messages using ASTM E1394 protocol
and forwards them to the Flask application for processing.

ASTM is commonly used by chemistry analyzers, hematology analyzers, and other lab equipment.
"""

import socketserver
import requests
import logging
import os
from dotenv import load_dotenv

# Load config
load_dotenv()

# Configuration
ASTM_HOST = os.getenv('ASTM_HOST', '0.0.0.0')
ASTM_PORT = int(os.getenv('ASTM_PORT', 7000))  # Standard ASTM listener port
API_ENDPOINT = f"http://localhost:{os.getenv('PORT', 5001)}/astm/receive"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ASTM Listener - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ASTM Protocol Constants
STX = b'\x02'  # Start of Text
ETX = b'\x03'  # End of Text
EOT = b'\x04'  # End of Transmission
ENQ = b'\x05'  # Enquiry (request to send)
ACK = b'\x06'  # Acknowledge
NAK = b'\x15'  # Negative Acknowledge
CR = b'\x0d'   # Carriage Return
LF = b'\x0a'   # Line Feed


class ASTMHandler(socketserver.StreamRequestHandler):
    """
    Handles incoming TCP connections containing ASTM messages
    """
    
    def handle(self):
        client_ip = self.client_address[0]
        client_port = self.client_address[1]
        logger.info(f"[ASTM] New connection established from {client_ip}:{client_port}")
        
        import time
        start_time = time.time()
        total_bytes = 0
        frame_count = 0
        
        try:
            buffer = b''
            messages = []
            frame_number = 0
            
            while True:
                # Read data chunk
                chunk = self.request.recv(4096)
                if not chunk:
                    logger.info(f"[ASTM] Connection closed by {client_ip} after {time.time()-start_time:.2f}s, {total_bytes} bytes, {frame_count} frames")
                    break
                
                total_bytes += len(chunk)
                logger.debug(f"[ASTM] Received {len(chunk)} bytes from {client_ip} (total: {total_bytes})")
                buffer += chunk
                
                # Handle ENQ (request to send)
                if ENQ in buffer:
                    logger.info(f"[ASTM] ← Received ENQ (Enquiry) from {client_ip} — analyzer requesting to send")
                    self.request.sendall(ACK)
                    logger.info(f"[ASTM] → Sent ACK (1 byte) to {client_ip} — ready to receive")
                    buffer = buffer.replace(ENQ, b'')
                    continue
                
                # Handle EOT (end of transmission)
                if EOT in buffer:
                    logger.info(f"[ASTM] ← Received EOT (End of Transmission) from {client_ip} — {len(messages)} frames complete")
                    if messages:
                        # Process all collected messages
                        full_message = '\n'.join(messages)
                        logger.info(f"[ASTM] Assembled {len(messages)} frames into {len(full_message)} char message")
                        self._process_message(full_message, client_ip)
                    messages = []
                    frame_number = 0
                    buffer = buffer.replace(EOT, b'')
                    continue
                
                # Parse ASTM frames (STX + frame_num + data + ETX + checksum + CR + LF)
                while STX in buffer and ETX in buffer:
                    stx_pos = buffer.find(STX)
                    etx_pos = buffer.find(ETX, stx_pos)
                    
                    if stx_pos != -1 and etx_pos != -1:
                        # Extract frame
                        frame_data = buffer[stx_pos + 1:etx_pos]
                        
                        # Check for checksum (2 bytes after ETX)
                        checksum_end = etx_pos + 3
                        if len(buffer) > checksum_end:
                            checksum = buffer[etx_pos + 1:etx_pos + 3]
                            frame_count += 1
                            
                            # Verify checksum
                            checksum_valid = self._verify_checksum(frame_data, checksum)
                            if checksum_valid:
                                # Extract frame number and data
                                if len(frame_data) > 0:
                                    frame_num = frame_data[0:1]
                                    data = frame_data[1:].decode('utf-8', errors='ignore')
                                    record_type = data[0] if data else '?'
                                    
                                    messages.append(data)
                                    logger.info(f"[ASTM] ← Frame {frame_num.decode('utf-8','ignore')} (record {record_type}): {len(data)} bytes, checksum OK")
                                
                                # Send ACK
                                self.request.sendall(ACK)
                                logger.debug(f"[ASTM] → Sent ACK to {client_ip}")
                            else:
                                logger.warning(f"[ASTM] Frame checksum FAILED (expected {checksum.hex()}, calculated mismatch) — sending NAK")
                                # Send NAK
                                self.request.sendall(NAK)
                                logger.info(f"[ASTM] → Sent NAK to {client_ip}")
                            
                            # Remove processed frame from buffer
                            buffer = buffer[checksum_end + 2:]  # +2 for CR+LF
                        else:
                            # Incomplete frame, wait for more data
                            break
                    else:
                        break
                        
        except Exception as e:
            logger.error(f"[ASTM] Error handling connection from {client_ip}:{client_port} after {time.time()-start_time:.2f}s: {str(e)}")
    
    def _verify_checksum(self, data, checksum):
        """Verify ASTM checksum"""
        try:
            # ASTM checksum is the sum of all bytes modulo 256
            calculated = sum(data) % 256
            expected = int(checksum, 16)
            return calculated == expected
        except:
            return True  # If checksum verification fails, accept anyway
    
    def _process_message(self, raw_data, client_ip):
        """Send received ASTM message to the main Flask API"""
        try:
            # Count records (H, P, O, R, etc.)
            lines = raw_data.strip().split('\n')
            record_summary = {}
            for line in lines:
                if line:
                    rec_type = line[0]
                    record_summary[rec_type] = record_summary.get(rec_type, 0) + 1
            summary = ', '.join([f"{k}:{v}" for k,v in sorted(record_summary.items())])
            
            logger.info(f"[ASTM] Parsed ASTM message from {client_ip}: {len(raw_data)} chars, records=[{summary}]")
            
            # Forward to Flask App
            try:
                logger.debug(f"[ASTM] POSTing to {API_ENDPOINT}")
                response = requests.post(
                    API_ENDPOINT,
                    data=raw_data,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip,
                        'X-Protocol': 'ASTM'
                    },
                    timeout=10
                )
                logger.info(f"[ASTM] HTTP POST → Flask: {response.status_code} ({len(response.content)} bytes response)")
            except requests.Timeout:
                logger.error(f"[ASTM] Timeout forwarding to Flask API after 10s")
            except requests.ConnectionError as e:
                logger.error(f"[ASTM] Connection error to Flask API: {str(e)}")
            except Exception as e:
                logger.error(f"[ASTM] Failed to forward to API: {str(e)}")
                
        except Exception as e:
            logger.error(f"[ASTM] Error processing ASTM message: {str(e)}")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Handle requests in a separate thread."""
    allow_reuse_address = True


if __name__ == "__main__":
    logger.info(f"Starting ASTM Listener on {ASTM_HOST}:{ASTM_PORT}")
    logger.info(f"Forwarding data to: {API_ENDPOINT}")
    
    try:
        server = ThreadedTCPServer((ASTM_HOST, ASTM_PORT), ASTMHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down ASTM server...")
        server.shutdown()
    except Exception as e:
        logger.error(f"Failed to start ASTM server: {str(e)}")
