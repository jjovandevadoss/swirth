"""
MLLP (Minimal Lower Layer Protocol) Receiver
This script listens for standard HL7 messages over TCP/IP (MLLP) 
and forwards them to the Flask application for processing.

Standard Lab Machines often use this protocol instead of HTTP.
"""

import socketserver
import requests
import logging
import time
import os
from dotenv import load_dotenv

# Load config
load_dotenv()

# Configuration
MLLP_HOST = os.getenv('MLLP_HOST', '0.0.0.0')
MLLP_PORT = int(os.getenv('MLLP_PORT', 6000))  # Standard listener port for machines
API_ENDPOINT = f"http://localhost:{os.getenv('PORT', 5001)}/hl7/receive"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - MLLP Listener - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MLLP Constants
SB = b'\x0b'  # Start Block (VT)
EB = b'\x1c'  # End Block (FS)
CR = b'\x0d'  # Carriage Return

class HL7MLLPHandler(socketserver.StreamRequestHandler):
    """
    Handles incoming TCP connections containing HL7 messages wrapped in MLLP
    """
    
    # Set socket timeout to None for persistent connections
    timeout = None
    
    def handle(self):
        client_ip = self.client_address[0]
        client_port = self.client_address[1]
        logger.info(f"[MLLP] New connection established from {client_ip}:{client_port}")
        
        start_time = time.time()
        total_bytes = 0
        message_count = 0
        
        try:
            # Read streaming data
            data = b''
            buffer = b''
            
            while True:
                # Read a chunk
                chunk = self.request.recv(4096)
                if not chunk:
                    logger.info(f"[MLLP] Connection closed by {client_ip} after {time.time()-start_time:.2f}s, {total_bytes} bytes, {message_count} messages")
                    break
                
                total_bytes += len(chunk)
                logger.debug(f"[MLLP] Received {len(chunk)} bytes from {client_ip} (total: {total_bytes})")
                buffer += chunk
                
                # Check for MLLP wrapping
                while SB in buffer and EB + CR in buffer:
                    # Extract message between SB and EB+CR
                    start = buffer.find(SB)
                    end = buffer.find(EB + CR)
                    
                    if start != -1 and end != -1:
                        # Get the raw HL7 message
                        hl7_data = buffer[start+1:end]
                        message_count += 1
                        logger.info(f"[MLLP] Extracted HL7 message #{message_count} from {client_ip} ({len(hl7_data)} bytes between MLLP framing)")
                        
                        # Process the message
                        self._process_message(hl7_data, client_ip)
                        
                        # Send MLLP ACK (Acknowledgement)
                        # Machines expect an ACK or they will retry/alarm
                        ack_bytes = self._send_ack()
                        logger.debug(f"[MLLP] Sent {ack_bytes} byte ACK to {client_ip}")
                        
                        # Remove processed message from buffer
                        buffer = buffer[end+2:]
                    else:
                        # Message incomplete, wait for more data
                        break
                        
        except Exception as e:
            logger.error(f"[MLLP] Error handling connection from {client_ip}:{client_port} after {time.time()-start_time:.2f}s: {str(e)}")

    def _process_message(self, raw_data, client_ip):
        """Send received HL7 message to the main Flask API"""
        try:
            # Decode bytes to string
            message = raw_data.decode('utf-8', errors='ignore')
            msg_lines = message.count('\r') + 1
            logger.info(f"[MLLP] Decoded HL7 message: {len(message)} chars, {msg_lines} segments from {client_ip}")
            
            # Extract MSH for logging
            if message.startswith('MSH'):
                msh_end = message.find('\r')
                if msh_end > 0:
                    logger.debug(f"[MLLP] MSH: {message[:msh_end]}")
            
            # Forward to Flask App
            try:
                logger.debug(f"[MLLP] POSTing to {API_ENDPOINT}")
                response = requests.post(
                    API_ENDPOINT,
                    data=message,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip
                    },
                    timeout=10
                )
                logger.info(f"[MLLP] HTTP POST → Flask: {response.status_code} ({len(response.content)} bytes response)")
            except requests.Timeout:
                logger.error(f"[MLLP] Timeout forwarding to Flask API after 10s")
            except requests.ConnectionError as e:
                logger.error(f"[MLLP] Connection error to Flask API: {str(e)}")
            except Exception as e:
                logger.error(f"[MLLP] Failed to forward to API: {str(e)}")
                
        except Exception as e:
            logger.error(f"[MLLP] Error processing message: {str(e)}")

    def _send_ack(self):
        """Send a basic HL7 ACK message back to machine"""
        # A generic ACK is usually sufficient for the transport layer
        # In a full system, we might parse MSH to allow matching IDs
        ack_msg = f"MSH|^~\\&|HL7_LISTENER|LOCAL|LAB_MACHINE|REMOTE|{time.strftime('%Y%m%d%H%M%S')}||ACK|1|P|2.3\rMSA|AA|1\r"
        
        # Wrap in MLLP envelopes
        wrapped_ack = SB + ack_msg.encode('utf-8') + EB + CR
        self.request.sendall(wrapped_ack)
        return len(wrapped_ack)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Handle requests in a separate thread."""
    allow_reuse_address = True
    
    def server_activate(self):
        """Enable TCP keepalive for persistent connections"""
        self.socket.setsockopt(socketserver.socket.SOL_SOCKET, socketserver.socket.SO_KEEPALIVE, 1)
        super().server_activate()


if __name__ == "__main__":
    logger.info(f"Starting MLLP Listener on {MLLP_HOST}:{MLLP_PORT}")
    logger.info(f"Forwarding data to: {API_ENDPOINT}")
    
    try:
        server = ThreadedTCPServer((MLLP_HOST, MLLP_PORT), HL7MLLPHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down MLLP server...")
        server.shutdown()
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
