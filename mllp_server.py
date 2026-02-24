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
    
    def handle(self):
        client_ip = self.client_address[0]
        logger.info(f"Connection received from {client_ip}")
        
        try:
            # Read streaming data
            data = b''
            buffer = b''
            
            while True:
                # Read a chunk
                chunk = self.request.recv(4096)
                if not chunk:
                    break
                
                buffer += chunk
                
                # Check for MLLP wrapping
                while SB in buffer and EB + CR in buffer:
                    # Extract message between SB and EB+CR
                    start = buffer.find(SB)
                    end = buffer.find(EB + CR)
                    
                    if start != -1 and end != -1:
                        # Get the raw HL7 message
                        hl7_data = buffer[start+1:end]
                        
                        # Process the message
                        self._process_message(hl7_data, client_ip)
                        
                        # Send MLLP ACK (Acknowledgement)
                        # Machines expect an ACK or they will retry/alarm
                        self._send_ack()
                        
                        # Remove processed message from buffer
                        buffer = buffer[end+2:]
                    else:
                        # Message incomplete, wait for more data
                        break
                        
        except Exception as e:
            logger.error(f"Error handling connection from {client_ip}: {str(e)}")

    def _process_message(self, raw_data, client_ip):
        """Send received HL7 message to the main Flask API"""
        try:
            # Decode bytes to string
            message = raw_data.decode('utf-8', errors='ignore')
            logger.info(f"Received HL7 message: {len(message)} chars from {client_ip}")
            
            # Forward to Flask App
            try:
                response = requests.post(
                    API_ENDPOINT,
                    data=message,
                    headers={
                        'Content-Type': 'text/plain',
                        'X-Original-Source-IP': client_ip
                    },
                    timeout=10
                )
                logger.info(f"Forwarded to API: Status {response.status_code}")
            except Exception as e:
                logger.error(f"Failed to forward to API: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def _send_ack(self):
        """Send a basic HL7 ACK message back to machine"""
        # A generic ACK is usually sufficient for the transport layer
        # In a full system, we might parse MSH to allow matching IDs
        ack_msg = f"MSH|^~\\&|HL7_LISTENER|LOCAL|LAB_MACHINE|REMOTE|{time.strftime('%Y%m%d%H%M%S')}||ACK|1|P|2.3\rMSA|AA|1\r"
        
        # Wrap in MLLP envelopes
        wrapped_ack = SB + ack_msg.encode('utf-8') + EB + CR
        self.request.sendall(wrapped_ack)


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Handle requests in a separate thread."""
    allow_reuse_address = True


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
