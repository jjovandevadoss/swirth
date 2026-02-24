"""
Configuration settings for the HL7 Lab Machine Interface
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration"""
    
    # Flask settings
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 5001))
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

    # MLLP Listener settings
    MLLP_HOST = os.getenv('MLLP_HOST', '0.0.0.0')
    MLLP_PORT = int(os.getenv('MLLP_PORT', 6000))

    # ASTM Listener settings
    ASTM_HOST = os.getenv('ASTM_HOST', '0.0.0.0')
    ASTM_PORT = int(os.getenv('ASTM_PORT', 7000))

    # API settings
    API_URL = os.getenv('API_URL', 'https://your-api-endpoint.com/data')
    API_KEY = os.getenv('API_KEY', '')
    API_TIMEOUT = int(os.getenv('API_TIMEOUT', 30))

    # Persistence and delivery settings
    DB_PATH = os.getenv('DB_PATH', 'data/messages.db')
    DELIVERY_MAX_ATTEMPTS = int(os.getenv('DELIVERY_MAX_ATTEMPTS', 5))
    DELIVERY_POLL_INTERVAL = int(os.getenv('DELIVERY_POLL_INTERVAL', 10))
    
    # Security settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'change-this-in-production')
    
    # Logging settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
