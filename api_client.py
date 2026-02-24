"""
API Client for forwarding parsed HL7 data to external APIs
"""

import requests
import logging
from typing import Dict, Any, Optional
import json

logger = logging.getLogger(__name__)


class APIClient:
    """
    Client for sending parsed HL7 data to external APIs in JSON format.
    Handles authentication, retries, and error handling.
    """
    
    def __init__(self, api_url: str, api_key: Optional[str] = None, timeout: int = 30):
        """
        Initialize the API client.
        
        Args:
            api_url: The URL of the external API endpoint
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds (default: 30)
        """
        self.api_url = api_url
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        
        # Set up headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'HL7-Lab-Interface/1.0'
        })
        
        # Add API key to headers if provided
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}'
            })
        
        logger.info(f"API Client initialized for endpoint: {self.api_url}")
    
    def send_data(self, data: Dict[str, Any], retry_count: int = 3) -> requests.Response:
        """
        Send parsed HL7 data to the external API.
        
        Args:
            data: Parsed HL7 data as dictionary
            retry_count: Number of retry attempts on failure (default: 3)
            
        Returns:
            Response object from the API
            
        Raises:
            Exception: If all retry attempts fail
        """
        # Validate data
        if not data:
            raise ValueError("Cannot send empty data to API")
        
        # Convert data to JSON
        try:
            json_data = json.dumps(data, indent=2)
            logger.debug(f"Prepared JSON data: {len(json_data)} bytes")
        except Exception as e:
            logger.error(f"Failed to serialize data to JSON: {str(e)}")
            raise Exception(f"JSON serialization error: {str(e)}")
        
        # Attempt to send data with retries
        last_exception = None
        
        for attempt in range(retry_count):
            try:
                logger.info(f"Sending data to API (attempt {attempt + 1}/{retry_count})")
                
                response = self.session.post(
                    self.api_url,
                    data=json_data,
                    timeout=self.timeout
                )
                
                # Check if request was successful
                if response.status_code in [200, 201, 202]:
                    logger.info(f"Successfully sent data to API: {response.status_code}")
                    return response
                elif response.status_code in [400, 401, 403, 404]:
                    # Client errors - don't retry
                    logger.error(f"API client error {response.status_code}: {response.text}")
                    raise Exception(f"API client error {response.status_code}: {response.text}")
                elif response.status_code >= 500:
                    # Server errors - retry
                    logger.warning(f"API server error {response.status_code}, will retry")
                    last_exception = Exception(f"API server error {response.status_code}: {response.text}")
                else:
                    # Other status codes
                    logger.warning(f"Unexpected API response {response.status_code}: {response.text}")
                    return response
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout on attempt {attempt + 1}")
                last_exception = Exception(f"Request timeout after {self.timeout} seconds")
            
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on attempt {attempt + 1}: {str(e)}")
                last_exception = Exception(f"Connection error: {str(e)}")
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception on attempt {attempt + 1}: {str(e)}")
                last_exception = Exception(f"Request error: {str(e)}")
            
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                last_exception = Exception(f"Unexpected error: {str(e)}")
            
            # Wait before retry (exponential backoff)
            if attempt < retry_count - 1:
                import time
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.info(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
        
        # All retries failed
        logger.error(f"Failed to send data after {retry_count} attempts")
        raise last_exception if last_exception else Exception("Failed to send data to API")
    
    def send_batch_data(self, data_list: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        """
        Send multiple HL7 records in batch.
        
        Args:
            data_list: List of parsed HL7 data dictionaries
            
        Returns:
            List of results for each record
        """
        results = []
        
        for idx, data in enumerate(data_list):
            try:
                response = self.send_data(data)
                results.append({
                    'index': idx,
                    'status': 'success',
                    'status_code': response.status_code,
                    'response': response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text
                })
            except Exception as e:
                logger.error(f"Failed to send batch item {idx}: {str(e)}")
                results.append({
                    'index': idx,
                    'status': 'error',
                    'error': str(e)
                })
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test the connection to the API endpoint.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try a simple GET request to check connectivity
            response = self.session.get(self.api_url, timeout=10)
            logger.info(f"Connection test result: {response.status_code}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def close(self):
        """Close the session"""
        self.session.close()
        logger.info("API Client session closed")
