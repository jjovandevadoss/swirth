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
    
    def __init__(self, api_url: str, api_key: Optional[str] = None, timeout: int = 30, mapping_service=None):
        """
        Initialize the API client.
        
        Args:
            api_url: The URL of the external API endpoint
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds (default: 30)
            mapping_service: Optional MappingService for custom field transformations
        """
        self.api_url = api_url
        self.api_key = api_key
        self.timeout = timeout
        self.mapping_service = mapping_service
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
    
    def _transform_to_client_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform parsed HL7/ASTM data to the client's required format.
        Uses custom mapping service if available, otherwise falls back to default format.
        
        Args:
            data: Parsed HL7 or ASTM data
            
        Returns:
            Transformed data in client format
        """
        # Detect protocol from data
        protocol = data.get('protocol', 'HL7')  # ASTM data has 'protocol' field, HL7 doesn't
        
        # Use mapping service if available
        if self.mapping_service:
            try:
                transformed = self.mapping_service.apply_mapping(data, protocol)
                if transformed is not data:
                    logger.info(
                        "[APIClient] Custom mapping applied  protocol=%s  top_level_keys=%d",
                        protocol,
                        len(transformed),
                    )
                    return transformed
                logger.debug("[APIClient] No custom mapping matched; using default layout  protocol=%s", protocol)
            except Exception as e:
                logger.warning(
                    "[APIClient] Custom mapping raised an exception — falling back to default  protocol=%s  error=%s",
                    protocol,
                    e,
                )
        
        # Fallback to the default requested JSON layout
        logger.debug("[APIClient] Using default transformation for outbound JSON  protocol=%s", protocol)

        patient = data.get("patient") or {}
        first_order = (data.get("orders") or [{}])[0] if data.get("orders") else {}
        specimen_id_parsed = first_order.get("specimen_id_parsed") or {} if isinstance(first_order, dict) else {}

        display_candidates = [
            patient.get("id"),
            patient.get("practice_patient_id"),
            patient.get("lab_patient_id"),
            specimen_id_parsed.get("accession_number") if isinstance(specimen_id_parsed, dict) else None,
            first_order.get("filler_order_number") if isinstance(first_order, dict) else None,
            first_order.get("placer_order_number") if isinstance(first_order, dict) else None,
            first_order.get("specimen_id") if isinstance(first_order, dict) else None,
        ]
        display_number = str(next((value for value in display_candidates if value not in (None, "", [], {})), ""))

        test_candidates = [
            data.get("message_profile"),
            first_order.get("universal_service_id") if isinstance(first_order, dict) else None,
            first_order.get("test_profile") if isinstance(first_order, dict) else None,
            (first_order.get("universal_test_id") or {}).get("display_name") if isinstance(first_order, dict) else None,
            (first_order.get("universal_test_id") or {}).get("raw") if isinstance(first_order, dict) else None,
        ]
        test_name = str(next((value for value in test_candidates if value not in (None, "", [], {})), ""))

        # Build result array from observations or results
        result = []
        
        # Try HL7 observations first
        if data.get("observations"):
            for obs in data["observations"]:
                field_name = str(obs.get("identifier") or "")
                test_result = str(obs.get("value") or "")
                if field_name or test_result:
                    result.append({
                        "fieldName": field_name,
                        "testResult": test_result
                    })
        # Try ASTM results
        elif data.get("results"):
            for res in data["results"]:
                test_id = res.get("universal_test_id", {}) or {}
                field_name = str(
                    test_id.get("display_name")
                    or test_id.get("mnemonic")
                    or test_id.get("test_name")
                    or test_id.get("test_id")
                    or ""
                )
                test_result = str(res.get("value") or "")
                if field_name or test_result:
                    result.append({
                        "fieldName": field_name,
                        "testResult": test_result
                    })
        
        transformed = {
            "displayNumber": display_number,
            "testName": test_name,
            "result": result
        }
        
        logger.info(
            "[APIClient] Default transform complete  protocol=%s  displayNumber=%s  testName=%s  results=%d",
            protocol,
            display_number or "(empty)",
            test_name or "(empty)",
            len(result),
        )
        return transformed
    
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
        
        # Transform data to client format
        try:
            transformed_data = self._transform_to_client_format(data)
        except Exception as e:
            logger.error(f"Failed to transform data: {str(e)}")
            raise Exception(f"Data transformation error: {str(e)}")
        
        # Convert data to JSON
        try:
            json_data = json.dumps(transformed_data, indent=2)
            logger.debug("[APIClient] Serialised outbound payload  bytes=%d", len(json_data))
        except Exception as e:
            logger.error("[APIClient] JSON serialisation failed: %s", e, exc_info=True)
            raise Exception(f"JSON serialization error: {str(e)}")
        
        # Attempt to send data with retries
        last_exception = None
        
        for attempt in range(retry_count):
            try:
                logger.info(
                    "[APIClient] POST %s  attempt=%d/%d  payload_bytes=%d",
                    self.api_url,
                    attempt + 1,
                    retry_count,
                    len(json_data),
                )
                
                response = self.session.post(
                    self.api_url,
                    data=json_data,
                    timeout=self.timeout
                )
                
                # Check if request was successful
                if response.status_code in [200, 201, 202]:
                    logger.info(
                        "[APIClient] Delivery accepted  http=%d  url=%s",
                        response.status_code,
                        self.api_url,
                    )
                    return response
                elif response.status_code in [400, 401, 403, 404]:
                    # Client errors - don't retry
                    logger.error(
                        "[APIClient] Client error  http=%d  body=%s",
                        response.status_code,
                        response.text[:500],
                    )
                    raise Exception(f"API client error {response.status_code}: {response.text}")
                elif response.status_code >= 500:
                    # Server errors - retry
                    logger.warning(
                        "[APIClient] Server error  http=%d  attempt=%d/%d  body=%s",
                        response.status_code,
                        attempt + 1,
                        retry_count,
                        response.text[:200],
                    )
                    last_exception = Exception(f"API server error {response.status_code}: {response.text}")
                else:
                    logger.warning(
                        "[APIClient] Unexpected status  http=%d  body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    return response
                
            except requests.exceptions.Timeout:
                logger.warning(
                    "[APIClient] Request timed out  attempt=%d/%d  timeout=%ds  url=%s",
                    attempt + 1, retry_count, self.timeout, self.api_url,
                )
                last_exception = Exception(f"Request timeout after {self.timeout} seconds")
            
            except requests.exceptions.ConnectionError as e:
                logger.warning(
                    "[APIClient] Connection error  attempt=%d/%d  url=%s  error=%s",
                    attempt + 1, retry_count, self.api_url, e,
                )
                last_exception = Exception(f"Connection error: {str(e)}")
            
            except requests.exceptions.RequestException as e:
                logger.error(
                    "[APIClient] Request exception  attempt=%d/%d  error=%s",
                    attempt + 1, retry_count, e,
                    exc_info=True,
                )
                last_exception = Exception(f"Request error: {str(e)}")
            
            except Exception as e:
                logger.error(
                    "[APIClient] Unexpected error  attempt=%d/%d  error=%s",
                    attempt + 1, retry_count, e,
                    exc_info=True,
                )
                last_exception = Exception(f"Unexpected error: {str(e)}")
            
            # Wait before retry (exponential backoff)
            if attempt < retry_count - 1:
                import time
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.info("[APIClient] Waiting %ds before retry", wait_time)
                time.sleep(wait_time)
        
        # All retries failed
        logger.error(
            "[APIClient] All %d attempt(s) failed  url=%s",
            retry_count,
            self.api_url,
        )
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
