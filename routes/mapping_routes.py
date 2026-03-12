"""
API routes for field mapping management
"""

from flask import Blueprint, request, jsonify
import logging

logger = logging.getLogger(__name__)


def create_mapping_blueprint(mapping_service):
    """
    Create Flask blueprint for mapping routes.
    
    Args:
        mapping_service: MappingService instance
        
    Returns:
        Flask Blueprint
    """
    bp = Blueprint('mappings', __name__)
    
    @bp.route('/api/mappings', methods=['GET'])
    def get_mappings():
        """Get all mapping profiles"""
        try:
            profiles = mapping_service.mapping_repository.get_all_profiles()
            return jsonify({
                'success': True,
                'profiles': profiles
            }), 200
        except Exception as e:
            logger.error(f"Failed to get mappings: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/<int:profile_id>', methods=['GET'])
    def get_mapping(profile_id):
        """Get specific mapping profile"""
        try:
            profile = mapping_service.mapping_repository.get_profile(profile_id)
            if not profile:
                return jsonify({
                    'success': False,
                    'error': 'Profile not found'
                }), 404
            
            return jsonify({
                'success': True,
                'profile': profile
            }), 200
        except Exception as e:
            logger.error(f"Failed to get mapping {profile_id}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings', methods=['POST'])
    def create_mapping():
        """Create new mapping profile"""
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'No data provided'
                }), 400
            
            name = data.get('name', '').strip()
            description = data.get('description', '')
            protocol_filter = data.get('protocol_filter', 'ALL')
            config = data.get('config', [])
            
            if not name:
                return jsonify({
                    'success': False,
                    'error': 'Profile name is required'
                }), 400
            
            # Validate config
            is_valid, error_msg = mapping_service.validate_config(config)
            if not is_valid:
                return jsonify({
                    'success': False,
                    'error': f'Invalid config: {error_msg}'
                }), 400
            
            # Create profile
            profile_id = mapping_service.mapping_repository.create_profile(
                name=name,
                description=description,
                protocol_filter=protocol_filter,
                config=config
            )
            
            return jsonify({
                'success': True,
                'profile_id': profile_id,
                'message': f'Created mapping profile: {name}'
            }), 201
            
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        except Exception as e:
            logger.error(f"Failed to create mapping: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/<int:profile_id>', methods=['PUT'])
    def update_mapping(profile_id):
        """Update existing mapping profile"""
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'No data provided'
                }), 400
            
            name = data.get('name')
            description = data.get('description')
            protocol_filter = data.get('protocol_filter')
            config = data.get('config')
            
            # Validate config if provided
            if config is not None:
                is_valid, error_msg = mapping_service.validate_config(config)
                if not is_valid:
                    return jsonify({
                        'success': False,
                        'error': f'Invalid config: {error_msg}'
                    }), 400
            
            # Update profile
            success = mapping_service.mapping_repository.update_profile(
                profile_id=profile_id,
                name=name,
                description=description,
                protocol_filter=protocol_filter,
                config=config
            )
            
            if not success:
                return jsonify({
                    'success': False,
                    'error': 'Profile not found'
                }), 404
            
            return jsonify({
                'success': True,
                'message': 'Mapping profile updated'
            }), 200
            
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        except Exception as e:
            logger.error(f"Failed to update mapping {profile_id}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/<int:profile_id>', methods=['DELETE'])
    def delete_mapping(profile_id):
        """Delete mapping profile"""
        try:
            success = mapping_service.mapping_repository.delete_profile(profile_id)
            
            if not success:
                return jsonify({
                    'success': False,
                    'error': 'Profile not found'
                }), 404
            
            return jsonify({
                'success': True,
                'message': 'Mapping profile deleted'
            }), 200
            
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        except Exception as e:
            logger.error(f"Failed to delete mapping {profile_id}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/<int:profile_id>/activate', methods=['POST'])
    def activate_mapping(profile_id):
        """Set mapping profile as active"""
        try:
            success = mapping_service.mapping_repository.set_active_profile(profile_id)
            
            if not success:
                return jsonify({
                    'success': False,
                    'error': 'Profile not found'
                }), 404
            
            return jsonify({
                'success': True,
                'message': 'Mapping profile activated'
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to activate mapping {profile_id}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/<int:profile_id>/deactivate', methods=['POST'])
    def deactivate_mapping(profile_id):
        """Deactivate specific mapping profile"""
        try:
            # Get profile to check if it's the one being deactivated
            profile = mapping_service.mapping_repository.get_profile(profile_id)
            if not profile:
                return jsonify({
                    'success': False,
                    'error': 'Profile not found'
                }), 404
            
            if profile['is_active']:
                mapping_service.mapping_repository.deactivate_all_profiles()
            
            return jsonify({
                'success': True,
                'message': 'Mapping profile deactivated'
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to deactivate mapping {profile_id}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/preview', methods=['POST'])
    def preview_mapping():
        """Preview mapping transformation without saving"""
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'No data provided'
                }), 400
            
            source_data = data.get('data')
            config = data.get('config', [])
            
            if not source_data:
                return jsonify({
                    'success': False,
                    'error': 'Source data is required'
                }), 400
            
            # Validate config
            is_valid, error_msg = mapping_service.validate_config(config)
            if not is_valid:
                return jsonify({
                    'success': False,
                    'error': f'Invalid config: {error_msg}'
                }), 400
            
            # Apply mapping
            result = mapping_service.preview_mapping(source_data, config)
            
            return jsonify({
                'success': True,
                'result': result
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to preview mapping: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @bp.route('/api/mappings/sample/<protocol>', methods=['GET'])
    def get_sample_data(protocol):
        """Get sample parsed data structure for HL7 or ASTM"""
        protocol = protocol.upper()
        
        if protocol == 'HL7':
            sample = {
                'hl7_version': '2.5',
                'timestamp': '2024-03-02T10:30:00',
                'message_type': {
                    'message_code': 'ORU',
                    'trigger_event': 'R01'
                },
                'message_control_id': 'MSG12345',
                'sending_application': 'LAB_SYSTEM',
                'sending_facility': 'Lab A',
                'patient': {
                    'id': 'PAT123',
                    'name': {
                        'family_name': 'Doe',
                        'given_name': 'John',
                        'middle_name': 'Q'
                    },
                    'date_of_birth': '1980-01-15',
                    'sex': 'M',
                    'address': '123 Main St, Boston, MA 02101',
                    'phone_home': '555-1234'
                },
                'orders': [
                    {
                        'filler_order_number': 'ORD789',
                        'placer_order_number': 'ORD123',
                        'universal_service_id': 'CBC^Complete Blood Count',
                        'observation_datetime': '2024-03-02T09:00:00',
                        'result_status': 'F'
                    }
                ],
                'observations': [
                    {
                        'set_id': '1',
                        'value_type': 'NM',
                        'identifier': 'WBC^White Blood Cell Count',
                        'value': '7.5',
                        'units': '10*3/uL',
                        'reference_range': '4.5-11.0',
                        'abnormal_flags': 'N',
                        'observation_status': 'F'
                    },
                    {
                        'set_id': '2',
                        'value_type': 'NM',
                        'identifier': 'RBC^Red Blood Cell Count',
                        'value': '4.8',
                        'units': '10*6/uL',
                        'reference_range': '4.5-5.5',
                        'abnormal_flags': 'N',
                        'observation_status': 'F'
                    }
                ]
            }
        elif protocol == 'ASTM':
            sample = {
                'protocol': 'ASTM',
                'timestamp': '2024-03-02T10:30:00',
                'header': {
                    'sender_name': 'Lab Analyzer',
                    'version': '1.0',
                    'processing_id': 'P'
                },
                'patient': {
                    'practice_patient_id': 'PAT123',
                    'lab_patient_id': 'LAB456',
                    'name': {
                        'last': 'Doe',
                        'first': 'John',
                        'middle': 'Q'
                    },
                    'date_of_birth': '1980-01-15',
                    'sex': 'M'
                },
                'orders': [
                    {
                        'specimen_id': 'SPEC789',
                        'universal_test_id': {
                            'test_id': 'CBC',
                            'test_name': 'Complete Blood Count',
                            'loinc_code': '58410-2'
                        },
                        'priority': 'R'
                    }
                ],
                'results': [
                    {
                        'universal_test_id': {
                            'test_id': 'WBC',
                            'test_name': 'White Blood Cell Count',
                            'loinc_code': '6690-2'
                        },
                        'value': '7.5',
                        'units': '10*3/uL',
                        'reference_range': '4.5-11.0',
                        'abnormal_flags': 'N',
                        'result_status': 'F'
                    },
                    {
                        'universal_test_id': {
                            'test_id': 'RBC',
                            'test_name': 'Red Blood Cell Count',
                            'loinc_code': '789-8'
                        },
                        'value': '4.8',
                        'units': '10*6/uL',
                        'reference_range': '4.5-5.5',
                        'abnormal_flags': 'N',
                        'result_status': 'F'
                    }
                ]
            }
        else:
            return jsonify({
                'success': False,
                'error': 'Protocol must be HL7 or ASTM'
            }), 400
        
        return jsonify({
            'success': True,
            'protocol': protocol,
            'sample': sample
        }), 200
    
    return bp
