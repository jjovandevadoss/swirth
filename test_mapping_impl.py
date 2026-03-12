#!/usr/bin/env python
"""Quick test of mapping service functionality"""

import sys
sys.path.insert(0, '.')

from storage.mapping_repository import MappingRepository
from services.mapping_service import MappingService

# Create repository
repo = MappingRepository('data/test_mappings.db')

# Test creating a profile
config = [
    {'source_path': 'patient.id', 'target_path': 'patientId', 'transform': None},
    {'source_path': 'observations[*].value', 'target_path': 'results[*].testValue', 'transform': 'lowercase'}
]

profile_id = repo.create_profile('Test Profile', 'Test description', 'HL7', config)
print(f"✓ Created profile ID: {profile_id}")

# Test loading profile
profile = repo.get_profile(profile_id)
print(f"✓ Loaded profile: {profile['name']}")
print(f"  Config has {len(profile['config'])} rules")

# Test mapping service
service = MappingService(repo)
repo.set_active_profile(profile_id)

# Test data
test_data = {
    'patient': {'id': 'PAT123'},
    'observations': [
        {'value': 'HIGH'},
        {'value': 'NORMAL'}
    ]
}

result = service.apply_mapping(test_data)
print(f"✓ Applied mapping successfully")
print(f"  Result: {result}")

print("\n✓✓✓ All mapping service tests passed! ✓✓✓")
