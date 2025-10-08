# Mock configuration file for testing
# This file provides mock configurations for the pipeline when running in test environments

from unittest.mock import Mock

# Mock Spark session
class MockSparkSession:
    def __init__(self):
        self.conf = Mock()
        self.read = Mock()
    
    def set(self, key, value):
        pass

# Create mock spark session
spark = MockSparkSession()

# Mock configuration variables for testing
STORAGE_ACCOUNT = 'teststorageaccount'
SERVICE_PRINCIPAL_CLIENT_ID = 'test-client-id'
SERVICE_PRINCIPAL_CLIENT_SECRET = 'test-client-secret'
TENANT_ID = 'test-tenant-id'
RAW_CONTAINER = 'raw-container'
INPUT_PATH = 'input/path'
SILVER_CONTAINER = 'silver-container'
DELTA_SILVER_PATH = 'delta/silver/path'