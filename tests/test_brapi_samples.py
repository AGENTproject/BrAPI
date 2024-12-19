
import pytest
from main import app  # Import your Flask app from where it is defined
import logging



# Pytest fixture to create a test client
@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

# Simple test for the /samples endpoint
def test_get_samples_simple(client):
    response = client.get('/genotyping/brapi/v2/samples?page=1&pageSize=10')
    assert response.status_code == 200
    data = response.get_json()
    assert 'result' in data
    assert 'data' in data['result']
    assert isinstance(data['result']['data'], list)

# More complex test with query parameters
def test_get_samples_complex(client):
    response = client.get('/genotyping/brapi/v2/samples?pageSize=5&currentPage=1&sampleType=DNA')
    assert response.status_code == 200
    data = response.get_json()
    assert 'result' in data
    assert 'data' in data['result']
    assert isinstance(data['result']['data'], list)
    assert data['metadata']['pagination']['currentPage'] == 1
    assert data['metadata']['pagination']['pageSize'] == 5

# Test for a specific sample by reference ID
def test_get_sample_by_reference_id_simple(client):
    reference_id = "3135477"  # Replace with a valid ID from your database.
    response = client.get(f'/genotyping/brapi/v2/samples/{reference_id}')
    assert response.status_code == 200
    data = response.get_json()
    assert 'result' in data
    assert 'sampleDbId' in data['result']
    assert data['result']['sampleDbId'] == reference_id

# Test with logging to track the /samples endpoint request
def test_sample_endpoint(client):
    logging.basicConfig(level=logging.INFO)  # Set up logging
    logger = logging.getLogger("test_sample_endpoint")  # Create logger instance
    
    logger.info("Starting the test for the /samples endpoint")
    
    # Use the test client to make an actual request to the endpoint
    response = client.get('/genotyping/brapi/v2/samples?page=1&pageSize=10')
    logger.info(f"Response received: {response.get_json()}")
    
    assert response.status_code == 200  # Check if status code is 200 (OK)
    
    logger.info("Test completed successfully")




