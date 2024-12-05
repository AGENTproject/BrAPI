import pytest
from main import app  # Import your Flask app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_get_attributes_complex(client):
    # Replace `attributeType` with a valid filter key specific to your endpoint
    response = client.get('/genotyping/brapi/v2/attributes?pageSize=10&page=1&attributeType=trait')
    assert response.status_code == 200
    data = response.get_json()

    # Check metadata and result structure
    assert 'metadata' in data
    assert 'result' in data
    assert 'data' in data['result']
    assert isinstance(data['result']['data'], list)

    # Ensure at least one attribute matches the filter
    attributes = data['result']['data']
    for attribute in attributes:
        assert 'attributeType' in attribute
        assert attribute['attributeType'] == 'trait'

    # Check pagination
    metadata = data['metadata']
    assert 'pagination' in metadata
    assert metadata['pagination']['pageSize'] == 10
    assert metadata['pagination']['currentPage'] == 1

def test_get_attribute_by_id_simple(client):
    valid_attribute_id = "10121"  # Replace with a valid attribute ID from your database
    response = client.get(f'/genotyping/brapi/v2/attributes/{valid_attribute_id}')
    
    assert response.status_code == 200
    data = response.get_json()

    # Check result structure
    assert 'result' in data
    assert 'attributeDbId' in data['result']
    assert data['result']['attributeDbId'] == valid_attribute_id

def test_get_attribute_by_id_not_found(client):
    invalid_attribute_id = "nonexistent_id"
    response = client.get(f'/genotyping/brapi/v2/attributes/{invalid_attribute_id}')
    
    assert response.status_code == 404
    data = response.get_json()
    
    # Fix the case mismatch for the error message
    assert data == "attribute not found!"

