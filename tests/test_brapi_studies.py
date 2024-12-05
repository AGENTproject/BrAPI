import pytest
from main import app  # Import your Flask app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

# Simple test for the `/studies` endpoint
def test_get_study_by_id_simple(client):
    valid_study_id = "8368"  # Replace with a valid study ID from your database
    response = client.get(f'/genotyping/brapi/v2/studies/{valid_study_id}')
    assert response.status_code == 200
    data = response.get_json()
    assert 'result' in data
    assert 'studyDbId' in data['result']
    # Convert study ID to integer for comparison
    assert data['result']['studyDbId'] == int(valid_study_id)


def test_get_studies_complex(client):
    response = client.get('/genotyping/brapi/v2/studies?pageSize=5&currentPage=1&studyType=field_trial')
    assert response.status_code == 200
    data = response.get_json()
    assert 'metadata' in data
    assert 'pagination' in data['metadata']
    pagination = data['metadata']['pagination']

    # Validate pagination fields
    assert 'totalCount' in pagination
    res_total_count = pagination['totalCount']
    assert res_total_count >= 0  # Allow for 0 records in the database
    assert pagination['currentPage'] == 1
    assert pagination['pageSize'] == 5

