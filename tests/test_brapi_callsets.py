
import pytest
from main import app  # Import your Flask app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_get_callsets_simple(client):
    response = client.get('/genotyping/brapi/v2/callsets?page=1&pageSize=10')
    assert response.status_code == 200
    data = response.get_json()
    assert 'result' in data
    assert 'data' in data['result']
    assert isinstance(data['result']['data'], list)
    assert 'metadata' in data
    assert 'pagination' in data['metadata']
    assert data['metadata']['pagination']['currentPage'] == 1
    assert data['metadata']['pagination']['pageSize'] == 10

def test_get_callsets_complex(client):
    # Replace 'samplePUI_value' with a valid callSetDbId/samplePUI from your database
    sample_pui = "samplePUI_value"
    response = client.get(f'/genotyping/brapi/v2/callsets?callSetDbId={sample_pui}&pageSize=5&currentPage=0')
    assert response.status_code == 200
    data = response.get_json()
    
    assert 'result' in data
    assert 'data' in data['result']
    assert isinstance(data['result']['data'], list)
    
    # Validate the returned data contains the specified callSetDbId
    callsets = data['result']['data']
    for callset in callsets:
        assert callset['callSetDbId'].lower().strip() == sample_pui.lower().strip()

    # Verify pagination metadata
    pagination = data['metadata']['pagination']
    assert 'currentPage' in pagination
    assert 'pageSize' in pagination
    assert 'totalCount' in pagination
    assert pagination['currentPage'] == 0
    assert pagination['pageSize'] == 5



