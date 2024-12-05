import pytest

@pytest.fixture
def client():
    """
    Fixture to set up the Flask test client.
    """
    from main import app  # Import your Flask app
    with app.test_client() as client:
        yield client


def test_get_germplasm_simple(client):
    """
    Simple test for /germplasm with just one query parameter.
    """
    response = client.get('/genotyping/brapi/v2/germplasm?page=1')
    assert response.status_code == 200, "Expected status code 200 for a valid request"
    data = response.get_json()

    # Validate metadata
    assert "metadata" in data
    assert "pagination" in data["metadata"]
    assert data["metadata"]["pagination"]["currentPage"] == 1

    # Validate result
    assert "result" in data
    assert "data" in data["result"]
    assert isinstance(data["result"]["data"], list), "Expected 'data' to be a list"


def test_get_germplasm_complex(client):
    """
    Complex test for /germplasm with multiple query parameters.
    """
    response = client.get('/genotyping/brapi/v2/germplasm?page=1&pageSize=5&commonCropName=Maize')
    assert response.status_code == 200, "Expected status code 200 for a valid request with multiple parameters"
    data = response.get_json()

    # Validate metadata
    assert "metadata" in data
    assert "pagination" in data["metadata"]
    assert data["metadata"]["pagination"]["pageSize"] == 5
    assert data["metadata"]["pagination"]["currentPage"] == 1

    # Validate result
    assert "result" in data
    assert "data" in data["result"]
    assert isinstance(data["result"]["data"], list), "Expected 'data' to be a list"

    # Validate filtered data
    for germplasm in data["result"]["data"]:
        assert germplasm.get("commonCropName") == "Maize", f"Unexpected crop name: {germplasm.get('commonCropName')}"

