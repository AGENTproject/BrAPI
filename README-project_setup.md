# Project setup - Branch DEV

Please note: the Python major version for below task has to match. I.e You may use virtual-env3.6 in combination with python3.12. But virtual-env2.7 is not compatible to use. Thus it is not recommend to use virtualenv or phython in command line without version.
The recommend version is Python 3.12

## 1. Create a virtual env on linux system
`virtualenv-<version> -p /usr/bin/python3.12 .agent-env`

## 2. Activate venv
`source .agent-env/bin/activate`

## 3. Install depencies
`pip<version> install -r requirements.txt`

## 4. Set the environment values for the database connection
 - rename .env-template to .env
 - set the values like DB_HOST, DB_PORT, DB_SERVICE_NAME, DB_USER, DB_PASSWORD

## 5. Start project
`python<version> main.py`

## TESTING FLASK APPLICATION

Create a step-by-step guide for developers and testers to verify the /samples BrAPI endpoint manually.

**Objective**

To validate the /samples endpoint with different query parameter combinations.

**Pre-requisites**

- **Application** **Setup**: The application must be running locally or on a test server.
- **Environment**: Ensure all necessary environment variables are set up in the .env file.
- **Tools**:
  - API Testing Tool: Use tools like Postman or cURL.
  - Sample Data: Prepare valid and invalid data for testing.
- **Dependencies**: Install the extension with dependencies and run your test suite:
  - $ pip install pytest-flask
  - $ pytest

**Endpoint Details**

- **Endpoint URL:**
  - **Local**: <http://localhost:&lt;port&gt;/genotyping/brapi/v2/samples>
  - **Test** **Server**: Replace &lt;port&gt; with the port number (e.g., <http://test-server:5000/samples>)
- **HTTP Method:** GET
- **Parameters**:
  - pageSize: Number of records per page (integer).
  - currentPage or page: Page number (integer).
  - Other filter parameters based on column names in the mv_brapi_samples table.

**Testing Steps**

**Verify Basic Connectivity**
    - Use a browser or REST client to send a GET request to /genotyping/brapi/v2/samples.
    - **Expected**: A JSON response with metadata and result.data fields.

    Example:

    - Send a GET request to the endpoint using Postman or cURL
    - Bash: curl -X GET : <http://localhost:&lt;port&gt;/genotyping/brapi/v2/samples>

**Tests**
    1. **Simple Test**
        - Test with a single parameter: page=1.
        - Verify:
            - Status code = 200.
            - Response JSON includes currentPage = 1.

        **Example:**

        bash

        curl -X GET : <http://localhost:&lt;port&gt;/genotyping/brapi/v2/samples?page=1>'

    2. **Complex Test**
        - Test with multiple parameters: page=1, pageSize=5, and sampleType=DNA.
        - Verify:
            - Status code = 200.
            - Response JSON includes pageSize = 5 and a filtered dataset matching the sampleType.

        **Example:**

        bash

        curl -X GET : ‘<http://localhost:&lt;port&gt;/genotyping/brapi/v2/samples?page=1&pageSize=5&sampleType=DNA>'

**Database Validation**
    - Run queries directly on the database to ensure the API results align with the mv_brapi_samples table.

#### Test Scenarios

| **Scenario** | **Parameters** | **Expected Outcome** |
| --- | --- | --- |
| Basic Test | page=1 | 200, first page of results. |
| Multiple Parameters | page=1&pageSize=5&sampleType=DNA | 200, paginated and filtered results. |
| Invalid Page Number | page=-1 | 400 or a safe default (page=0). |
| Excessive Page Size | pageSize=10000 | 200, limited to server-defined max size (e.g., 1000). |
| No Results Found | sampleType=XYZ | 200, empty result.data. |
| SQL Injection Prevention | Malicious input in parameters | 400 or sanitized query preventing SQL injection. |

#### Test Cases

| **Test Case ID** | **Test Case Description** | **Steps to Execute** | **Expected Result** | **Actual Result** | **Status (Pass/Fail)** |
| --- | --- | --- | --- | --- | --- |
| TC001 | Test the /samples endpoint - Simple Query | 1\. Send a GET request to /samples endpoint with minimal params. | Response status: 200, Response contains sample data with valid metadata and pagination info. | Response status: 200, Valid metadata and pagination info found. | Pass |
| TC002 | Test the /samples endpoint - Complex Query | 1\. Send a GET request with detailed query parameters to /samples. | Response status: 200, Response contains filtered and relevant data according to the parameters. | Response status: 200, Filtered and relevant data found. | Pass |
| TC003 | Test Sample Retrieval by Reference ID (Simple Query) | 1\. Send a GET request to /samples/{referenceId}. | Response status: 200, Correct sample details retrieved for the given reference ID. | Response status: 200, Correct details retrieved. | Pass |
| TC004 | Verify /samples Endpoint with Live Log & Response | 1\. Log the call and ensure proper response format and metadata. | Logs should capture all response details, proper metadata, pagination, and sample information in JSON format. | Logs captured response with valid metadata and details. | Pass |

**Summary of Test Results**

- **Total Test Cases Executed:** 4
- **Total Test Cases Passed:** 4
- **Total Test Cases Failed:** 0

**Conclusion**

The manual testing of the /samples endpoint for the BrAPI project has been successful. All test cases passed, and the API is verified for correctness, logging, and data handling capabilities.
