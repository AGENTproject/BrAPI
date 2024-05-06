from flask import Blueprint, render_template, jsonify
import oracledb
from flask import request
import math
import os

brapi_bp = Blueprint('brapi_bp', __name__, url_prefix='/genotyping/brapi/v2')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

@brapi_bp.route('/')
def index():
    return render_template('index.html')


@brapi_bp.route('/serverinfo', methods=['GET', 'OPTIONS'])
def server_info():
    # serverinfo = config.get('brapi', {}).get('serverinfo', {})
    serverinfo = os.environ.get('brapi', {})

    server_name = serverinfo.get('server_name', None)
    server_description = serverinfo.get('server_description', None)
    organization_name = serverinfo.get('organization_name', None)
    organization_url = serverinfo.get('organization_url', None)
    location = serverinfo.get('location', None)
    contact_email = serverinfo.get('contact_email', None)
    documentation_url = serverinfo.get('documentation_url', None)

    output = {
        "@context": [
            "https://brapi.org/jsonld/context/metadata.jsonld"
        ],
        "metadata": {
            "datafiles": [],
            "pagination": None,
            "status": [
                {
                    "message": "Request accepted, response successful",
                    "messageType": "INFO"
                }
            ]
        },
        "result": {
            "calls": [
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "serverinfo",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "samples",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "callsets",
                    "versions": ["2.1"]
                }
            ],
            "contactEmail": contact_email,
            "documentationURL": documentation_url,
            "location": location,
            "organizationName": organization_name,
            "organizationURL": organization_url,
            "serverDescription": server_description,
            "serverName": server_name
        }
    }
    return jsonify(output)


@brapi_bp.route('/samples')
def get_samples():
    res_context = None
    res_datafiles = []
    res_status = []

    # Get page size and page number from query parameters
    res_page_size = max(int(request.args.get('pageSize', 1000)), 1)
    res_current_page = max(int(request.args.get('currentPage', request.args.get('page', 0))), 0)

    # Construct the WHERE clause based on query parameters
    where_clause = ""
    query_parameters = request.args.to_dict()
    for key, value in query_parameters.items():
        if key != 'pageSize' and key != 'currentPage' and key != 'page':
            if where_clause:
                where_clause += " AND "
            where_clause += f'"{key}" = \'{value}\''

    samples = []

    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = f"""SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            if where_clause:
                sql += f" WHERE {where_clause}"
            for r in cursor.execute(sql):
                sample = {
                    'additionalInfo': r[0], 'column': r[1], 'externalReferences': [{"referenceId": r[2], "referenceSource": ""}], 'germplasmDbId': r[3], 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': str(r[10]), 'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]}
                samples.append(sample)

    res_total_count = len(samples)
    res_total_pages = math.ceil(res_total_count / res_page_size)

    # Apply pagination to samples
    start_index = res_current_page * res_page_size
    end_index = min(start_index + res_page_size, res_total_count)
    paginated_samples = samples[start_index:end_index]

    return jsonify({
        "@context": res_context,
        "metadata": {
            "datafiles": res_datafiles,
            "status": res_status,
            "pagination": {
                "pageSize": res_page_size,
                "totalCount": res_total_count,  # Remove this line to eliminate the totalCount entry
                "totalPages": res_total_pages,
                "currentPage": res_current_page
            }
        },
        "result": {
            "data": paginated_samples
        }
    })



@brapi_bp.route('/samples/<reference_id>')
def get_sample_by_reference_id(reference_id):
    sample = None
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
                if str(r[10]) == reference_id:
                    sample = {
                        'additionalInfo': r[0], 'column': r[1], 'externalReferences': [{"referenceId": r[2], "referenceSource": ""}], 'germplasmDbId': str(r[3]), 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': str(r[10]),
                              'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]}
                    break

    if sample:
        return jsonify({
            "metadata": {
                "datafiles": [],
                "status": [],
                "pagination": {
                    "pageSize": 0,
                    "totalCount": 1,
                    "totalPages": 1,
                    "currentPage": 0
                }
            },
            "result": sample
        }), 200
    else:
        return jsonify("sample not found!"), 404

@brapi_bp.route('/callsets')
def get_callsets():
    callSetDbId = request.args.get('callSetDbId')
    if callSetDbId:
        callSet = None
    

    res_context = None
    res_datafiles = []
    res_status = []

    # Get page size and page number from query parameters
    res_page_size = max(int(request.args.get('pageSize', 1000)), 1)
    res_current_page = max(int(request.args.get('currentPage', request.args.get('page', 0))), 0)

    # Construct the WHERE clause based on query parameters
    where_clause = ""
    query_parameters = request.args.to_dict()
    for key, value in query_parameters.items():
        if key not in ['pageSize', 'currentPage', 'page']:
            if where_clause:
                where_clause += " AND "
            if key == 'callSetDbId':
                 where_clause += f'"samplePUI" = \'{value}\''
            else:
                 where_clause += f'"{key}" = \'{value}\''

    callSets = []
    
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = f"""SELECT "samplePUI", "sampleDbId", "sampleTimestamp" FROM mv_brapi_samples"""
            if where_clause:
                sql += f" WHERE {where_clause}"
            cursor.execute(sql)
            for r in cursor.fetchall():
                callSet = {
                    'callSetDbId': str(r[0]),  # Mapping samplePUI to callSetDbId
                    'sampleDbId': r[1],
                    'created': r[2],
                    'updated': r[2],  # Assuming the sampleTimestamp for both created and updated dates
                    'additionalInfo': {},  # If there are additional attributes to include
                    'externalReferences': [],  # Assuming how to handle external references if available
                    'variantSetDbIds': []  # Placeholder if there's relevant data to link
                }
                callSets.append(callSet)

    res_total_count = len(callSets)
    res_total_pages = math.ceil(res_total_count / res_page_size)

    # Apply pagination
    start_index = res_current_page * res_page_size
    end_index = min(start_index + res_page_size, res_total_count)
    paginated_callSets = callSets[start_index:end_index]

    return jsonify({
        "@context": res_context,
        "metadata": {
            "datafiles": res_datafiles,
            "status": res_status,
            "pagination": {
                "pageSize": res_page_size,
                "totalCount": res_total_count,
                "totalPages": res_total_pages,
                "currentPage": res_current_page
            }
        },
        "result": {
            "data": paginated_callSets
        }
    })

@brapi_bp.route('/callsets/<reference_id>')
def get_callset_by_reference_id(reference_id):
    callSet = None
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "samplePUI", "sampleDbId", "sampleTimestamp" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
               if str(r[0]) == reference_id:
                    callSet = {
                        'callSetDbId': str(r[0]),
                        'samplePUI': r[0],
                        'sampleDbId': str(r[1]),
                        'created': r[2],
                        'updated': r[2],
                        'additionalInfo': {},
                        'externalReferences': [],
                        'variantSetDbIds': []
                    }
                    

    if callSet:
        return jsonify({
            "metadata": {
                "datafiles": [],
                "status": [],
                "pagination": {
                    "pageSize": 0,
                    "totalCount": 1,
                    "totalPages": 1,
                    "currentPage": 0
                }
            },
            "result": callSet
        }), 200
    else:
        return jsonify("Callset not found!"), 404
    

