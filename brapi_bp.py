from flask import Blueprint, render_template
import oracledb
from flask import request
import math
import os

brapi_bp = Blueprint('brapi_bp', __name__,url_prefix='/genotyping/brapi/v2')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

@brapi_bp.route('/')
def index():
    return render_template('index.html')
    

@brapi_bp.route('/serverinfo',methods = ['GET', 'OPTIONS'])
def server_info():
    #serverinfo = config.get('brapi', {}).get('serverinfo', {})
    serverinfo = os.environ.get('brapi',{})

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
                        "methods": ["GET",],
                        "service": "serverinfo",
                        "versions": ["2.1"]
                    },
                    {
                        "contentTypes": ["application/json"],
                        "dataTypes": ["application/json"],
                        "methods": ["GET",],
                        "service": "samples",
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
    return output


@brapi_bp.route('/samples')
def get_samples():
    res_context = None
    res_datafiles = []
    res_status = []
    
     # Get page size and page number from query parameters
    res_page_size = max(int(request.args.get('pageSize', 1000)),1)
    res_current_page = max(int(request.args.get('currentPage', request.args.get('page', 0))),0)

    samples = []
    total_count = 0
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
                samples.append(
                    {'additionalInfo': r[0], 'column': r[1], 'externalReferences': [{"referenceId":r[2],"referenceSource":  ""}], 'germplasmDbId': r[3], 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId':  str(r[10]), 'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]})
    res_total_count = len(samples)
    res_total_pages = math.ceil(res_total_count / res_page_size )

    # Apply pagination to samples
    start_index = (res_current_page - 1) * res_page_size
    end_index = start_index + res_page_size
    paginated_samples = samples[start_index:end_index]
    
    
    return {
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
        "totalcount": total_count,
        "result": {
            "data": samples
        }
    }


@brapi_bp.route('/samples/<reference_id>')
def get_sample_by_reference(reference_id):
    sample = 'sample not found!'
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
                if r[2] == reference_id or str(r[10]) == reference_id:
                    sample = {'additionalInfo': r[0], 'column': r[1], 'externalReferences':[{"referenceId":r[2],"referenceSource":  ""}], 'germplasmDbId': str("r[3]"), 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': str(r[10]),
                              'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]}
    return  {
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
        "totalcount": 1,
        "result": {
            "data": sample
        }
    }
