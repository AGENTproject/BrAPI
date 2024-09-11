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

# Routes and their functions

FAO_STORAGE_CODES = {
    "10":"Seed collection",
    "11":"Short term",
    "12":"Medium term",
    "13":"Long term",
    "20":"Field collection",
    "30":"In vitro collection",
    "40":"Cryopreserved collection",
    "50":"DNA collection",
    "99":"Other",
}

FAO_SAMPSTAT_CODES = {
    100:"Wild",
    110:"Natural",
    120:"Semi-natural/wild",
    130:"Semi-natural/sown",
    200:"Weedy",
    300:"Traditional cultivar/landrace",
    400:"Breeding/research material",
    410:"Breeder's line",
    411:"Synthetic population",
    412:"Hybrid",
    413:"Founder stock/base population",
    414:"Inbred line",
    415:"Segregating population",
    416:"Clonal selection",
    420:"Genetic stock",
    421:"Mutant",
    422:"Cytogenetic stocks",
    423:"Other genetic stocks",
    500:"Advanced or improved cultivar",
    600:"GMO",
    999:"Other",
}

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
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "attributes",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "attributevalues",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "germplasm",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "studies",
                    "versions": ["2.1"]
                },

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

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def handle_lob(value):
    """Helper function to convert LOB to string."""
    if isinstance(value, oracledb.LOB):
        return value.read()
    return value

@brapi_bp.route('samples')
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
            if is_number(value):
                where_clause += f'"{key}" = {value}'
            else:
                where_clause += f'"{key}" = \'{value}\''

    samples = []
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}") as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM mv_brapi_samples"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "sampleDbId" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    sample = {
                        'additionalInfo':handle_lob(r[0]),  # Handle LOB
                        'column': r[1], 
                        'externalReferences': [{"referenceId": handle_lob(r[2]), "referenceSource": ""}], 
                        'germplasmDbId': str(r[3]), 
                        'observationUnitDbId': str(r[4]), 
                        'plateDbId': r[5], 
                        'plateName': r[6], 
                        'programDbId': r[7], 
                        'row': r[8], 
                        'sampleBarcode': r[9], 
                        'sampleDbId': str(r[10]), 
                        'sampleDescription': handle_lob(r[11]), 
                        'sampleGroupDbId': r[12], 
                        'sampleName': r[13], 
                        'samplePUI': handle_lob(r[14]), 
                        'sampleTimestamp': r[15], 
                        'sampleType': r[16], 
                        'studyDbId': r[17], 
                        'takenBy': r[18], 
                        'tissueType': r[19], 
                        'trialDbId': r[20], 
                        'well': r[21]
                    }
                    samples.append(sample)
    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        samples = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        samples = []

    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": samples
        }
    })

@brapi_bp.route('germplasm')
def get_germplasm():
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

    germplasms = []

    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V006_ACCESSION_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "CROPNAME", "ID", "ACCENAME", "AGENT_ID", "ACCENUMB", "ACQDATE", "SAMPSTAT", "ORIGCTY", "DONORNUMB", "DONORCODE", "GENUS", "COORDUNCERT", "DECLATITUDE", "DECLONGITUDE", "INSTCODE", "ANCEST", "SPECIES", "SPAUTHOR", "STORAGE", "SUBTAXON", "SUBTAUTHOR" FROM V006_ACCESSION_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "ID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                for r in cursor.execute(sql):
                    germplasm = {
                        'commonCropName': r[0],
                        'germplasmDbId': str(r[1]),
                        'germplasmName': r[2],
                        'germplasmPUI': r[3],
                        'accessionNumber': r[4],
                        'acquisitionDate': r[5],
                        'countryOfOriginCode': r[7],
                        'defaultDisplayName': r[2],
                        'donors': [],
                        'genus': r[10],
                        'instituteCode': r[14],
                        'pedigree': r[15],
                        'species': r[16],
                        'speciesAuthority': r[17],
                        'storageTypes': [],
                        'subtaxa': r[19],
                        'subtaxaAuthority': r[20],
                    }
                    if r[6]:
                        germplasm['biologicalStatusOfAccessionCode'] = str(r[6])
                        germplasm['biologicalStatusOfAccessionDescription'] = FAO_SAMPSTAT_CODES[r[6]]
                    else:
                        germplasm['biologicalStatusOfAccessionCode'] = None
                        germplasm['biologicalStatusOfAccessionDescription'] = None
                    {"donorAccessionNumber": r[8], "donorInstituteCode": r[9]}
                    if r[8] or r[9]:
                        germplasm['donors'].append({"donorAccessionNumber": r[8], "donorInstituteCode": r[9]})
                    if r[11] and r[12] and r[13]:
                        germplasm['germplasmOrigin'] = [{
                            "coordinateUncertainty": r[11], 
                            "coordinates": {
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [float(r[12]), float(r[13])],
                                }, 
                                "type": "Feature"
                            }
                        }]
                    else:
                        germplasm['germplasmOrigin'] = []
                    if r[18]:
                        for i in r[18].split(";"):
                            germplasm['storageTypes'].append({"code": i, "description": FAO_STORAGE_CODES[i]})
                    germplasms.append(germplasm)
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        germplasms = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        germplasms = []

    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": germplasms
        }
    })  
    
@brapi_bp.route('studies')
def get_studies():
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
            where_clause += f'"{key.upper()}" = \'{value}\''
    
    studies = []

    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            # Get number of rows
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V007_STUDY_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            # Get studies data
            with connection.cursor() as cursor:
                sql = f"""SELECT "STUDYDBID", "STUDYNAME", "ADDITIONALINFO", "COMMONCROPNAME", "ENDDATE", "LOCATIONNAME", "STARTDATE", "STUDYCODE", "STUDYDESCRIPTION" FROM V007_STUDY_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                print (sql)
                sql += f""" ORDER BY "STUDYDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                for r in cursor.execute(sql):
                    study = {
                        'studyDbId': r[0], 
                        'studyName': r[1], 
                        'additionalInfo': r[2], 
                        'commonCropName': r[3], 
                        'endDate': r[4], 
                        'environmentParameters': [], 
                        'locationName': r[5], 
                        'startDate': r[6], 
                        'studyCode': r[7], 
                        'studyDescription': r[8], 
                        'observationVariableDbIds': []
                    }
                    studies.append(study)
        
        if studies:
            # Build where clause for filtering by paginated data
            where_clause = ""
            for study in studies:
                if where_clause:
                    where_clause += " OR "
                where_clause += f'"STUDYDBID" = \'{study["studyDbId"]}\''
                
            # For every study in paginated data get environment parameters
            with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "STUDYDBID", "PARAMETERNAME", "VALUE" FROM V008_ENVIRONMENT_PARAMETERS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    for r in cursor.execute(sql):
                        studyDbId = r[0]
                        environmentParameter = {
                            "parameterName": r[1], 
                            "value": r[2]
                        }
                        for study in studies:
                            if study["studyDbId"] == studyDbId:
                                study['environmentParameters'].append(environmentParameter)
                
            # For every study in paginated data get observation variables
            with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "STUDYDBID", "OBSERVATIONVARIABLEDBID" FROM V009_OBSERVATION_VARIABLE_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    for r in cursor.execute(sql):
                        studyDbId = r[0]
                        observationVariableDbId = r[1]
                        for study in studies:
                            if study["studyDbId"] == studyDbId:
                                study['observationVariableDbIds'].append(observationVariableDbId)
                            
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        studies = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        studies = []

    # Calculate number of pages
    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": studies
        }
    })

@brapi_bp.route('/samples/<reference_id>')
def get_sample_by_reference_id(reference_id):
    sample = None
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
                sql += f""" WHERE "sampleDbId" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    sample = {
                        'additionalInfo': handle_lob(result[0]), 
                        'column': result[1], 
                        'externalReferences': [{"referenceId": handle_lob(result[2]), "referenceSource": ""}], 
                        'germplasmDbId': str(result[3]), 
                        'observationUnitDbId': result[4], 
                        'plateDbId': result[5], 
                        'plateName': result[6], 
                        'programDbId': result[7], 
                        'row': result[8], 
                        'sampleBarcode': result[9], 
                        'sampleDbId': str(result[10]),
                        'sampleDescription': handle_lob(result[11]),
                        'sampleGroupDbId': result[12],
                        'sampleName': result[13],
                        'samplePUI': handle_lob(result[14]),
                        'sampleTimestamp': result[15],
                        'sampleType': result[16],
                        'studyDbId': result[17],
                        'takenBy': result[18],
                        'tissueType': result[19],
                        'trialDbId': result[20],
                        'well': result[21]
                    }
                else:
                    sample = None
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        sample = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        sample = None
    
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

@brapi_bp.route('/studies/<reference_id>')
def get_study_by_reference_id(reference_id):
    study = None
    
    where_clause = f'"STUDYDBID" = \'{reference_id}\''
    
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "STUDYDBID", "STUDYNAME", "ADDITIONALINFO", "COMMONCROPNAME", "ENDDATE", "LOCATIONNAME", "STARTDATE", "STUDYCODE", "STUDYDESCRIPTION" FROM V007_STUDY_BRAPI"""
                sql += f""" WHERE "STUDYDBID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    study = {
                        'studyDbId': result[0], 
                        'studyName': result[1], 
                        'additionalInfo': result[2], 
                        'commonCropName': result[3], 
                        'endDate': result[4], 
                        'environmentParameters': [], 
                        'locationName': result[5], 
                        'startDate': result[6], 
                        'studyCode': result[7], 
                        'studyDescription': result[8], 
                        'observationVariableDbIds': []
                    }
                else:
                    study = None

        if study:
            # Build where clause for filtering by paginated data
            where_clause = f'"STUDYDBID" = \'{study["studyDbId"]}\''
        
            # For every study in paginated data get environment parameters
            with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "PARAMETERNAME", "VALUE" FROM V008_ENVIRONMENT_PARAMETERS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    for r in cursor.execute(sql):
                        environmentParameter = {
                            "parameterName": r[0], 
                            "value": r[1]
                        }
                        study['environmentParameters'].append(environmentParameter)
                
            # For every study in paginated data get observation variables
            with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "OBSERVATIONVARIABLEDBID" FROM V009_OBSERVATION_VARIABLE_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    for r in cursor.execute(sql):
                        observationVariableDbId = r[0]
                        study['observationVariableDbIds'].append(observationVariableDbId)
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        study = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        study = None

    if study:
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
            "result": study
        }), 200
    else:
        return jsonify("sample not found!"), 404

@brapi_bp.route('/germplasm/<reference_id>')
def get_germplasm_by_reference_id(reference_id):
    germplasm = None
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT "CROPNAME", "ID", "ACCENAME", "AGENT_ID", "ACCENUMB", "ACQDATE", "SAMPSTAT", "ORIGCTY", "DONORNUMB", "DONORCODE", "GENUS", "COORDUNCERT", "DECLATITUDE", "DECLONGITUDE", "INSTCODE", "ANCEST", "SPECIES", "SPAUTHOR", "STORAGE", "SUBTAXON", "SUBTAUTHOR" FROM V006_ACCESSION_BRAPI"""
                sql += f""" WHERE "ID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    germplasm = {
                        'commonCropName': result[0],
                        'germplasmDbId': str(result[1]),
                        'germplasmName': result[2],
                        'germplasmPUI': result[3],
                        'accessionNumber': result[4],
                        'acquisitionDate': result[5],
                        'countryOfOriginCode': result[7],
                        'defaultDisplayName': result[2],
                        'donors': [],
                        'genus': result[10],
                        'instituteCode': result[14],
                        'pedigree': result[15],
                        'species': result[16],
                        'speciesAuthority': result[17],
                        'storageTypes': [],
                        'subtaxa': result[19],
                        'subtaxaAuthority': result[20],
                    }
                    if result[6]:
                        germplasm['biologicalStatusOfAccessionCode'] = str(result[6])
                        germplasm['biologicalStatusOfAccessionDescription'] = FAO_SAMPSTAT_CODES[result[6]]
                    else:
                        germplasm['biologicalStatusOfAccessionCode'] = None
                        germplasm['biologicalStatusOfAccessionDescription'] = None
                    {"donorAccessionNumber": result[8], "donorInstituteCode": result[9]}
                    if result[8] or result[9]:
                        germplasm['donors'].append({"donorAccessionNumber": result[8], "donorInstituteCode": result[9]})
                    if result[11] and result[12] and result[13]:
                        germplasm['germplasmOrigin'] = [{
                            "coordinateUncertainty": result[11], 
                            "coordinates": {
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [float(result[12]), float(result[13])],
                                }, 
                                "type": "Feature"
                            }
                        }]
                    else:
                        germplasm['germplasmOrigin'] = []
                    if result[18]:
                        for i in result[18].split(";"):
                            germplasm['storageTypes'].append({"code": i, "description": FAO_STORAGE_CODES[i]})
                else:
                    germplasm = None
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        germplasm = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        germplasm = None

    if germplasm:
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
            "result": germplasm
        }), 200
    else:
        return jsonify("sample not found!"), 404

@brapi_bp.route('attributes')
def get_attributes():
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
            if is_number(value):
                 where_clause += f'"{key.upper()}" = \'{value}\'' 
            else:
                where_clause +=  f'"{key}" = \'{value}\''

    attributes = []
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}") as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V010_TRAIT_ATTRIBUTE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "ATTRIBUTEDBID", "ATTRIBUTENAME", "METHOD", "TRAIT", "ATTRIBUTECATEGORY", "ATTRIBUTEDESCRIPTION" FROM V010_TRAIT_ATTRIBUTE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "ATTRIBUTEDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    attribute = {
                        'attributeDbId': r[0], 
                        'attributeName': r[1], 
                        'method': r[2], 
                        'trait': r[3], 
                        'attributeCategory': r[4], 
                        'attributeDescription': r[5]
                    }
                    attributes.append(attribute)
    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        attributes = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        attributes = []

    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": attributes
        }
    })

@brapi_bp.route('/attributes/<reference_id>')
def get_attribute_by_reference_id(reference_id):
    attribute = None
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "ATTRIBUTEDBID", "ATTRIBUTENAME", "METHOD", "TRAIT", "ATTRIBUTECATEGORY", "ATTRIBUTEDESCRIPTION" FROM V010_TRAIT_ATTRIBUTE_BRAPI"""
                sql += f""" WHERE "ATTRIBUTEDBID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    attribute = {
                        'attributeDbId': str(result[0]), 
                        'attributeName': str(result[1]), 
                        'method': str(result[2]), 
                        'trait': result[3], 
                        'attributeCategory': result[4], 
                        'attributeDescription': result[5]
                    }
                else:
                    attribute = None
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        attribute = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        attribute = None

    if attribute:
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
            "result": attribute
        }), 200
    else:
        return jsonify("attribute not found!"), 404
    
@brapi_bp.route('attributevalues')
def get_attributevalues():
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
            if is_number(value):
                 where_clause += f'"{key.upper()}" = \'{value}\''
            else:
                where_clause +=  f'"{key}" = \'{value}\''

    attributevalues = []
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}") as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V011_TRAIT_VALUE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "ATTRIBUTENAME", "ATTRIBUTEVALUEDBID", "ADDITIONALINFO", "ATTRIBUTEDBID", "DETERMINEDDATE", "GERMPLASMDBID", "GERMPLASMNAME", "VALUE" FROM V011_TRAIT_VALUE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "ATTRIBUTEVALUEDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    attributevalue = {
                        'attributeName': r[0], 
                        'attributeValueDbId': str(r[1]), 
                        'additionalInfo': r[2], 
                        'attributeDbId': str(r[3]), 
                        'determinedDate': r[4], 
                        'germplasmDbId': str(r[5]), 
                        'germplasmName': r[6], 
                        'value': r[7]
                    }
                    attributevalues.append(attributevalue)
    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        attributevalues = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        attributevalues = []

    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": attributevalues
        }
    })

@brapi_bp.route('/attributevalues/<reference_id>')
def get_attributevalue_by_reference_id(reference_id):
    attributevalue = None
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "ATTRIBUTENAME", "ATTRIBUTEVALUEDBID", "ADDITIONALINFO", "ATTRIBUTEDBID", "DETERMINEDDATE", "GERMPLASMDBID", "GERMPLASMNAME", "VALUE" FROM V011_TRAIT_VALUE_BRAPI""" 
                sql += f""" WHERE "ATTRIBUTEVALUEDBID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    attributevalue = {
                        'attributeName': result[0], 
                        'attributeValueDbId': str(result[1]), 
                        'additionalInfo': result[2], 
                        'attributeDbId': str(result[3]), 
                        'determinedDate': str(result[4]), 
                        'germplasmDbId': str(result[5]), 
                        'germplasmName': result[6], 
                        'value': result[7]
                    }
                else:
                    attributevalue = None
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        attributevalue = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        attributevalue = None
        
    
    if attributevalue:
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
            "result": attributevalue
        }), 200
    else:
        return jsonify("attributevalue not found!"), 404

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
                 where_clause += f'UPPER(TRIM("samplePUI")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''

    callSets = []
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM mv_brapi_samples"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "samplePUI", "sampleDbId", "sampleTimestamp", "sampleName" AS "callSetName", "studyDbId" FROM mv_brapi_samples"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "samplePUI" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    callSet = {
                        'callSetDbId': str(r[0]), # Mapping samplePUI to callSetDbId
                        'callSetName': r[3],
                        'sampleDbId': str(r[1]),
                        'studyDbId': r[4],
                        'created': r[2],
                        'updated': r[2],  # Assuming the sampleTimestamp for both created and updated dates
                        'additionalInfo': {},  # If there are additional attributes to include
                        'externalReferences': [],  # Assuming how to handle external references if available
                        'variantSetDbIds': []  # Placeholder if there's relevant data to link
                    }
                    callSets.append(callSet)

    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        callSets = []
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        callSets = []
    
    res_total_pages = math.ceil(res_total_count / res_page_size)

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
            "data": callSets
        }
    })

@brapi_bp.route('/callsets/<reference_id>')
def get_callset_by_reference_id(reference_id):
    callSet = None
    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "samplePUI", "studyDbId", "sampleDbId", "sampleName" AS "callSetName", "sampleTimestamp" FROM mv_brapi_samples"""
                sql += f""" WHERE "samplePUI" = '{reference_id}' """
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    callSet = {
                        'callSetDbId': str(result[0]),
                        'callSetName': result[3],
                        'sampleDbId': str(result[1]),
                        'studyDbId': result[4],
                        'created': result[2],
                        'updated': result[2],
                        'additionalInfo': {},
                        'externalReferences': [],
                        'variantSetDbIds': []
                    }
                else:
                    callSet = None

    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return empty list on database error
        callSet = None
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return empty list on generic error
        callSet = None

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
    

