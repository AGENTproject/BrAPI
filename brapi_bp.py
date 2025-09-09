from flask import Blueprint, render_template, jsonify
import oracledb
from flask import request
import math
import os
from shared import res_context, res_datafiles, res_status 
import platform

brapi_bp = Blueprint('brapi_bp', __name__, url_prefix='/genotyping/brapi/v2')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

d = None                               # On Linux, no directory should be passed
if platform.system() == "Darwin":      # macOS
        d = os.environ.get("HOME")+("/Downloads/instantclient_23_7")
elif platform.system() == "Windows":   # Windows
    d = r"C:\oracle\instantclient_23_7"
oracledb.init_oracle_client(lib_dir=d)


# Create a connection pool
pool = oracledb.create_pool(
    user=DB_USER,
    password=DB_PASSWORD,
    dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}",
    min=2,  # Minimum number of connections in the pool
    max=10,  # Maximum number of connections in the pool
    increment=1  # Number of connections to open at a time if needed#
)


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
                    "service": "samples/{sampleDbId}",
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
                    "service": "callsets/{callSetDbId}",
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
                    "service": "attributes/{attributeDbId}",
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
                    "service": "attributevalues/{attributeValueDbId}",
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
                    "service": "germplasm/{germplasmDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "studies",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "studies/{studyDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "methods",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "methods/{methodDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "observationunits",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "observationunits/{observationUnitDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "variables",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "variables/{observationVariableDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "observations",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "observations/{observationDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "scales",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "scales/{scaleDbId}",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "traits",
                    "versions": ["2.1"]
                },
                {
                    "contentTypes": ["application/json"],
                    "dataTypes": ["application/json"],
                    "methods": ["GET", ],
                    "service": "traits/{traitDbId}",
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
        
def is_int(s):
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

def chunk_list(lst, chunk_size):
    """Yield successive chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def handle_non_numeric_ids(db_id):
    # certain database ids should be numeric or they are invalid and should be converted to None
    if db_id.isnumeric():
        return (db_id)
    else:
        return (None)
    

@brapi_bp.route('samples')
def get_samples():

    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))

    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0
    
    # Construct the WHERE clause and bind variables
    where_clause = ""
    bind_variables = {}
    
    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)
    
    # Build the WHERE clause with bind variables
    for key, value in query_parameters.items():
        if key not in ['PAGESIZE', 'CURRENTPAGE', 'PAGE']: #Exclude pagination keys
            if where_clause:
                where_clause += " AND "
            if is_number(value):                      # Handle numeric value
                where_clause += f'"{key}" = :{key}'   # Use bind variables
                bind_variables[key] = int(value)      # Bind numeric value 
            else:
                where_clause += f'"{key}" = :{key}'   # Use bind variables
                bind_variables[key] = value           # Assign bind variable values

     # Base SQL query without pagination
    base_sql = f"""
                SELECT "additionalInfo" AS "ADDITIONALINFO", "column" AS "COLUMN", "externalReferences" AS "EXTERNALREFERENCES", "germplasmDbId" AS "GERMPLASMDBID", 
                       "observationUnitDbId" AS "OBSERVATIONUNITDBID", "plateDbId" AS "PLATEDBID", "plateName" AS "PLATENAME", "programDbId" AS "PROGRAMDBID", "row" AS "ROW", 
                       "sampleBarcode" AS "SAMPLEBARCODE", "sampleDbId" AS "SAMPLEDBID", "sampleDescription" AS "SAMPLEDESCRIPTION", "sampleGroupDbId" AS "SAMPLEGROUPDBID", 
                       "sampleName" AS "SAMPLENAME", "samplePUI" AS "SAMPLEPUI", "sampleTimestamp" AS "SAMPLETIMESTAMP", "sampleType" AS "SAMPLETYPE", "studyDbId" AS "STUDYDBID", 
                       "takenBy" AS "TAKENBY", "tissueType" AS "TISSUETYPE", "trialDbId" AS "TRIALDBID", "well" AS "WELL"
                FROM MV_BRAPI_SAMPLES
                """
    
    # SQL for counting total rows
    count_sql = f"SELECT COUNT(*) FROM (" + base_sql + ")"
    if where_clause:
        count_sql += f" WHERE {where_clause}"


    samples = []
    total_count = 0

    # Connect to the Oracle database
    try:
        with pool.acquire() as connection:  # Acquire a connection from the pool
        #with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}") as connection:
            with connection.cursor() as cursor:
                
                
                #excute the query with the bind_variables dictionary
                cursor.execute(count_sql, bind_variables)
                total_count = cursor.fetchone()[0]  # Get the total count from the first row

                # Prepare the SQL statement for paginated results
                paginated_sql = f"SELECT * FROM (" + base_sql + ")"
                if where_clause:
                    paginated_sql += f" WHERE {where_clause}"

                # Add OFFSET and LIMIT for pagination
                paginated_sql += f""" ORDER BY "SAMPLEDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                                
                 #Execute the paginated SQL statement
                cursor.execute(paginated_sql, bind_variables)

                # Fetch all rows at once
                rows = cursor.fetchall()

                # Process the results in the loop, no further SQL execution inside
                for r in rows:
                    sample = {
                        'additionalInfo':handle_lob(r[0]),  # Handle LOB
                        'column': r[1], 'externalReferences': [{"referenceId": handle_lob(r[2]), "referenceSource": ""}], 'germplasmDbId': str(r[3]), 'observationUnitDbId': handle_non_numeric_ids(str(r[4])), 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': str(r[10]), 'sampleDescription': handle_lob(r[11]), 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': handle_lob(r[14]), 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': handle_non_numeric_ids(str(r[17])), 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]
                    }
                    samples.append(sample)

    except oracledb.DatabaseError as e:
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    # Calculate total pages
    total_pages = math.ceil(total_count / res_page_size)

    return jsonify({
        "@context": res_context,
        "metadata": {
            "datafiles": res_datafiles,
            "status": res_status,
            "pagination": {
                "pageSize": res_page_size,
                "totalCount":total_count,  # Remove this line to eliminate the totalCount entry
                "totalPages": total_pages,
                "currentPage": res_current_page
            }
        },
        "result": {
            "data": samples
        }
    })


@brapi_bp.route('germplasm')
def get_germplasm():
    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))
    
    # if BIOLOGICALSTATUSOFACCESSIONDESCRIPTION is in keys, convert to BIOLOGICALSTATUSOFACCESSIONCODE using the lookup table
    if "BIOLOGICALSTATUSOFACCESSIONDESCRIPTION" in list(query_parameters.keys()):
        bio_status_desc = query_parameters["BIOLOGICALSTATUSOFACCESSIONDESCRIPTION"]
        if bio_status_desc in FAO_SAMPSTAT_CODES.values():
            bio_status_code = list(FAO_SAMPSTAT_CODES.keys())[list(FAO_SAMPSTAT_CODES.values()).index(bio_status_desc)]
        else:
            bio_status_code = -1
        del query_parameters["BIOLOGICALSTATUSOFACCESSIONDESCRIPTION"]
        query_parameters["BIOLOGICALSTATUSOFACCESSIONCODE"] = bio_status_code
    
    res_context = None
    res_datafiles = []
    res_status = []

    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0

    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)

    # Construct the WHERE clause and bind variables based on query parameters
    where_clause = ""
    bind_variables = {}

    COLUMN_MAP = {
        "COMMONCROPNAME": "CROPNAME",
        "ACCESSIONNUMBER":"ACCENUMB",
        "BIOLOGICALSTATUSOFACCESSIONCODE":"SAMPSTAT",
        "DEFAULTDISPLAYNAME":"ACCENAME",
        "SPECIESAUTHORITY":"SPAUTHOR",
        "INSTITUTECODE":"INSTCODE",
        "PEDIGREE":"ANCEST",
        "COUNTRYOFORIGINCODE":"ORIGCTY",
        "GERMPLASMDBID":"AGENT_ID"
    }

    for key, value in query_parameters.items():
        if key not in ['PAGESIZE', 'CURRENTPAGE', 'PAGE']:  # Exclude pagination keys
            column_name = COLUMN_MAP.get(key, key)  # Map to actual column or use the key directly
            if where_clause:
                where_clause += " AND "
            where_clause += f'"{column_name}" = :{key}'  # Use mapped column name
            bind_variables[key] = value  # Assign bind variable value

    print(where_clause)

    germplasms = []
    
    res_total_count = 0  # Initialize to prevent unbound errors
    try:
        # Use connection pooling
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                # Query to get the total count of rows
                count_sql = "SELECT COUNT(*) FROM V006_ACCESSION_BRAPI"
                if where_clause:
                    count_sql += f" WHERE {where_clause}"
                print(count_sql, bind_variables)
                cursor.execute(count_sql, bind_variables)
                res_total_count = cursor.fetchone()[0]  # Fetch the total count

            with connection.cursor() as cursor:
                # Query to fetch paginated germplasm data
                germplasm_sql = f"""
                    SELECT "CROPNAME", "ID", "ACCENAME", "AGENT_ID", "ACCENUMB", 
                           "ACQDATE", "SAMPSTAT", "ORIGCTY", "DONORNUMB", "DONORCODE", 
                           "GENUS", "COORDUNCERT", "DECLATITUDE", "DECLONGITUDE", 
                           "INSTCODE", "ANCEST", "SPECIES", "SPAUTHOR", "STORAGE", 
                           "SUBTAXON", "SUBTAUTHOR"
                    FROM V006_ACCESSION_BRAPI
                """
                if where_clause:
                    germplasm_sql += f" WHERE {where_clause}"
                
                germplasm_sql += f"""
                    ORDER BY "ID" 
                    OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
                """

                # Add offset and limit to bind variables
                bind_variables['offset'] = res_page_size * res_current_page
                bind_variables['limit'] = res_page_size

                # Execute the paginated germplasm query
                cursor.execute(germplasm_sql, bind_variables)

                for r in cursor.fetchall():
                    germplasm = {
                        'commonCropName': r[0],
                        'germplasmDbId': r[3],
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

                    # Biological status of accession
                    if r[6]:
                        germplasm['biologicalStatusOfAccessionCode'] = str(r[6])
                        germplasm['biologicalStatusOfAccessionDescription'] = FAO_SAMPSTAT_CODES.get(r[6], "")
                    else:
                        germplasm['biologicalStatusOfAccessionCode'] = None
                        germplasm['biologicalStatusOfAccessionDescription'] = None

                    # Donors
                    if r[8] or r[9]:
                        germplasm['donors'].append({"donorAccessionNumber": r[8], "donorInstituteCode": r[9]})

                    # Germplasm origin with coordinates
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

                    # Storage types
                    if r[18]:
                        for code in r[18].split(";"):
                            germplasm['storageTypes'].append({
                                "code": code,
                                "description": FAO_STORAGE_CODES.get(code, "")
                            })

                    germplasms.append(germplasm)

    except oracledb.DatabaseError as e:
        # Log the database error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log any other errors
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    # Calculate total pages
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
            "data": germplasms
        }
    })

    
import oracledb
from flask import request, jsonify
import math

@brapi_bp.route('/studies')
def get_studies():
    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))
    
    res_context = None
    res_datafiles = []
    res_status = []

    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0

    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)

    # Construct the WHERE clause based on query parameters
    where_clause = []
    bind_variables = {}

    for key, value in query_parameters.items():
        if key not in ['PAGESIZE', 'CURRENTPAGE', 'PAGE']:
            where_clause.append(f'"{key.upper()}" = :{key}')
            bind_variables[key] = value
    
    studies = []
    
    res_total_count = 0  # Initialize to 0 at the start

    try:
        # Using connection pooling for better performance
        with pool.acquire() as connection:
            # Get number of rows
            with connection.cursor() as cursor:
                sql = """SELECT COUNT(*) FROM V007_STUDY_BRAPI"""
                if where_clause:
                    sql += " WHERE " + " AND ".join(where_clause)
                cursor.execute(sql, bind_variables)
                res_total_count = cursor.fetchone()[0]
            
            # Get studies data
            with connection.cursor() as cursor:
                sql = """SELECT "STUDYDBID", "STUDYNAME", "ADDITIONALINFO", "COMMONCROPNAME", "ENDDATE", 
                         "LOCATIONNAME", "STARTDATE", "STUDYCODE", "STUDYDESCRIPTION" 
                         FROM V007_STUDY_BRAPI"""
                if where_clause:
                    sql += " WHERE " + " AND ".join(where_clause)
                
                # Add pagination to the SQL query
                sql += f""" ORDER BY "STUDYDBID" OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"""
                
                # Calculate the offset for pagination
                bind_variables['offset'] = res_page_size * res_current_page
                bind_variables['page_size'] = res_page_size
                
                cursor.execute(sql, bind_variables)
                for r in cursor.fetchall():
                    study = {
                        'studyDbId': str(r[0]),
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
        
               

        # After fetching studies, retrieve environment parameters and observation variables
        if studies:
            # Build where_clause for fetching environment parameters and observation variables
            study_db_ids = [study['studyDbId'] for study in studies]
            study_db_ids_chunks = list(chunk_list(study_db_ids, 1000))
            study_db_ids_chunks_str = []
            
            for chunk in study_db_ids_chunks:
                chunk_str = """"STUDYDBID" IN ({})""".format(', '.join(str(id) for id in chunk))
                study_db_ids_chunks_str.append(chunk_str)
                
            study_db_ids_str = ' OR '.join(str(id) for id in study_db_ids_chunks_str)
                
            
            # Fetch environment parameters
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql_env_params = """SELECT "STUDYDBID", "PARAMETERNAME", "VALUE" 
                                         FROM V008_ENVIRONMENT_PARAMETERS_BRAPI 
                                         WHERE {}""".format(study_db_ids_str)
                    
                    
                                         
                    
                    cursor.execute(sql_env_params)
                    for r in cursor.fetchall():
                        study_db_id = str(r[0])
                        environment_parameter = {
                            "parameterName": r[1],
                            "value": r[2]
                        }
                        
                        for study in studies:
                            
                            if study["studyDbId"] == study_db_id:
                                study['environmentParameters'].append(environment_parameter)

            # Fetch observation variables
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql_obs_vars = """SELECT "STUDYDBID", "OBSERVATIONVARIABLEDBID" 
                                      FROM V009_OBSERVATION_VARIABLE_BRAPI 
                                      WHERE {}""".format(study_db_ids_str)
                    cursor.execute(sql_obs_vars)
                    for r in cursor.fetchall():
                        study_db_id = str(r[0])
                        observation_variable_db_id = str(r[1])
                        for study in studies:
                            if study["studyDbId"] == study_db_id:
                                study['observationVariableDbIds'].append(observation_variable_db_id)
            
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    # Calculate number of pages
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
            "data": studies
        }
    })


@brapi_bp.route('/samples/<reference_id>')
def get_sample_by_reference_id(reference_id):
    # sampleDbId is a number field so return 404 for all non numeric searches
    if not reference_id.isnumeric():
        return jsonify("sample not found!"), 404
    sample = None
    try:
        # Use pooled connection
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                # Use bind variable for reference_id to prevent SQL injection
                sql = """
                    SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", 
                           "observationUnitDbId", "plateDbId", "plateName", "programDbId", 
                           "row", "sampleBarcode", "sampleDbId", "sampleDescription", 
                           "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", 
                           "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" 
                    FROM mv_brapi_samples
                    WHERE "sampleDbId" = :reference_id
                """
                print(sql)
                # Execute query with bind variable
                cursor.execute(sql, {'reference_id': reference_id})
                results = cursor.fetchall()

                if results:
                    result = results[0]
                    sample = {
                        'additionalInfo': handle_lob(result[0]), 
                        'column': result[1], 
                        'externalReferences': [{"referenceId": handle_lob(result[2]), "referenceSource": ""}], 
                        'germplasmDbId': handle_non_numeric_ids(str(result[3])), 
                        'observationUnitDbId': handle_non_numeric_ids(str(result[4])), 
                        'plateDbId': result[5], 
                        'plateName': result[6], 
                        'programDbId': result[7], 
                        'row': result[8], 
                        'sampleBarcode': result[9], 
                        'sampleDbId': handle_non_numeric_ids(str(result[10])),
                        'sampleDescription': handle_lob(result[11]),
                        'sampleGroupDbId': result[12],
                        'sampleName': result[13],
                        'samplePUI': handle_lob(result[14]),
                        'sampleTimestamp': result[15],
                        'sampleType': result[16],
                        'studyDbId': handle_non_numeric_ids(str(result[17])),
                        'takenBy': result[18],
                        'tissueType': result[19],
                        'trialDbId': handle_non_numeric_ids(str(result[20])),
                        'well': result[21]
                    }
    except oracledb.DatabaseError as e:
        # Log the database error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log any other error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    # Respond based on whether a sample was found
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
    # studyDbId is a number field so return 404 for all non numeric searches
    if not reference_id.isnumeric():
        return jsonify("study not found!"), 404
    study = None
    
    try:
        # Establish the database connection
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                # Ensure the placeholder is consistently named :studyDbId
                sql = """
                    SELECT "STUDYDBID", "STUDYNAME", "ADDITIONALINFO", "COMMONCROPNAME", 
                           "ENDDATE", "LOCATIONNAME", "STARTDATE", "STUDYCODE", "STUDYDESCRIPTION"
                    FROM V007_STUDY_BRAPI
                    WHERE "STUDYDBID" = :studyDbId
                """
                
                # Bind variables as a dictionary with the key matching the placeholder in the SQL query
                bind_variables = {'studyDbId': reference_id}  # Ensure 'studyDbId' is used consistently

                # Execute the query with the bind variables
                cursor.execute(sql, bind_variables)
                
                # Fetch the results
                results = cursor.fetchall()

                if len(results) > 0:
                    result = results[0]
                    study = {
                        'studyDbId': str(result[0]),
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
            # Use the same consistent placeholder for filtering environment parameters
            where_clause = '"STUDYDBID" = :studyDbId'
        
            # Get environment parameters
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql_env_params = f"""SELECT "PARAMETERNAME", "VALUE" 
                                          FROM V008_ENVIRONMENT_PARAMETERS_BRAPI 
                                          WHERE {where_clause}"""
                    cursor.execute(sql_env_params, {'studyDbId': study["studyDbId"]})
                    for r in cursor.fetchall():
                        environmentParameter = {
                            "parameterName": r[0], 
                            "value": r[1]
                        }
                        study['environmentParameters'].append(environmentParameter)
                
            # Get observation variables
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql_obs_vars = f"""SELECT "OBSERVATIONVARIABLEDBID" 
                                        FROM V009_OBSERVATION_VARIABLE_BRAPI 
                                        WHERE {where_clause}"""
                    cursor.execute(sql_obs_vars, {'studyDbId': study["studyDbId"]})
                    for r in cursor.fetchall():
                        observationVariableDbId = str(r[0])
                        study['observationVariableDbIds'].append(observationVariableDbId)

    except oracledb.DatabaseError as e:
        # Log any database errors
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    except Exception as e:
        # Log any general exceptions
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    # Return the result or error
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
        return jsonify("study not found!"), 404



@brapi_bp.route('/germplasm/<reference_id>')
def get_germplasm_by_reference_id(reference_id):
    germplasm = None
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT "CROPNAME", "ID", "ACCENAME", "AGENT_ID", "ACCENUMB", "ACQDATE", "SAMPSTAT", "ORIGCTY", "DONORNUMB", "DONORCODE", "GENUS", "COORDUNCERT", "DECLATITUDE", "DECLONGITUDE", "INSTCODE", "ANCEST", "SPECIES", "SPAUTHOR", "STORAGE", "SUBTAXON", "SUBTAUTHOR" FROM V006_ACCESSION_BRAPI"""
                sql += f""" WHERE "AGENT_ID" = '{reference_id}'"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    germplasm = {
                        'commonCropName': result[0],
                        'germplasmDbId': result[3],
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))
    
    res_context = None
    res_datafiles = []
    res_status = []

    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0

    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)

    # Construct the WHERE clause based on query parameters
    where_clause = ""
    for key, value in query_parameters.items():
        if key != 'PAGESIZE' and key != 'CURRENTPAGE' and key != 'PAGE':
            if where_clause:
                where_clause += " AND "
            if is_number(value):
                 where_clause += f'"{key}" = \'{value}\'' 
            else:
                where_clause +=  f'"{key}" = \'{value}\''

    attributes = []

    res_total_count =0
    try:
        with pool.acquire() as connection:
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
                        'attributeDbId': str(r[0]), 
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
    # attributeDbId is a number field so return 404 for all non numeric searches
    if not reference_id.isnumeric():
        return jsonify("attribute not found!"), 404
    sample = None
    attribute = None
    try:
        with pool.acquire() as connection:
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))
    
    res_context = None
    res_datafiles = []
    res_status = []

    
    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0

    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)

    # Construct the WHERE clause based on query parameters
    where_clause = ""
    for key, value in query_parameters.items():
        if key != 'PAGESIZE' and key != 'CURRENTPAGE' and key != 'page':
            if where_clause:
                where_clause += " AND "
            if is_number(value):
                 where_clause += f'"{key}" = \'{value}\''
            else:
                where_clause +=  f'"{key}" = \'{value}\''

    attributevalues = []
    try:
        with pool.acquire() as connection:
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
    # attributeDbId is a number field so return 404 for all non numeric searches
    if not reference_id.isnumeric():
        return jsonify("attribute not found!"), 404
    attributevalue = None
    try:
        with pool.acquire() as connection:
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
        
    
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
    query_parameters = request.args.to_dict()
    
    # convert all keys to upper case to ignore case sensitivity
    query_parameters = dict(map(lambda x: (x[0].upper(), x[1]), query_parameters.items()))
    
    
    if "callSetDbId" in list(query_parameters.keys()):
        callSetDbId = query_parameters["callSetDbId"]
        callSet = None
    
    res_context = None
    res_datafiles = []
    res_status = []
    
    # set default pagination variables
    res_page_size = 1000
    res_current_page = 0
    
    # Get page size and page number from query parameters
    if "PAGESIZE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGESIZE"])):
            res_page_size = max(int(query_parameters["PAGESIZE"]), 1)
    if "CURRENTPAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["CURRENTPAGE"])):
            res_current_page = max(int(query_parameters["CURRENTPAGE"]), res_current_page)
    if "PAGE" in list(query_parameters.keys()):
        if is_int((query_parameters["PAGE"])):
            res_current_page = max(int(query_parameters["PAGE"]), res_current_page)


    # Construct the WHERE clause based on query parameters
    where_clause = ""
    for key, value in query_parameters.items():
        if key not in ['PAGESIZE', 'CURRENTPAGE', 'PAGE']:
            if where_clause:
                where_clause += " AND "
            if key == 'CALLSETDBID':
                 where_clause += f'UPPER(TRIM("SAMPLEPUI")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''

    print(where_clause)

    callSets = []
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM (SELECT "samplePUI" AS "SAMPLEPUI", "sampleDbId" AS "SAMPLEDBID", "sampleTimestamp" AS "SAMPLETIMESTAMP", "sampleName" AS "CALLSETNAME", "studyDbId" AS "STUDYDBID" FROM MV_BRAPI_SAMPLES)"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT * FROM (SELECT "samplePUI" AS "SAMPLEPUI", "sampleDbId" AS "SAMPLEDBID", "sampleTimestamp" AS "SAMPLETIMESTAMP", "sampleName" AS "CALLSETNAME", "studyDbId" AS "STUDYDBID" FROM MV_BRAPI_SAMPLES)"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "SAMPLEPUI" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    callSet = {
                        'callSetDbId': str(r[0]), # Mapping samplePUI to callSetDbId
                        'callSetName': r[3],
                        'sampleDbId': handle_non_numeric_ids(str(r[1])),
                        'studyDbId': handle_non_numeric_ids(str(r[4])),
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    
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
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "samplePUI", "sampleDbId", "sampleTimestamp", "sampleName" AS "callSetName", "studyDbId" FROM mv_brapi_samples"""
                sql += f""" WHERE "samplePUI" = '{reference_id}' """
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    callSet = {
                        'callSetDbId': str(result[0]),
                        'callSetName': result[3],
                        'sampleDbId': handle_non_numeric_ids(str(result[1])),
                        'studyDbId': handle_non_numeric_ids(str(result[4])),
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
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
    
@brapi_bp.route('/scales')
def get_scales():
    scaleDbId = request.args.get('scaleDbId')
    if scaleDbId:
        scale = None
    
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
            if key == 'scaleDbId':
                 where_clause += f'UPPER(TRIM("SCALEDBID")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''

    scales = []
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V017_SCALE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "SCALEDBID", "SCALENAME" FROM V017_SCALE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "SCALEDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    scale = {
                        'scaleDbId': r[0],
                        'scaleName': r[1],
                    }
                    scales.append(scale)

    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    
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
            "data": scales
        }
    })

@brapi_bp.route('/scales/<reference_id>')
def get_scale_by_reference_id(reference_id):
    scale = None
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "SCALEDBID", "SCALENAME" FROM V017_SCALE_BRAPI"""
                sql += f""" WHERE "SCALEDBID" = '{reference_id}' """
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    scale = {
                        'scaleDbId': result[0],
                        'scaleName': result[1],
                    }
                else:
                    scale = None

    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if scale:
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
            "result": scale
        }), 200
    else:
        return jsonify("Scale not found!"), 404
    
@brapi_bp.route('/methods')
def get_methods():
    methodDbId = request.args.get('methodDbId')
    if methodDbId:
        method = None
    
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
            if key == 'methodDbId':
                 where_clause += f'UPPER(TRIM("METHODDBID")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''

    methods = []
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V016_METHODS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "METHODDBID", "METHODNAME", "BIBLIOGRAPHICALREFERENCE", "DESCRIPTION" FROM V016_METHODS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "METHODDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    method = {
                        'methodDbId': r[0],
                        'methodName': r[1],
                        'bibliographicalReference': r[2],
                        'description': r[3],
                    }
                    methods.append(method)

    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    
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
            "data": methods
        }
    })

@brapi_bp.route('/methods/<reference_id>')
def get_method_by_reference_id(reference_id):
    method = None
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "METHODDBID", "METHODNAME", "BIBLIOGRAPHICALREFERENCE", "DESCRIPTION" FROM V016_METHODS_BRAPI"""
                sql += f""" WHERE "METHODDBID" = '{reference_id}' """
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    method = {
                        'methodDbId': result[0],
                        'methodName': result[1],
                        'bibliographicalReference': result[2],
                        'description': result[3],
                    }
                else:
                    method = None

    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if method:
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
            "result": method
        }), 200
    else:
        return jsonify("Method not found!"), 404
        
        
@brapi_bp.route('/traits')
def get_traits():
    traitDbId = request.args.get('traitDbId')
    if traitDbId:
        trait = None
    
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
            if key == 'traitDbId':
                 where_clause += f'UPPER(TRIM("TRAITDBID")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''

    traits = []
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V015_TRAITS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "TRAITDBID", "TRAITNAME", "ADDITIONALINFO", "MAINABBREVIATION", "STATUS", "TRAITDESCRIPTION" FROM V015_TRAITS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "TRAITDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    trait = {
                        'traitDbId': str(r[0]),
                        'traitName': r[1],
                        'additionalInfo': r[2],
                        'mainAbbreviation': r[3],
                        'status': r[4],
                        'traitDescription': r[5],
                    }
                    traits.append(trait)

    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    
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
            "data": traits
        }
    })

@brapi_bp.route('/traits/<reference_id>')
def get_trait_by_reference_id(reference_id):
    trait = None
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "TRAITDBID", "TRAITNAME", "ADDITIONALINFO", "MAINABBREVIATION", "STATUS", "TRAITDESCRIPTION" FROM V015_TRAITS_BRAPI"""
                sql += f""" WHERE "TRAITDBID" = '{reference_id}' """
                print(sql)
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    trait = {
                        'traitDbId': str(result[0]),
                        'traitName': result[1],
                        'additionalInfo': result[2],
                        'mainAbbreviation': result[3],
                        'status': result[4],
                        'traitDescription': result[5],
                    }
                else:
                    trait = None

    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if trait:
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
            "result": trait
        }), 200
    else:
        return jsonify("Trait not found!"), 404
        
        
@brapi_bp.route('variables')
def get_variables():
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
    
    variables = []

    try:
        with pool.acquire() as connection:
            # Get number of rows
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V014_VARIABLE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            # Get variables data
            with connection.cursor() as cursor:
                sql = f"""SELECT "OBSERVATIONVARIABLEDBID", "OBSERVATIONVARIABLENAME", "ADDITIONALINFO", "COMMONCROPNAME", "STATUS" FROM V014_VARIABLE_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                print (sql)
                sql += f""" ORDER BY "OBSERVATIONVARIABLEDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                for r in cursor.execute(sql):
                    variable = {
                        'method': {}, 
                        'observationVariableDbId': r[0], 
                        'observationVariableName': r[1], 
                        'scale': {}, 
                        'trait': {}, 
                        'additionalInfo': r[2], 
                        'commonCropName': r[3], 
                        'status': r[4], 
                    }
                    variables.append(variable)
        
        if variables:
            # Build where clause for filtering by paginated data
            where_clause = ""
            for variable in variables:
                if where_clause:
                    where_clause += " OR "
                where_clause += f'"METHODDBID" = \'{variable["observationVariableDbId"]}\''
                
            # For every variable in paginated data get method
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "METHODDBID", "METHODNAME", "BIBLIOGRAPHICALREFERENCE", "DESCRIPTION" FROM V016_METHODS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        method = {
                            "methodDbId": r[0], 
                            "methodName": r[1],
                            "bibliographicalReference": r[2],
                            "description": r[3],
                        }
                        # TODO what happends if there is more thean one method found?
                        for variable in variables:
                            if variable["observationVariableDbId"] == method["methodDbId"]:
                                variable['method'] = method
                                
            
            # Build where clause for filtering by paginated data
            where_clause = ""
            for variable in variables:
                if where_clause:
                    where_clause += " OR "
                where_clause += f'"SCALEDBID" = \'{variable["observationVariableDbId"]}\''
                
            # For every variable in paginated data get scale
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "SCALEDBID", "SCALENAME" FROM V017_SCALE_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        scale = {
                            "scaleDbId": r[0], 
                            "scaleName": r[1],
                        }
                        # TODO what happends if there is more thean one scale found?
                        for variable in variables:
                            if variable["observationVariableDbId"] == scale["scaleDbId"]:
                                variable['scale'] = scale
                                
            
            # Build where clause for filtering by paginated data
            where_clause = ""
            for variable in variables:
                if where_clause:
                    where_clause += " OR "
                where_clause += f'"TRAITDBID" = \'{variable["observationVariableDbId"]}\''
                
            # For every variable in paginated data get trait
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "TRAITDBID","TRAITNAME","ADDITIONALINFO","MAINABBREVIATION","STATUS","TRAITDESCRIPTION" FROM V015_TRAITS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        trait = {
                            "traitDbId": r[0], 
                            "traitName": r[1],
                            "additionalInfo": r[2],
                            "mainAbbreviation": r[3],
                            "status": r[4],
                            "traitDescription": r[5],
                        }
                        # TODO what happends if there is more thean one trait found?
                        for variable in variables:
                            if variable["observationVariableDbId"] == trait["traitDbId"]:
                                variable['trait'] = trait
                
                            
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
            "data": variables
        }
    })
    
    
@brapi_bp.route('/variables/<reference_id>')
def get_variable_by_reference_id(reference_id):
    variable = None
    
    where_clause = f'"VARIABLEDBID" = \'{reference_id}\''
    
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "OBSERVATIONVARIABLEDBID","OBSERVATIONVARIABLENAME","ADDITIONALINFO","COMMONCROPNAME","STATUS" FROM V014_VARIABLE_BRAPI"""
                sql += f""" WHERE "OBSERVATIONVARIABLEDBID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    variable = {
                        'method': {}, 
                        'observationVariableDbId': result[0], 
                        'observationVariableName': result[1], 
                        'scale': {}, 
                        'trait': {}, 
                        'additionalInfo': result[2], 
                        'commonCropName': result[3], 
                        'status': result[4], 
                    }
                else:
                    variable = None
        
        if variable:
            # Build where clause for filtering by paginated data
            where_clause = f'"METHODDBID" = \'{variable["observationVariableDbId"]}\''
        
            # For every variable in paginated data get method
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "METHODDBID", "METHODNAME", "BIBLIOGRAPHICALREFERENCE", "DESCRIPTION" FROM V016_METHODS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        method = {
                            "methodDbId": r[0], 
                            "methodName": r[1],
                            "bibliographicalReference": r[2],
                            "description": r[3],
                        }
                        # TODO what happends if there is more thean one method found?
                        variable['method'] = method
            
            # Build where clause for filtering by paginated data
            where_clause = f'"SCALEDBID" = \'{variable["observationVariableDbId"]}\''
        
            # For every variable in paginated data get scale
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "SCALEDBID","SCALENAME" FROM V017_SCALE_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        scale = {
                            "scaleDbId": r[0], 
                            "scaleName": r[1],
                        }
                        # TODO what happends if there is more than one scale found?
                        variable['scale'] = scale
            
            # Build where clause for filtering by paginated data
            where_clause = f'"TRAITDBID" = \'{variable["observationVariableDbId"]}\''
        
            # For every variable in paginated data get trait
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "TRAITDBID","TRAITNAME","ADDITIONALINFO","MAINABBREVIATION","STATUS","TRAITDESCRIPTION" FROM V015_TRAITS_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print (sql)
                    for r in cursor.execute(sql):
                        trait = {
                            "traitDbId": r[0], 
                            "traitName": r[1],
                            "additionalInfo": r[2],
                            "mainAbbreviation": r[3],
                            "status": r[4],
                            "traitDescription": r[5],
                        }
                        # TODO what happends if there is more than one trait found?
                        variable['trait'] = trait
        
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if variable:
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
            "result": variable
        }), 200
    else:
        return jsonify("Variable not found!"), 404
        
@brapi_bp.route('/observations')
def get_observations():
    observationDbId = request.args.get('observationDbId')
    if observationDbId:
        observation = None
    
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
            if key == 'observationDbId':
                 where_clause += f'UPPER(TRIM("OBSERVATIONUNITDBID")) = UPPER(TRIM(\'{value}\'))'
            else:
                 where_clause += f'"{key}" = \'{value}\''
    
    observations = []
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V012_OBSERVATION_UNITS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            with connection.cursor() as cursor:
                sql = f"""SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","OBSERVATIONTIMESTAMP","OBSERVATIONVARIABLEDBID","OBSERVATIONVARIABLENAME","STUDYDBID","UPLOADEDBY","VALUE" FROM V013_OBSERVATION_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                sql += f""" ORDER BY "OBSERVATIONUNITDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                cursor.execute(sql)
                for r in cursor.fetchall():
                    observation = {
                        'observationDbId': r[0],
                        'additionalInfo': r[1],
                        'germplasmDbId': r[2],
                        'observationTimeStamp': r[3],
                        'observationUnitDbId': r[0],
                        'observationVariableDbId': r[4],
                        'observationVariableName': r[5],
                        'studyDbId': r[6],
                        'uploadedBy': r[7],
                        'value': r[8],
                    }
                    observations.append(observation)

    except oracledb.DatabaseError as e:
         # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    
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
            "data": observations
        }
    })

@brapi_bp.route('/observations/<reference_id>')
def get_observation_by_reference_id(reference_id):
    observation = None
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","OBSERVATIONTIMESTAMP","OBSERVATIONVARIABLEDBID","OBSERVATIONVARIABLENAME","STUDYDBID","UPLOADEDBY","VALUE" FROM V013_OBSERVATION_BRAPI"""
                sql += f""" WHERE "OBSERVATIONUNITDBID" = '{reference_id}' """
                print(sql)
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    observation = {
                        'observationDbId': result[0],
                        'additionalInfo': result[1],
                        'germplasmDbId': result[2],
                        'observationTimeStamp': result[3],
                        'observationUnitDbId': result[0],
                        'observationVariableDbId': result[4],
                        'observationVariableName': result[5],
                        'studyDbId': result[6],
                        'uploadedBy': result[7],
                        'value': result[8],
                    }
                else:
                    observation = None

    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if observation:
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
            "result": observation
        }), 200
    else:
        return jsonify("Observation not found!"), 404
        
        
@brapi_bp.route('observationunits')
def get_observationunits():
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
    
    observationunits = []
    
    try:
        with pool.acquire() as connection:
            # Get number of rows
            with connection.cursor() as cursor:
                sql = f"""SELECT COUNT(*) FROM V012_OBSERVATION_UNITS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                cursor.execute(sql)
                res_total_count = cursor.fetchall()[0][0]
            # Get observationunits data
            with connection.cursor() as cursor:
                sql = f"""SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","STUDYDBID","STUDYNAME" FROM V012_OBSERVATION_UNITS_BRAPI"""
                if where_clause:
                    sql += f" WHERE {where_clause}"
                print (sql)
                sql += f""" ORDER BY "OBSERVATIONUNITDBID" OFFSET {res_page_size * res_current_page} ROWS FETCH NEXT {res_page_size} ROWS ONLY"""
                for r in cursor.execute(sql):
                    observationunit = {
                        'observationUnitDbId': r[0], 
                        'additionalInfo': r[1], 
                        'germplasmDbId': r[2], 
                        'observations': [], 
                        'studyDbId': r[3], 
                        'studyName': r[4], 
                    }
                    observationunits.append(observationunit)
        if observationunits:
            # Build where clause for filtering by paginated data
            where_clause = ""
            for observationunit in observationunits:
                if where_clause:
                    where_clause += " OR "
                where_clause += f'"OBSERVATIONUNITDBID" = \'{observationunit["observationUnitDbId"]}\''
            
            # For every observationunit in paginated data get observations
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","OBSERVATIONTIMESTAMP","OBSERVATIONVARIABLEDBID","OBSERVATIONVARIABLENAME","STUDYDBID","UPLOADEDBY","VALUE" FROM V013_OBSERVATION_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    print(sql)
                    for r in cursor.execute(sql):
                        observationunitDbId = r[0]
                        observation = {
                            "observationDbId": r[0], 
                            "additionalInfo": r[1], 
                            "germplasmDbId": r[2], 
                            "observationTimeStamp": r[3], 
                            "observationUnitDbId": r[0], 
                            "observationVariableDbId": r[4], 
                            "observationVariableName": r[5], 
                            "studyDbId": r[6], 
                            "uploadedBy": r[7], 
                            "value": r[8], 
                        }
                        for observationunit in observationunits:
                            if observationunit["observationUnitDbId"] == observationunitDbId:
                                observationunit['observations'].append(observation)

                            
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

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
            "data": observationunits
        }
    })
    
    
@brapi_bp.route('/observationunits/<reference_id>')
def get_observationunit_by_reference_id(reference_id):
    observationunit = None
    
    where_clause = f'"OBSERVATIONUNITDBID" = \'{reference_id}\''
    
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                sql = """SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","STUDYDBID","STUDYNAME" FROM V012_OBSERVATION_UNITS_BRAPI"""
                sql += f""" WHERE "OBSERVATIONUNITDBID" = {reference_id}"""
                cursor.execute(sql)
                results = cursor.fetchall()
                if len(results) > 0:
                    result = results[0]
                    observationunit = {
                        'observationUnitDbId': result[0], 
                        'additionalInfo': result[1], 
                        'germplasmDbId': result[2], 
                        'observations': [], 
                        'studyDbId': result[3], 
                        'studyName': result[4], 
                    }
                else:
                    observationunit = None

        if observationunit:
            # Build where clause for filtering by paginated data
            where_clause = f'"OBSERVATIONUNITDBID" = \'{observationunit["observationUnitDbId"]}\''
        
            # For every observationunit in paginated data get observations
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    sql = f"""SELECT "OBSERVATIONUNITDBID","ADDITIONALINFO","GERMPLASMDBID","OBSERVATIONTIMESTAMP","OBSERVATIONVARIABLEDBID","OBSERVATIONVARIABLENAME","STUDYDBID","UPLOADEDBY","VALUE" FROM V013_OBSERVATION_BRAPI"""
                    sql += f" WHERE {where_clause}"
                    for r in cursor.execute(sql):
                        observation = {
                            "observationDbId": r[0], 
                            "additionalInfo": r[1], 
                            "germplasmDbId": r[2], 
                            "observationTimeStamp": r[3], 
                            "observationUnitDbId": r[0], 
                            "observationVariableDbId": r[4], 
                            "observationVariableName": r[5], 
                            "studyDbId": r[6], 
                            "uploadedBy": r[7], 
                            "value": r[8], 
                        }
                        observationunit['observations'].append(observation)
           
    except oracledb.DatabaseError as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"Database error: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500
    except Exception as e:
        # Log the error
        from flask import current_app as app
        app.logger.error(f"An error occurred: {e}")
        # Return internal server error 500
        return jsonify("500 Internal Server Error"), 500

    if observationunit:
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
            "result": observationunit
        }), 200
    else:
        return jsonify("Observation unit not found!"), 404