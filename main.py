#from .models import# 
from flask import Flask, render_template
import oracledb
import os

# load envinornment variables from .env file
from dotenv import load_dotenv
load_dotenv()


# create the app
app = Flask(__name__)


# Access environment variablesfor Oracle Database Connectivity
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/samples")
def get_samples():
    res_context = None
    res_datafiles = []
    res_status = []
    res_page_size = 10
    res_total_count = 0
    res_total_pages = 0
    res_current_page = 0
    samples = []
    total_count = 0
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
                samples.append(
                    {'additionalInfo': r[0], 'column': r[1], 'externalReferences': r[2], 'germplasmDbId': r[3], 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': r[10], 'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]})
    res_total_count = len(samples)
    
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


@app.route("/samples/<string:reference_id>")
def get_sample_by_reference(reference_id):
    sample = 'sample not found!'
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, service_name=DB_SERVICE_NAME) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT "additionalInfo", "column", "externalReferences", "germplasmDbId", "observationUnitDbId", "plateDbId", "plateName", "programDbId", "row", "sampleBarcode", "sampleDbId", "sampleDescription", "sampleGroupDbId", "sampleName", "samplePUI", "sampleTimestamp", "sampleType", "studyDbId", "takenBy", "tissueType", "trialDbId", "well" FROM mv_brapi_samples"""
            for r in cursor.execute(sql):
                if r[2] == reference_id:
                    sample = {'additionalInfo': r[0], 'column': r[1], 'externalReferences': r[2], 'germplasmDbId': r[3], 'observationUnitDbId': r[4], 'plateDbId': r[5], 'plateName': r[6], 'programDbId': r[7], 'row': r[8], 'sampleBarcode': r[9], 'sampleDbId': r[10],
                              'sampleDescription': r[11], 'sampleGroupDbId': r[12], 'sampleName': r[13], 'samplePUI': r[14], 'sampleTimestamp': r[15], 'sampleType': r[16], 'studyDbId': r[17], 'takenBy': r[18], 'tissueType': r[19], 'trialDbId': r[20], 'well': r[21]}
    return sample


if __name__ == "__main__":
    app.run(debug=True)
