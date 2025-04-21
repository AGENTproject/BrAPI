from flask import Flask
import os
import platform
import oracledb


# load envinornment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# create the app
app = Flask(__name__)
app.debug = True

from flask import Flask, redirect, url_for

@app.route('/favicon.ico')
def serve_favicon():
    return redirect(url_for('static', filename='favicon.ico'))

#Blueprints
with app.app_context():
    from brapi_bp import brapi_bp
    from admin_bp import admin_bp
    app.register_blueprint(brapi_bp)
    app.register_blueprint(admin_bp)
     
if __name__ == "__main__":    
    app.run('localhost', 5000)
