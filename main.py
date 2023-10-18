from flask import Flask
from brapi_bp import brapi_bp
from admin_bp import admin_bp


# load envinornment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# create the app
app = Flask(__name__)
app.debug = True

#Blueprints
app.register_blueprint(brapi_bp)
app.register_blueprint(admin_bp)



if __name__ == "__main__":
    app.run('localhost', 5000)
