# Project setup - Branch DEV

## 1. Create a virtual env on linux system
`virtualenv .agent-env`

## 2. Activate venv
### Linux
`source .agent-env/bin/activate`
### Windows
`source .agent-env/Script/activate`
## 3. Install dependecies
`pip install -r requirements.txt`

## 4. Set the environment values for the database connection
 - rename .env-template to .env
 - set the values like DB_HOST, DB_PORT, DB_SERVICE_NAME, DB_USER, DB_PASSWORD

## 5. Start project
`python main.py` or `python3 main.py`
