# Project setup - Branch DEV

## 1. Create a virtual env on linux system
`virtualenv -p /usr/bin/python3.9 .agent-env`

## 2. Activate venv
`source .agent-env/bin/activate`

## 3. Install depencies
`pip install -r requirements.txt`

## 4. Set the environment values for the database connection
 - rename .env-template to .env
 - set the values like DB_HOST, DB_PORT, DB_SERVICE_NAME, DB_USER, DB_PASSWORD
