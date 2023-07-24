# Project setup - Branch DEV

Please note: the Python major version for below task has to match. I.e You may use virtual-env3.6 in combination with python3.9. But virtual-env2.7 is not compatible to use. Thus it is not recommend to use virtualenv or phython in command line without version.
The recommend version is Python 3.9

## 1. Create a virtual env on linux system
`virtualenv-<version> -p /usr/bin/python3.9 .agent-env`

## 2. Activate venv
`source .agent-env/bin/activate`

## 3. Install depencies
`pip<version> install -r requirements.txt`

## 4. Set the environment values for the database connection
 - rename .env-template to .env
 - set the values like DB_HOST, DB_PORT, DB_SERVICE_NAME, DB_USER, DB_PASSWORD

## 5. Start project
`python<version> main.py`
