FROM python:3.11-bookworm


# Install dependencies
RUN apt-get update && \
    apt-get install -y libaio1 unzip && \
    mkdir -p /opt/oracle
	
# Add Instant Client directly
COPY instantclient-basic-linux.x64-23.26.1.0.0.zip /opt/oracle/
RUN unzip /opt/oracle/instantclient-basic-linux.x64-23.26.1.0.0.zip -d /opt/oracle/ && \
    rm /opt/oracle/instantclient-basic-linux.x64-23.26.1.0.0.zip


# Environment variables for Oracle Instant Client
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_26
ENV ORACLE_HOME=/opt/oracle/instantclient_23_26
ENV PATH=$PATH:/opt/oracle/instantclient_23_26



WORKDIR /app
RUN apt upgrade -y
RUN apt install gcc
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8080
ENV FLASK_APP=main.py
CMD ["flask", "run","--host","0.0.0.0","--port","8080"]
