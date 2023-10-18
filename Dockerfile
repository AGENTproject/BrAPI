FROM python:3.11
WORKDIR /app
RUN apt upgrade
RUN apt install gcc
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8080
ENV FLASK_APP=main.py
CMD ["flask", "run","--host","0.0.0.0","--port","8080"]
