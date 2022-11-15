FROM apache/airflow:2.3.3
COPY requirements.txt .
# install dependency for running service
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir --compile -r requirements.txt
