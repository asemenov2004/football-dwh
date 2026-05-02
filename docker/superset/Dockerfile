FROM apache/superset:4.0.2

USER root

COPY requirements.txt /tmp/superset-requirements.txt
RUN pip install --no-cache-dir -r /tmp/superset-requirements.txt

USER superset
