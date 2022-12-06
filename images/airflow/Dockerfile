FROM repo.bit.admin.ch:8444/${AIRFLOW_IMAGE_NAME:-apache/airflow:2.4.3}
COPY --from=repo.bit.admin.ch:8444/bit/ca-bundle:latest /certs/ /usr/local/share/ca-certificates/

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

USER root
RUN update-ca-certificates
USER airflow

COPY pip.conf /etc/pip.conf
COPY requirements.txt /opt/airflow

RUN set -uxe && \
    cd /opt/airflow && \
    pip install --no-deps -r requirements.txt