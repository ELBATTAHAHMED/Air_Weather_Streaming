FROM apache/superset:latest@sha256:7bdc8845cd0a303d3a96963b07d05419979800ea919736de553804360df121b8

USER root
RUN /app/docker/pip-install.sh --no-cache trino sqlalchemy-trino psycopg2-binary
USER superset
