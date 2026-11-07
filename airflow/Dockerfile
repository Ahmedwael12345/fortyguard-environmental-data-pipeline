FROM apache/airflow:2.10.5-python3.11

USER root

# Create an isolated Python environment for the project
RUN python -m venv /opt/airflow/project-venv \
    && chown -R airflow:root /opt/airflow/project-venv

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow

# Install project dependencies without modifying Airflow's environment
RUN /opt/airflow/project-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
