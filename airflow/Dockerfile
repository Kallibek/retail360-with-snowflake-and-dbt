# Use the official Apache Airflow image as the base
FROM apache/airflow:2.10.5

# Switch to root to install additional packages
USER airflow

# Install custom dependencies (adjust the list as needed)
RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-core \
    dbt-snowflake \
    names \
    pandas \
    faker \
    numpy

# (Optional) Copy custom config files if required
# COPY config/ /opt/airflow/config/

# Ensure proper file permissions and switch back to the airflow user
RUN chown -R airflow: /opt/airflow
