# Andy Dockerfile edit
# Updated FROM apache/airflow:2.2.3
FROM apache/airflow:2.5.0-python3.10

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

ARG CLOUD_SDK_VERSION=410.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# ENV AIRFLOW_HOME=/opt/airflow

RUN apt-get update -qq && apt-get install vim -qqq


COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html





# Updated from CLOUD_SDK_VERSION=322.0.0


WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER ${AIRFLOW_UID}
