##########################################
#   Keplerfi Airflow Docker Container    #
##########################################
# - Build Instructions:
#    docker build --rm -t airflow .
#    docker build --rm -t airflow -f /path/to/Dockerfile .
# - Run Instructions:
#    docker run -t -i -P airflow /bin/bash
#

FROM python:3.7-slim-stretch
LABEL maintainer="will@keplerfi.com"

ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV name William Murphy


# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""

# Airflow Environment variable setup
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Copy over configuration requirements
#COPY src/config.sh /config.sh
COPY /hidden_data /usr/local/hidden_data
COPY /requirements.txt /usr/local/requirements.txt
COPY /airflow /usr/local/airflow
COPY /airflow/airflow.cfg  ${AIRFLOW_USER_HOME}/airflow.cfg

# Set up a Volume for logs
VOLUME /airflow/logs ${AIRFLOW_USER_HOME}/logs




# Run shell command to add the airflow user directory
RUN useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow

# Run shell command to intall nessecary cmd line packages
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    '

# Install vim for editing inside the Docker shell
RUN apt-get update \
	&& apt-get -y install vim


# Run shell command to isntall packages
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales

RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# Switch to /usr/local to run pip install
WORKDIR /usr/local
# Run pip install if a requirements.txt file is found
RUN pip install -r requirements.txt
RUN pip install -U pip setuptools wheel \
    && pip install pytest \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh]==${AIRFLOW_VERSION}

# supervisord setup - New (06/10/2020)                       
RUN apt-get update && apt-get install -y supervisor 
COPY supervisord.conf /usr/local/supervisor/supervisord.conf



# New - (06/10/2020)
#COPY script/entrypoint.sh /entrypoint.sh

#RUN chown -R airflow: ${AIRFLOW_USER_HOME}
#USER airflow
# END- New


EXPOSE 8080 5555 8793

# New - (06/10/2020)
#ENTRYPOINT ["sh","/entrypoint.sh"]
