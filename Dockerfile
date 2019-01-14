FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Install gcloud tools
# (commands taken from https://cloud.google.com/storage/docs/gsutil_install#deb)
RUN apt-get update && apt-get install -y lsb-release apt-transport-https curl gnupg git
RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk

# Install pyflame (for statistical profiling)
RUN apt-get update && apt-get install -y autoconf automake autotools-dev g++ pkg-config python-dev python3-dev libtool make
WORKDIR /pyflame
RUN git clone https://github.com/uber/pyflame.git .
RUN ./autogen.sh && ./configure && make && make install

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Make a directory for intermediate data
RUN mkdir /data

# Copy the rest of the project
ADD project_redss /app/project_redss
ADD code_schemes/*.json /app/code_schemes/
ADD redss_pipeline.py /app
