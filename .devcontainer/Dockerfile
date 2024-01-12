# See here for image contents: https://github.com/microsoft/vscode-dev-containers/blob/main/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version: 3, 3.10, 3.9, 3.8, 3.7, 3.6
ARG VARIANT="3.10"
FROM mcr.microsoft.com/devcontainers/python:${VARIANT}

# Install additional OS packages, including appropriate Java SDK.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        openjdk-17-jre \
        curl \
        gnupg \
        apt-transport-https

# Install Spark and configure environment variables
RUN cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    cd / && \
    tar -xzf /tmp/spark-3.3.1-bin-hadoop3.tgz && \
    mv /spark-3.3.1-bin-hadoop3 /spark && \
    rm -rf /tmp/spark-3.3.1-bin-hadoop3.tgz

ENV SPARK_HOME /spark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Install Poetry and add exe to PATH
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH ${PATH}:/root/.local/bin

# Prepare and install PowerShell

# Import the public repository GPG keys
RUN curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
# Register the Microsoft Product feed
RUN sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-debian-bullseye-prod bullseye main" > /etc/apt/sources.list.d/microsoft.list'
# Install PowerShell
RUN sudo apt update && sudo apt -y install --no-install-recommends \
        dotnet-sdk-8.0 \
        powershell \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*
