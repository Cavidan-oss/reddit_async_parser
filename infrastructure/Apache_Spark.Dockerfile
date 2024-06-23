FROM bitnami/spark:3.4.1

USER root
RUN mkdir -p /temp/checkpoint
# Install additional libraries
RUN pip install py4j