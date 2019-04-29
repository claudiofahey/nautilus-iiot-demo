ARG SOURCE_IMAGE
ARG SOURCE_TAG

FROM ${SOURCE_IMAGE}:${SOURCE_TAG}

USER 0

# Add Pravega libraries
COPY lib /usr/local/share/java/

# Add CA certificates
COPY ca-certificates/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

USER ${NB_USER}
