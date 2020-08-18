# Cloud Spanner Emulator Samples

This repository contains samples that demonstrate how to use
[Cloud Spanner emulator](https://cloud.google.com/spanner/docs/emulator) in
CI/CD pipelines.

Please feel free to report issues and send pull requests, but note that these
samples are not officially supported as part of the Cloud Spanner product.

## Starting Emulator from Docker

The emulator can be started using Docker on Linux, MacOS and Windows. As a
prerequisite, you would need to install docker on your system. You can configure
a default instance and/or database using following sample recipe:

### Build and run docker image.

```shell
cd emulator-samples/docker

# Build an image.
docker build -t start-spanner-emulator .

# Run docker container.
docker run --detach --name emulator -p 9010:9010 -p 9020:9020
start-spanner-emulator

# Verify the container is running.
docker ps
```

### Test that everything is working.

As a prerequisite, install [Google Cloud SDK](https://cloud.google.com/sdk). You
can then update gcloud configuration to point to the docker container running
emulator as following:

```shell
# Verify that gcloud is installed.
gcloud --version

# Create a new gcloud configuration for this test.
gcloud config configurations create emulator-docker

gcloud config set api_endpoint_overrides/spanner http://0.0.0.0:9020/
gcloud config set auth/disable_credentials true
gcloud config set core/project test-project

gcloud config list
[api_endpoint_overrides]
spanner = http://0.0.0.0:9020/
[auth]
disable_credentials = true
[core]
project = test-project

gcloud spanner instances list --project=test-project
NAME           DISPLAY_NAME   CONFIG                NODE_COUNT  STATE
test-instance  Test Instance  emulator-test-config  1           READY

gcloud spanner databases list --instance=test-instance --project=test-project
NAME           STATE
test-database  READY
```

### Cleanup

```shell
# Stop docker when your integration tests complete.
docker stop emulator
```

## Disclaimer

This is not an official Google product.
