#!/usr/bin/env bash
set -euo pipefail

IMAGE="dobadevv/goq"
VERSION="$(git describe --tags --abbrev=0)"

docker build -t "${IMAGE}:${VERSION}" -t "${IMAGE}:latest" .

docker push "${IMAGE}:${VERSION}"
docker push "${IMAGE}:latest"
