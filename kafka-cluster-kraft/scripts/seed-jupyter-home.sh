#!/usr/bin/env bash
# Copy the image's /home/jupyter contents into a local bind-mount directory
# so notebooks and course materials can be edited on the host.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${ROOT}/pyspark-jupyter-kafka/home"
IMAGE="${JUPYTER_IMAGE:-easewithdata/pyspark-jupyter-lab}"

mkdir -p "${DEST}"

cid="$(docker create "${IMAGE}")"
trap 'docker rm -f "${cid}" >/dev/null 2>&1 || true' EXIT

echo "Seeding ${DEST} from ${IMAGE}:/home/jupyter ..."
docker cp "${cid}:/home/jupyter/." "${DEST}/"
echo "Done. Contents:"
ls -la "${DEST}"
