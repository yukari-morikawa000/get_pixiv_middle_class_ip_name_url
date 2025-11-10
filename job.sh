#!/bin/bash
set -euo pipefail

JOB_NAME="$1"
NAMESPACE="$2"
PROJECT_ID="$3"
DATASET="$4"
IMAGE="$5"

cat <<EOF > job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  template:
    spec:
      containers:
      - name: pixiv-ip
        image: ${IMAGE}
        env:
        - name: GCP_PROJECT_ID
          value: "${PROJECT_ID}"
        - name: BIGQUERY_DATASET
          value: "${DATASET}"
      restartPolicy: Never
EOF

kubectl apply -f job.yaml
kubectl wait --for=condition=complete job/${JOB_NAME} -n ${NAMESPACE} --timeout=21600s
kubectl logs job/${JOB_NAME} -n ${NAMESPACE}
kubectl delete job ${JOB_NAME} -n ${NAMESPACE}
