#!/bin/bash
set -euo pipefail

# 5つの引数を正しく受け取る
JOB_NAME="$1"
NAMESPACE="$2"
PROJECT_ID="$3"
DATASET="$4"
IMAGE="$5" # Digdagから渡されたDockerイメージパス

cat <<EOF > job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  template:
  # コンテナが使用するサービスアカウント
    serviceAccountName: collectro
    spec:
      containers:
      - name: pixiv-ip
        image: ${IMAGE} # Digdagから渡されたIMAGE変数を使用
        # unified_pixiv_search.py の main() を実行
        command: ["python", "unified_pixiv_search.py"]
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