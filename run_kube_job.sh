#!/bin/bash
set -e

# 引数を変数に格納 (Digdagから渡される)
BATCH_INDEX=$1
JOB_NAME=$2
GCP_PROJECT_ID=$3
BIGQUERY_DATASET=$4
NAMESPACE="collectro"
# JOB_NAMEからsession_uuidを抽出
SESSION_ID=$(echo $JOB_NAME | cut -d'-' -f4-8)

# --- デバッグ情報の表示 ---
echo "--- Starting Job ---"
echo "Job Name: ${JOB_NAME}"
echo "--------------------"

# job.yamlの変数を置き換えて一時ファイルを作成 (envsubstが必要)
export BATCH_INDEX
export JOB_NAME
export GCP_PROJECT_ID
export BIGQUERY_DATASET
export SESSION_ID
envsubst < job.yaml > "job_${SESSION_ID}_${BATCH_INDEX}.yaml"
echo "Generated temporary manifest: job_${SESSION_ID}_${BATCH_INDEX}.yaml"

# 1. Kubernetesにジョブを適用
kubectl apply -f "job_${SESSION_ID}_${BATCH_INDEX}.yaml" --namespace=${NAMESPACE}
echo "Job applied to Kubernetes."

# 2. Podが作成されるのを待つ
# ... (Pod待機ロジックは以前の成功版をそのまま使用) ...
POD_NAME=""
for i in {1..12}; do
    POD_NAME=$(kubectl get pods -l job-name=${JOB_NAME} --namespace=${NAMESPACE} -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)
    if [[ ! -z "$POD_NAME" ]]; then
        echo "Found Pod: ${POD_NAME}"
        break
    fi
    echo "Pod not found, retrying in 10 seconds... (${i}/12)"
    sleep 10
done

# 3. Podが見つからなかった場合のデバッグと終了処理
if [[ -z "$POD_NAME" ]]; then
    echo "Error: Could not find Pod using selector for job ${JOB_NAME} after 120 seconds."
    kubectl get pods --namespace=${NAMESPACE}
    kubectl describe job ${JOB_NAME} --namespace=${NAMESPACE}
    exit 1
fi

# 4. ジョブの完了を待つ (タイムアウト30分)
echo "--- Waiting for job completion (Max 30m) ---"
kubectl wait --for=condition=complete "job/${JOB_NAME}" --namespace=${NAMESPACE} --timeout=30m || echo "Job did not complete in time. Checking logs..."

# 5. コンテナのログを出力
echo "--- Pod Logs for ${POD_NAME} ---"
kubectl logs ${POD_NAME} --namespace=${NAMESPACE} --all-containers=true || true

# 6. 最終的なジョブの状態を確認して成功/失敗を判断
JOB_STATUS=$(kubectl get job ${JOB_NAME} --namespace=${NAMESPACE} -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
if [ "${JOB_STATUS}" == "True" ]; then
  echo "Job completed successfully."
  kubectl delete "job/${JOB_NAME}" --namespace=${NAMESPACE} # 成功時ジョブを削除
  rm "job_${SESSION_ID}_${BATCH_INDEX}.yaml"
  exit 0
else
  echo "Job failed. Exit code 1."
  exit 1
fi