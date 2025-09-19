
import os, json, uuid, boto3
from datetime import datetime, timezone
from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

TABLE_NAME = os.getenv("TRANSACTION_TABLE")
BUCKET_NAME = os.getenv("OBJECT_BUCKET")
IDENTITY_RESPONSE_QUEUE_URL = os.getenv("IDENTITY_RESPONSE_QUEUE_URL")
REGISTRY_RESPONSE_QUEUE_URL = os.getenv("REGISTRY_RESPONSE_QUEUE_URL")
FOLDER_CREATED_TOPIC_ARN = os.getenv("FOLDER_CREATED_TOPIC_ARN")
REGISTRADURIA_FUNCTION_NAME = os.getenv("REGISTRADURIA_FUNCTION_NAME")
MINTIC_FUNCTION_NAME = os.getenv("MINTIC_FUNCTION_NAME")

dynamo = boto3.resource("dynamodb")
table = dynamo.Table(TABLE_NAME) if TABLE_NAME else None
s3 = boto3.client("s3")
sqs = boto3.client("sqs")
sns = boto3.client("sns")
lambda_client = boto3.client("lambda")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_txn_id(payload: dict) -> str:
    tx = payload.get("transactionId")
    if not tx:
        tx = str(uuid.uuid4())
        payload["transactionId"] = tx
    return tx

def dyn_put(tx, step, data):
    if not table:
        return
    item = {
        "pk": f"TX#{tx}",
        "sk": f"STEP#{step}",
        "createdAt": now_iso(),
        "data": data,
    }
    table.put_item(Item=item)

import time
def handler(event, context):
    # Input: ResultadoVerificacion (canonical)
    msg = event if isinstance(event, dict) else {}
    tx = ensure_txn_id(msg)

    time.sleep(0.1)
    resultado_registro = {
        "resourceType": "ResultadoRegistro",
        "transactionId": tx,
        "estadoRegistro": "Aceptado",
        "procesadoEn": now_iso()
    }

    xray_recorder.begin_subsegment("send_to_registry_response_queue")
    sqs.send_message(QueueUrl=REGISTRY_RESPONSE_QUEUE_URL, MessageBody=json.dumps(resultado_registro))
    xray_recorder.end_subsegment()

    dyn_put(tx, "ResultadoRegistro:SENT", resultado_registro)
    return {"ok": True}
