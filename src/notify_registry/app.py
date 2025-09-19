
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

def handler(event, context):
    # Trigger: SQS identity-response-queue (ResultadoVerificacion)
    for r in event.get("Records", []):
        msg = json.loads(r.get("body"))
        if msg.get("resourceType") != "ResultadoVerificacion":
            # normalize older messages
            msg["resourceType"] = "ResultadoVerificacion"

        tx = ensure_txn_id(msg)
        dyn_put(tx, "ResultadoVerificacion:RECEIVED", msg)

        # Forward to MinTIC preserving canonical resourceType
        xray_recorder.begin_subsegment("invoke_mintic_mock")
        lambda_client.invoke(
            FunctionName=MINTIC_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(msg).encode("utf-8"),
        )
        xray_recorder.end_subsegment()
    return {"ok": True}
