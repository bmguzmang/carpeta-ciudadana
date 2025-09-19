
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
    # Trigger: SNS folder-created (NotificacionCarpetaCreada)
    for r in event.get("Records", []):
        m = json.loads(r["Sns"]["Message"])
        if m.get("resourceType") != "NotificacionCarpetaCreada":
            m["resourceType"] = "NotificacionCarpetaCreada"
        tx = ensure_txn_id(m)
        email = f"ciudadano.{tx[:8]}@carpetacolombia.co"
        salida = {"resourceType":"EmailAsignado","transactionId":tx,"email":email,"asignadoEn":now_iso()}
        dyn_put(tx, "EmailAsignado:DONE", salida)
    return {"ok": True}
