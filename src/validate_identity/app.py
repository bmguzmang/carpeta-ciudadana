
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

def get_citizen_key(ciudadano: dict) -> str:
    if not ciudadano:
        return "UNKNOWN"
    return f"{ciudadano.get('tipoId','?')}-{ciudadano.get('numeroId','?')}"

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
    # Trigger: SQS folder-requests-queue (messages from SNS with RawMessageDelivery=true)
    records = event.get("Records", [])
    for r in records:
        body = r.get("body")
        try:
            payload = json.loads(body)
        except Exception:
            # If SNS wrapping is present
            payload = json.loads(json.loads(body)["Message"])

        tx = ensure_txn_id(payload)
        ciudadano = payload.get("ciudadano", {})
        citizen_key = get_citizen_key(ciudadano)

        # S3 mock object (operator object storage)
        xray_recorder.begin_subsegment("write_mock_s3")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"validations/{tx}.txt",
            Body=f"validation started for {citizen_key} at {now_iso()}".encode("utf-8"),
        )
        xray_recorder.end_subsegment()

        # Dynamo record
        dyn_put(tx, "ValidateIdentity:RECEIVED", {"ciudadano": ciudadano})

        # Invoke Registraduría mock with our canonical 'SolicitudVerificacion'
        solicitud = {"transactionId": tx, "ciudadano": ciudadano, "motivo": "Registro"}
        xray_recorder.begin_subsegment("invoke_registraduria_mock")
        lambda_client.invoke(
            FunctionName=REGISTRADURIA_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(solicitud).encode("utf-8"),
        )
        xray_recorder.end_subsegment()
    return {"ok": True, "processed": len(records)}
