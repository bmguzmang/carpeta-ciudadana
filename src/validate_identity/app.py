
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

def build_xml_solicitud_verificacion(tx: str, ciudadano: dict, motivo: str) -> str:
    def esc(v):
        if v is None: return ""
        return str(v).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<SolicitudVerificacion>
  <TransactionId>{esc(tx)}</TransactionId>
  <Motivo>{esc(motivo)}</Motivo>
  <Ciudadano>
    <TipoId>{esc(ciudadano.get('tipoId'))}</TipoId>
    <NumeroId>{esc(ciudadano.get('numeroId'))}</NumeroId>
    <Nombres>{esc(ciudadano.get('nombres'))}</Nombres>
    <Apellidos>{esc(ciudadano.get('apellidos'))}</Apellidos>
    <FechaNacimiento>{esc(ciudadano.get('fechaNacimiento'))}</FechaNacimiento>
  </Ciudadano>
</SolicitudVerificacion>
"""

def handler(event, context):
    # Trigger: SQS (SolicitudApertura via SNS)
    for rec in event.get("Records", []):
        body = rec.get("body")
        try:
            msg = json.loads(body)
        except Exception:
            msg = json.loads(json.loads(body)["Message"])

        # Expect canonical: SolicitudApertura
        if msg.get("resourceType") != "SolicitudApertura":
            # make it canonical on the fly (backward compatibility)
            msg["resourceType"] = "SolicitudApertura"

        tx = ensure_txn_id(msg)
        ciudadano = msg.get("ciudadano", {})

        # Persist the received canonical request
        dyn_put(tx, "SolicitudApertura:RECEIVED", msg)

        # Optional: S3 breadcrumb
        xray_recorder.begin_subsegment("write_mock_s3_breadcrumb")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"validations/{tx}.txt",
            Body=f"SolicitudApertura received at {now_iso()}".encode("utf-8"),
        )
        xray_recorder.end_subsegment()

        # Build XML version of SolicitudVerificacion (canonical name preserved in envelope meta)
        xml_payload = build_xml_solicitud_verificacion(tx, ciudadano, "Registro")
        envelope = {
            "contentType": "application/xml",
            "resourceType": "SolicitudVerificacion",
            "payload": xml_payload
        }

        xray_recorder.begin_subsegment("invoke_registraduria_mock_xml")
        lambda_client.invoke(
            FunctionName=REGISTRADURIA_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(envelope).encode("utf-8"),
        )
        xray_recorder.end_subsegment()

    return {"ok": True}
