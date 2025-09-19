import os, json, uuid, boto3
from datetime import datetime, timezone
from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

TABLE_NAME = os.getenv("TRANSACTION_TABLE")
IDENTITY_RESPONSE_QUEUE_URL = os.getenv("IDENTITY_RESPONSE_QUEUE_URL")

dynamo = boto3.resource("dynamodb")
table = dynamo.Table(TABLE_NAME) if TABLE_NAME else None
sqs = boto3.client("sqs")

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

# --- XML helpers ---
import xml.etree.ElementTree as ET

def parse_xml_envelope(event):
    """Accepts either {contentType, payload} JSON (XML inside) or legacy JSON.
    Returns (tx, ciudadano, motivo)."""
    if isinstance(event, dict) and event.get("contentType") == "application/xml":
        xml = event.get("payload", "")
        root = ET.fromstring(xml)
        tx = (root.findtext("./TransactionId") or "").strip()
        motivo = (root.findtext("./Motivo") or "").strip()
        ciudadano = {
            "tipoId": root.findtext("./Ciudadano/TipoId"),
            "numeroId": root.findtext("./Ciudadano/NumeroId"),
            "nombres": root.findtext("./Ciudadano/Nombres"),
            "apellidos": root.findtext("./Ciudadano/Apellidos"),
            "fechaNacimiento": root.findtext("./Ciudadano/FechaNacimiento"),
        }
        return tx, ciudadano, motivo
    # Fallback to JSON canonical
    tx = ensure_txn_id(event if isinstance(event, dict) else {})
    ciudadano = (event or {}).get("ciudadano", {})
    return tx, ciudadano, (event or {}).get("motivo", "Registro")

import random, time
def handler(event, context):
    tx, ciudadano, motivo = parse_xml_envelope(event)
    if not tx:
        tx = str(uuid.uuid4())  # ensure one

    time.sleep(0.3)
    estado = random.choice(["Verificado","Verificado","Inconcluso"])  # mostly verified

    resultado = {
        "transactionId": tx,
        "ciudadano": ciudadano,
        "estado": estado,
        "verificadoEn": now_iso(),
        "detalles": f"Mock OK ({motivo})" if estado == "Verificado" else "Manual review suggested"
    }

    xray_recorder.begin_subsegment("send_to_identity_response_queue")
    sqs.send_message(QueueUrl=IDENTITY_RESPONSE_QUEUE_URL, MessageBody=json.dumps(resultado))
    xray_recorder.end_subsegment()

    dyn_put(tx, "Registraduria:PROCESSED", {"estado": estado, "motivo": motivo, "payloadType": "XML" if isinstance(event, dict) and event.get("contentType") == "application/xml" else "JSON"})
    return {"ok": True}
