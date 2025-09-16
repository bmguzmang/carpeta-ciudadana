
# Carpeta Ciudadana — POC (SAM)

Flujo completo:
SNS (folder-opening-requests) → SQS (folder-requests-queue) → validate-identity → registraduria-mock → SQS (identity-response-queue) → notify-registry → mintic-mock → SQS (registry-response-queue) → create-folder → SNS (folder-created) → [generate-unique-email, load-signed-identity-document]

## Despliegue
sam build && sam deploy --guided

## Publicar solicitud de apertura de carpeta
aws sns publish --topic-arn <OpeningRequestsTopicArn> --message '{
  "transactionId":"demo-123",
  "ciudadano":{"tipoId":"CC","numeroId":"1020304050","nombres":"Andres","apellidos":"Zapata"},
  "consentimiento":{"id":"cons-001","otorgadoEn":"2025-09-08T00:00:00Z","alcances":["carpeta:registro"]}
}'
