#!/bin/sh
# =============================================================================
# Dremio Source Auto-Provisioning Script
# 1. Cria o usuário admin se ainda não existir (first-run).
# 2. Autentica e cria a fonte Nessie via API REST.
# =============================================================================

DREMIO_URL="http://dremio:9047"
DREMIO_USER="${DREMIO_ADMIN_USER}"
DREMIO_PASS="${DREMIO_ADMIN_PASSWORD}"
NESSIE_ENDPOINT="${NESSIE_URI:-http://nessie:19120/api/v2}"
MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-admin}"
MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-password}"
MAX_RETRIES=40
RETRY_INTERVAL=10

echo ">>> [Dremio Setup] Aguardando Dremio inicializar..."

for i in $(seq 1 $MAX_RETRIES); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${DREMIO_URL}/apiv2/server_status")
  if [ "$STATUS" = "200" ]; then
    echo ">>> [Dremio Setup] Dremio está pronto!"
    break
  fi
  echo ">>> [Dremio Setup] Tentativa $i/$MAX_RETRIES (status: $STATUS). Aguardando ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
  if [ "$i" = "$MAX_RETRIES" ]; then
    echo ">>> [Dremio Setup] ERRO: Dremio não iniciou a tempo. Abortando."
    exit 1
  fi
done

# ── Passo 1: Tentar criar o primeiro usuário (first-run setup) ──────────────
echo ">>> [Dremio Setup] Tentando criar usuário admin (first-run)..."

for j in $(seq 1 5); do
  BOOTSTRAP_OUT=$(curl -s -w "\n%{http_code}" \
    -X PUT "${DREMIO_URL}/apiv2/bootstrap/firstuser" \
    -H "Content-Type: application/json" \
    -d "{
      \"userName\": \"${DREMIO_USER}\",
      \"firstName\": \"Admin\",
      \"lastName\": \"User\",
      \"email\": \"admin@lakehouse.local\",
      \"password\": \"${DREMIO_PASS}\"
    }")
  
  BOOTSTRAP_BODY=$(echo "$BOOTSTRAP_OUT" | head -n -1)
  BOOTSTRAP_RESPONSE=$(echo "$BOOTSTRAP_OUT" | tail -n 1)

  if [ "$BOOTSTRAP_RESPONSE" = "200" ]; then
    echo ">>> [Dremio Setup] Usuário admin criado com sucesso!"
    break
  elif [ "$BOOTSTRAP_RESPONSE" = "409" ]; then
    echo ">>> [Dremio Setup] Usuário admin já existe. Continuando..."
    break
  elif [ "$BOOTSTRAP_RESPONSE" = "400" ] && echo "$BOOTSTRAP_BODY" | grep -q "already exists"; then
    echo ">>> [Dremio Setup] Dremio já inicializado. Continuando..."
    break
  else
    echo ">>> [Dremio Setup] Tentativa $j/5: Bootstrap retornou status $BOOTSTRAP_RESPONSE"
    echo ">>> [Dremio Setup] Resposta: $BOOTSTRAP_BODY"
    if [ "$j" = "5" ]; then
      echo ">>> [Dremio Setup] Continuando para tentativa de login mesmo com erro no bootstrap..."
    else
      sleep 5
    fi
  fi
done

sleep 3

# ── Passo 2: Autenticar ────────────────────────────────────────────────────
echo ">>> [Dremio Setup] Autenticando..."
LOGIN_BODY="{\"userName\": \"${DREMIO_USER}\", \"password\": \"${DREMIO_PASS}\"}"
LOGIN_RESPONSE=$(curl -s -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "$LOGIN_BODY")

TOKEN=$(echo "$LOGIN_RESPONSE" | sed 's/.*"token":"\([^"]*\)".*/\1/')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "$LOGIN_RESPONSE" ]; then
  echo ">>> [Dremio Setup] ERRO: Falha na autenticação."
  echo ">>> [Dremio Setup] Resposta: $LOGIN_RESPONSE"
  exit 1
fi

echo ">>> [Dremio Setup] Autenticado! Token obtido."

# ── Passo 3: Verificar se a fonte já existe ────────────────────────────────
SOURCE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: _dremio${TOKEN}" \
  "${DREMIO_URL}/apiv2/source/lakehouse/")

if [ "$SOURCE_STATUS" = "200" ]; then
  echo ">>> [Dremio Setup] ✅ Fonte 'lakehouse' já existe. Nada a fazer."
  exit 0
fi

# ── Passo 4: Criar a fonte Nessie ──────────────────────────────────────────
echo ">>> [Dremio Setup] Criando fonte Nessie 'lakehouse'..."
CREATE_RESPONSE=$(curl -s -X PUT "${DREMIO_URL}/apiv2/source/lakehouse/" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"lakehouse\",
    \"type\": \"NESSIE\",
    \"config\": {
      \"nessieEndpoint\": \"${NESSIE_ENDPOINT}\",
      \"nessieAuthType\": \"NONE\",
      \"credentialType\": \"ACCESS_KEY\",
      \"awsAccessKey\": \"${MINIO_ACCESS_KEY}\",
      \"awsAccessSecret\": \"${MINIO_SECRET_KEY}\",
      \"awsRootPath\": \"warehouse\",
      \"secure\": false,
      \"propertyList\": [
        {\"name\": \"dremio.s3.compat\", \"value\": \"true\"},
        {\"name\": \"fs.s3a.path.style.access\", \"value\": \"true\"},
        {\"name\": \"fs.s3a.endpoint\", \"value\": \"minio:9000\"},
        {\"name\": \"fs.s3a.connection.ssl.enabled\", \"value\": \"false\"},
        {\"name\": \"fs.s3a.aws.credentials.provider\", \"value\": \"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"}
      ]
    }
  }")

echo ">>> [Dremio Setup] Resposta da criação: $CREATE_RESPONSE"

# Verifica se o nome aparece na resposta (indica sucesso)
if echo "$CREATE_RESPONSE" | grep -q '"name":"lakehouse"'; then
  echo ">>> [Dremio Setup] ✅ Fonte 'lakehouse' criada com sucesso!"
else
  echo ">>> [Dremio Setup] ⚠️  Verifique a resposta acima para detalhes do erro."
  exit 1
fi
