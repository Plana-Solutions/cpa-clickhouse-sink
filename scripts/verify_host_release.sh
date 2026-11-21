#!/usr/bin/env bash
set -euo pipefail
zip_path="$(realpath "$1")"
deriver_path="$(realpath "$2")"
prefix="plana-ch-release-$(date +%s)-$$"
work="$(mktemp -d "$HOME/plana-ch-release.XXXXXX")"
containers=()
network_created=false
cleanup() {
  for name in "${containers[@]}"; do sudo -n docker rm -fv "$name" >/dev/null; done
  if "$network_created"; then sudo -n docker network rm "$prefix" >/dev/null; fi
  rm -rf "$work"
}
trap cleanup EXIT
mkdir -p "$work/cpa/plugins/linux/arm64"
python3 - "$zip_path" "$work/cpa/plugins/linux/arm64" <<'PY'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    assert z.namelist() == ['clickhouse-sink.so'], z.namelist()
    z.extractall(sys.argv[2])
PY
cp "$(dirname "$0")/fake_upstream.py" "$work/fake.py"
cp "$(dirname "$0")/host_soak.py" "$work/host_soak.py"
cat > "$work/cpa/config.yaml" <<YAML
config-version: 8
server:
  host: ""
  port: 8317
access:
  api-keys: ["isolated-release-client-0123456789"]
api-keys:
  openai-compatibility:
    - name: "fake"
      base-url: "http://$prefix-fake:9000/v1"
      keys:
        - api-key: "isolated-release-upstream-0123456789"
      models:
        - name: "fake-model"
          alias: "fake-model"
plugins:
  enabled: true
  configs:
    clickhouse-sink:
      enabled: true
      url: "http://$prefix-ch:8123"
      database: "cliproxy"
      username: "sink"
      password: "isolated-release-clickhouse"
      store-bodies: true
      body-dedup: ${BODY_DEDUP:-true}
      redact-files: ["/CLIProxyAPI/config.yaml"]
YAML
cat > "$work/client.py" <<'PY'
import json, sys, time, urllib.request
url = 'http://' + sys.argv[1] + ':8317'
for attempt in range(60):
    try:
        urllib.request.urlopen(url + '/v1/models', timeout=1)
        break
    except urllib.error.HTTPError:
        break
    except OSError:
        time.sleep(1)
for stream in [False, True, False]:
    body = {'model':'fake-model', 'stream':stream, 'messages':[{'role':'user','content':'release verification ' + sys.argv[2]}]}
    req = urllib.request.Request(url+'/v1/chat/completions', data=json.dumps(body).encode(), headers={'Authorization':'Bearer isolated-release-client-0123456789','Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
        assert r.status == 200
        assert b'echo:' in data
    print('request status=200 stream=' + str(stream))
PY
sudo -n docker network create --internal "$prefix" >/dev/null
network_created=true
containers+=("$prefix-ch")
sudo -n docker run -d --name "$prefix-ch" --network "$prefix" -e CLICKHOUSE_PASSWORD=isolated-release-clickhouse -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 --memory 1503238553 clickhouse/clickhouse-server:25.8@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77 >/dev/null
containers+=("$prefix-fake")
sudo -n docker run -d --name "$prefix-fake" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/fake.py >/dev/null
for attempt in $(seq 1 60); do
  if sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --multiquery --query "CREATE USER sink IDENTIFIED BY 'isolated-release-clickhouse'; GRANT ALL ON cliproxy.* TO sink"
containers+=("$prefix-cpa")
sudo -n docker run -d --name "$prefix-cpa" --network "$prefix" -v "$work/cpa/config.yaml:/CLIProxyAPI/config.yaml:ro" -v "$work/cpa/plugins:/CLIProxyAPI/plugins:ro" cpa-local/image-results:v8.0.77-a5e8f69 >/dev/null
containers+=("$prefix-client")
sudo -n docker run --name "$prefix-client" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/client.py "$prefix-cpa" initial
for attempt in $(seq 1 30); do
  rows=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.requests' 2>/dev/null || true)
  if [[ "$rows" == 3 ]]; then break; fi
  sleep 1
done
[[ "$rows" == 3 ]]
usage=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.usage')
[[ "$usage" -ge 3 ]]
# Keep a failed body unit queued before rotating credentials and hot-reloading config.
sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "REVOKE INSERT ON cliproxy.body_blobs FROM sink"
containers+=("$prefix-rotation-client")
sudo -n docker run --name "$prefix-rotation-client" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/client.py "$prefix-cpa" rotation
for attempt in $(seq 1 30); do
  if sudo -n docker logs "$prefix-cpa" 2>&1 | grep -q 'insert .*body_blobs.*rows kept for retry'; then break; fi
  sleep 1
done
sudo -n docker logs "$prefix-cpa" 2>&1 | grep 'insert .*body_blobs.*rows kept for retry'
sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --multiquery --query "ALTER USER sink IDENTIFIED BY 'isolated-release-rotated'; GRANT INSERT ON cliproxy.body_blobs TO sink"
python3 - "$work/cpa/config.yaml" <<'PYROTATE'
import sys
from pathlib import Path
p=Path(sys.argv[1]); p.write_text(p.read_text().replace('password: "isolated-release-clickhouse"','password: "isolated-release-rotated"'))
PYROTATE
for attempt in $(seq 1 45); do
 rows=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.requests')
 if [[ "$rows" == 6 ]]; then break; fi
 sleep 1
done
[[ "$rows" == 6 ]]
containers+=("$prefix-recovered-client")
sudo -n docker run --name "$prefix-recovered-client" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/client.py "$prefix-cpa" recovered
for attempt in $(seq 1 30); do
 rows=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.requests')
 if [[ "$rows" == 9 ]]; then break; fi
 sleep 1
done
[[ "$rows" == 9 ]]
usage=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.usage')
[[ "$usage" -ge 9 ]]
echo "Password rotation and hot-config recovery verified. requests=$rows usage=$usage no CPA restart"
cp "$deriver_path" "$work/deriver"
printf '%s\n' 'isolated-release-clickhouse' > "$work/password"
chmod a+rx "$work" "$work/deriver"
chmod a+r "$work/password"
containers+=("$prefix-deriver")
sudo -n docker run -d --name "$prefix-deriver" --network "$prefix" -v "$work/deriver:/deriver:ro" -v "$work/password:/run/secrets/ch_writer:ro" -e CLICKHOUSE_URL="http://$prefix-ch:8123" -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_writer cpa-local/clickhouse-deriver:60b5048 --interval 1s >/dev/null
for attempt in $(seq 1 60); do
  facts=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.request_facts' 2>/dev/null || true)
  if [[ "$facts" == 9 ]]; then break; fi
  sleep 1
done
[[ "$facts" == 9 ]]
if [[ "${BODY_DEDUP:-true}" == true ]]; then
  ids=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT request_id FROM cliproxy.requests')
  expected=$(python3 - <<'PYHASH'
import json,hashlib
values=[]
for phase in ["initial","rotation","recovered"]:
  for stream in [False,True]:
    body={'model':'fake-model','stream':stream,'messages':[{'role':'user','content':'release verification ' + phase}]}
    values.append("'"+hashlib.sha256(json.dumps(body).encode()).hexdigest().upper()+"'")
print(','.join(values))
PYHASH
)
  for id in $ids; do
    exact=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT count() FROM cliproxy.raw_request(id='$id') WHERE body_status='complete' AND hex(SHA256(request_body)) IN ($expected)")
    [[ "$exact" == 1 ]]
  done
  inline=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT countIf(request_body!=''),countIf(body_len>0) FROM cliproxy.requests")
  [[ "$inline" == $'0\t9' ]]
  echo "Dedup production-image byte-exact SQL reconstruction verified. rows=9"
fi
if [[ "${SOAK_SECONDS:-0}" -ge 1200 ]]; then
  containers+=("$prefix-soak")
  sudo -n docker run --name "$prefix-soak" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/host_soak.py "$prefix-cpa" "$prefix-ch" "$SOAK_SECONDS"
fi
rows=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.requests')
expected_facts=$((rows + 2))
# Inject a new row referencing a removed blob, followed by a valid inline row.
sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --multiquery --query "
TRUNCATE TABLE cliproxy.body_blobs;
INSERT INTO cliproxy.requests SELECT * EXCEPT(duration_ms) REPLACE ('missing-blob-test' AS request_id,now64(9) AS ingested_at) FROM cliproxy.requests LIMIT 1;
INSERT INTO cliproxy.requests SELECT * EXCEPT(duration_ms) REPLACE ('after-missing-blob' AS request_id,now64(9) AS ingested_at,'{\"messages\":[{\"role\":\"user\",\"content\":\"valid\"}]}' AS request_body,0 AS body_len,repeat(char(0),32) AS body_sha256,'' AS body_residual,[] AS body_ref_offsets,[] AS body_refs) FROM cliproxy.requests LIMIT 1;"
for attempt in $(seq 1 45); do
 facts=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.request_facts')
 if [[ "$facts" == "$expected_facts" ]]; then break; fi
 sleep 1
done
[[ "$facts" == "$expected_facts" ]]
status=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT parse_status FROM cliproxy.request_facts WHERE request_id='missing-blob-test'")
[[ "$status" == body_incomplete ]]
status=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT parse_status FROM cliproxy.request_facts WHERE request_id='after-missing-blob'")
[[ "$status" != body_incomplete ]]
echo "Missing blob stored as body_incomplete. Later good row derived. facts=$facts"
echo "Production-image load verified. requests=$rows usage=$usage request_facts=$facts"
echo "Released deriver verified in stock distroless image with mounted password file."
if sudo -n docker logs "$prefix-cpa" 2>&1 | grep -E 'buffer full, dropped|dropped [1-9]'; then exit 1; fi
sudo -n docker inspect "$prefix-ch" --format 'ClickHouse cap={{.HostConfig.Memory}} OOMKilled={{.State.OOMKilled}}'
sudo -n docker logs "$prefix-cpa" 2>&1 | grep -E 'clickhouse-sink|plugin.*load' | tail -10
