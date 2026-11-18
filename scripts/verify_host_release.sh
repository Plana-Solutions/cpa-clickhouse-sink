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
cp "$HOME/chtest/fake.py" "$work/fake.py"
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
      username: "default"
      password: "isolated-release-clickhouse"
      store-bodies: true
      body-dedup: ${BODY_DEDUP:-false}
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
    body = {'model':'fake-model', 'stream':stream, 'messages':[{'role':'user','content':'release verification'}]}
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
sudo -n docker run -d --name "$prefix-ch" --network "$prefix" -e CLICKHOUSE_PASSWORD=isolated-release-clickhouse clickhouse/clickhouse-server:25.8 >/dev/null
containers+=("$prefix-fake")
sudo -n docker run -d --name "$prefix-fake" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/fake.py >/dev/null
for attempt in $(seq 1 60); do
  if sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
containers+=("$prefix-cpa")
sudo -n docker run -d --name "$prefix-cpa" --network "$prefix" -v "$work/cpa/config.yaml:/CLIProxyAPI/config.yaml:ro" -v "$work/cpa/plugins:/CLIProxyAPI/plugins:ro" cpa-local/image-results:v8.0.77-a5e8f69 >/dev/null
containers+=("$prefix-client")
sudo -n docker run --name "$prefix-client" --network "$prefix" -v "$work:/verify:ro" python:3.12-slim python /verify/client.py "$prefix-cpa"
for attempt in $(seq 1 30); do
  rows=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.requests' 2>/dev/null || true)
  if [[ "$rows" == 3 ]]; then break; fi
  sleep 1
done
[[ "$rows" == 3 ]]
usage=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.usage')
[[ "$usage" -ge 3 ]]
cp "$deriver_path" "$work/deriver"
printf '%s\n' 'isolated-release-clickhouse' > "$work/password"
chmod a+rx "$work" "$work/deriver"
chmod a+r "$work/password"
containers+=("$prefix-deriver")
sudo -n docker run -d --name "$prefix-deriver" --network "$prefix" -v "$work/deriver:/deriver:ro" -v "$work/password:/run/secrets/ch_writer:ro" -e CLICKHOUSE_URL="http://$prefix-ch:8123" -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_writer cpa-local/clickhouse-deriver:60b5048 --interval 1s >/dev/null
for attempt in $(seq 1 60); do
  facts=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT count() FROM cliproxy.request_facts' 2>/dev/null || true)
  if [[ "$facts" == 3 ]]; then break; fi
  sleep 1
done
[[ "$facts" == 3 ]]
if [[ "${BODY_DEDUP:-false}" == true ]]; then
  ids=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query 'SELECT request_id FROM cliproxy.requests')
  expected=$(python3 - <<'PYHASH'
import json,hashlib
values=[]
for stream in [False,True]:
    body={'model':'fake-model','stream':stream,'messages':[{'role':'user','content':'release verification'}]}
    values.append("'"+hashlib.sha256(json.dumps(body).encode()).hexdigest().upper()+"'")
print(','.join(values))
PYHASH
)
  for id in $ids; do
    exact=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT count() FROM cliproxy.raw_request(id='$id') WHERE body_status='complete' AND hex(SHA256(request_body)) IN ($expected)")
    [[ "$exact" == 1 ]]
  done
  inline=$(sudo -n docker exec "$prefix-ch" clickhouse-client --password isolated-release-clickhouse --query "SELECT countIf(request_body!=''),countIf(body_len>0) FROM cliproxy.requests")
  [[ "$inline" == $'0\t3' ]]
  echo "Dedup production-image byte-exact SQL reconstruction verified. rows=3"
fi
echo "Production-image load verified. requests=$rows usage=$usage request_facts=$facts"
echo "Released deriver verified in stock distroless image with mounted password file."
sudo -n docker logs "$prefix-cpa" 2>&1 | grep -E 'clickhouse-sink|plugin.*load' | tail -10
