"""Exercise the production host with synthetic bodies and verify every captured byte."""
import base64
import http.client
import hashlib
import json
import sys
import time
import urllib.parse
import urllib.request

cpa, ch, seconds = sys.argv[1], sys.argv[2], int(sys.argv[3])
assert seconds >= 1200
def query(sql, **settings):
    params = {"user": "default", "password": "isolated-release-clickhouse", "http_write_exception_in_output_format": "0", **settings}
    with urllib.request.urlopen(urllib.request.Request("http://" + ch + ":8123/?" + urllib.parse.urlencode(params), data=sql.encode()), timeout=60) as r:
        return r.read()

expected = set()
start = time.monotonic()
count = 0
while time.monotonic() - start < seconds:
    body = json.dumps({"model": "fake-model", "stream": count % 2 == 0, "messages": [{"role": "system", "content": "synthetic repeated context " * 2048}, {"role": "user", "content": "soak-" + str(count)}]}).encode()
    expected.add(hashlib.sha256(body).hexdigest().upper())
    req = urllib.request.Request("http://" + cpa + ":8317/v1/chat/completions", data=body, headers={"Authorization": "Bearer isolated-release-client-0123456789", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        assert b"echo:" in r.read()
    count += 1
    if count % 60 == 0:
        print("soak elapsed=" + str(round(time.monotonic()-start)) + " requests=" + str(count), flush=True)
    time.sleep(1)
elapsed = time.monotonic() - start
for attempt in range(60):
    rows = query("SELECT count() FROM cliproxy.requests").strip()
    if int(rows) == count + 9:
        break
    time.sleep(1)
assert int(rows) == count + 9, (rows, count)
records = [json.loads(line) for line in query("SELECT request_id,hex(body_sha256) AS sha,lineage,body_len,base64Encode(body_residual) residual,body_ref_offsets offsets,arrayMap(x->hex(x),body_refs) refs FROM cliproxy.requests FORMAT JSONEachRow").splitlines()]
blobs = {}
for line in query("SELECT lineage,hex(hash) hash,base64Encode(data) data FROM cliproxy.body_blobs LIMIT 1 BY lineage,hash FORMAT JSONEachRow").splitlines():
    item = json.loads(line)
    data = base64.b64decode(item["data"])
    assert hashlib.sha256(data).hexdigest()[:32].upper() == item["hash"]
    blobs[(str(item["lineage"]),item["hash"])] = data
verified = 0
for row in records:
    if row["sha"] not in expected:
        continue
    residual = base64.b64decode(row["residual"])
    assert len(row["refs"]) == len(row["offsets"])
    parts, last = [], 0
    for ref, offset in zip(row["refs"],row["offsets"]):
        assert last <= offset <= len(residual)
        parts.extend([residual[last:offset],blobs[(str(row["lineage"]),ref)]])
        last = offset
    parts.append(residual[last:])
    body = b"".join(parts)
    assert len(body) == row["body_len"] and hashlib.sha256(body).hexdigest().upper() == row["sha"], row["request_id"]
    verified += 1
assert verified == count, (verified, count)
# Force a real 25.8 error after response headers and data have streamed.
try:
    raw = query("SELECT number, throwIf(number=50000) FROM numbers(100000) SETTINGS max_block_size=1000 FORMAT JSONEachRow", buffer_size="1024", wait_end_of_query="0")
except http.client.IncompleteRead as exc:
    assert b'{"number":' in exc.partial, "no prefix rows before transport failure"
else:
    lines = raw.splitlines()
    assert any(line.startswith(b'{"number":') for line in lines), "no prefix rows"
    assert b"Code:" in raw, "no mid-stream exception"
    try:
        [json.loads(line) for line in lines]
    except (ValueError, UnicodeError):
        pass
    else:
        raise AssertionError("mid-stream exception decoded as data")
print(json.dumps({"duration_seconds": elapsed, "synthetic_requests": count, "captured_requests": int(rows), "drops": int(count+9-int(rows)), "byte_exact": verified, "midstream_error": "rejected"}), flush=True)
