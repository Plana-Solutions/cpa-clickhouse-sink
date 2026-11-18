"""Exercise the production host with synthetic bodies and verify every captured byte."""
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
records = [json.loads(line) for line in query("SELECT request_id,hex(body_sha256) AS sha FROM cliproxy.requests FORMAT JSONEachRow").splitlines()]
verified = 0
for row in records:
    if row["sha"] not in expected:
        continue
    result = query("SELECT body_status,hex(SHA256(request_body)) AS sha FROM cliproxy.raw_request(id='" + row["request_id"] + "') FORMAT JSONEachRow")
    decoded = json.loads(result)
    assert decoded["body_status"] == "complete" and decoded["sha"] == row["sha"], row["request_id"]
    verified += 1
assert verified == count, (verified, count)
# Force a real 25.8 error after response headers and data have streamed.
raw = query("SELECT number, throwIf(number=50000) FROM numbers(100000) SETTINGS max_block_size=1000 FORMAT JSONEachRow", buffer_size="1024", wait_end_of_query="0")
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
