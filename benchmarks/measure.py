import datetime
import json
import os
import subprocess
import sys
import time
import shutil
import urllib.request

url = 'http://127.0.0.1:28123/'

def q(sql):
    return urllib.request.urlopen(urllib.request.Request(url, data=sql.encode()), timeout=180).read().decode()

for binary in sys.argv[1:]:
    assert shutil.disk_usage("/System/Volumes/Data").free >= 3*(1<<30), "stop below 3 GiB free"
    start = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    began = time.monotonic()
    run = subprocess.run([os.path.abspath(binary), '--rebuild'], env={**os.environ, 'CLICKHOUSE_URL': url}, capture_output=True, text=True)
    seconds = time.monotonic() - began
    q('SYSTEM FLUSH LOGS')
    stats = q("SELECT max(memory_usage) AS peak_query_bytes, countIf(exception_code != 0) AS errors FROM system.query_log WHERE event_time_microseconds >= toDateTime64('%s',6,'UTC') AND type != 'QueryStart' AND query NOT LIKE '%%system.query_log%%' FORMAT JSONEachRow" % start)
    counts = q("SELECT count() AS rows, uniqExact(request_id) AS unique_rows, countIf(reply_text='synthetic answer') AS replies FROM cliproxy.request_facts FORMAT JSONEachRow")
    result = dict(binary=binary, seconds=seconds, exit_code=run.returncode, **json.loads(stats), **json.loads(counts))
    result['rows_per_second'] = result['rows'] / seconds
    result['log'] = run.stdout + run.stderr
    print(json.dumps(result), flush=True)
    if run.returncode == 0:
        assert result['rows'] == result['unique_rows'] == result['replies'] == 2400, result
        assert result['errors'] == 0, result
