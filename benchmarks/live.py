import json
import os
import subprocess
import sys
import time
import shutil
assert shutil.disk_usage("/System/Volumes/Data").free >= 3*(1<<30), "stop below 3 GiB free"
import urllib.request

url = 'http://127.0.0.1:28123/'
def q(sql):
    return urllib.request.urlopen(urllib.request.Request(url, data=sql.encode()),timeout=180).read().decode()

started = time.time()
for i in range(60):
    q("INSERT INTO cliproxy.requests (request_id,trace_id,started_at,completed_at,ingested_at,source_format,request_body,response_chunks) SELECT 'pause-%d','pause-%d',now64(9)-INTERVAL 15 MINUTE,now64(9),now64(9)-toIntervalSecond(%d),'openai',concat('{\"messages\":[{\"role\":\"user\",\"content\":\"',repeat('synthetic ',20000),'\"}]}'),['data: {\"choices\":[{\"delta\":{\"content\":\"synthetic answer\"}}]}']" % (i,i,900-i*15))
q('TRUNCATE TABLE cliproxy.request_facts')
q('TRUNCATE TABLE cliproxy.deriver_progress')
log = open('/tmp/cpa-deriver-bench/live.log','w')
p = subprocess.Popen([os.path.abspath(sys.argv[1])], env={**os.environ,'CLICKHOUSE_URL':url}, stdout=log,stderr=log)
sent = {}
latencies = {}
start = time.monotonic()
try:
    while time.monotonic()-start < 120:
        now = time.monotonic()
        if len(sent)<20 and now-start >= len(sent)*0.5:
            rid = 'live-'+str(len(sent))
            q("INSERT INTO cliproxy.requests (request_id,trace_id,started_at,completed_at,ingested_at,source_format,request_body,response_chunks) SELECT '%s','%s',now64(9),now64(9),now64(9),'openai',concat('{\"messages\":[{\"role\":\"user\",\"content\":\"',repeat('synthetic ',30000),'\"}]}'),['data: {\"choices\":[{\"delta\":{\"content\":\"synthetic answer\"},\"finish_reason\":\"stop\"}]}']"%(rid,rid))
            sent[rid] = time.monotonic()
        for rid in q("SELECT request_id FROM cliproxy.request_facts WHERE startsWith(request_id,'live-') FORMAT TSV").splitlines():
            if rid not in latencies: latencies[rid] = time.monotonic()-sent[rid]
        if len(latencies)==20: break
        if p.poll() is not None: raise RuntimeError('deriver exited')
        time.sleep(0.5)
    remaining = int(q('SELECT count() FROM cliproxy.requests WHERE request_id NOT IN (SELECT request_id FROM cliproxy.request_facts)'))
    result=dict(sent=len(sent),derived=len(latencies),max_latency_seconds=max(latencies.values(),default=0),backlog_remaining=remaining)
    print(json.dumps(result),flush=True)
    assert len(latencies)==20 and max(latencies.values())<120 and remaining>0,result
finally:
    p.terminate()
    p.wait(timeout=30)
    log.close()

log = open('/tmp/cpa-deriver-bench/recovery.log','w')
began=time.monotonic()
p=subprocess.Popen([os.path.abspath(sys.argv[1]),'--interval=1s'],env={**os.environ,'CLICKHOUSE_URL':url},stdout=log,stderr=log)
try:
    while time.monotonic()-began<600:
        remaining=int(q('SELECT count() FROM cliproxy.requests WHERE request_id NOT IN (SELECT request_id FROM cliproxy.request_facts)'))
        if remaining==0: break
        if p.poll() is not None: raise RuntimeError('recovery process exited')
        time.sleep(1)
    counts=json.loads(q("SELECT count() AS rows,uniqExact(request_id) AS unique_rows,countIf(reply_text='synthetic answer') AS replies,countIf(startsWith(request_id,'pause-')) AS pause_rows FROM cliproxy.request_facts FORMAT JSONEachRow"))
    result=dict(recovery_seconds=time.monotonic()-began,remaining=remaining,**counts)
    print(json.dumps(result),flush=True)
    assert remaining==0 and counts['rows']==counts['unique_rows']==counts['replies']==2480 and counts['pause_rows']==60,result
finally:
    p.terminate()
    p.wait(timeout=30)
    log.close()

q('SYSTEM FLUSH LOGS')
stats=json.loads(q("SELECT max(memory_usage) AS peak_query_bytes,countIf(exception_code!=0) AS query_errors FROM system.query_log WHERE event_time_microseconds>=fromUnixTimestamp64Micro(%d) AND type!='QueryStart' AND query NOT LIKE '%%system.query_log%%' FORMAT JSONEachRow" % int(started*1000000)))
print(json.dumps(stats),flush=True)
assert stats['query_errors']==0,stats
