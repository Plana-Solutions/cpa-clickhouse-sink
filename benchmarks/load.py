import urllib.request,pathlib,json,time,shutil
assert shutil.disk_usage("/System/Volumes/Data").free >= 3*(1<<30), "stop below 3 GiB free"
url='http://127.0.0.1:28123/'
def q(sql):
 return urllib.request.urlopen(urllib.request.Request(url,data=sql.encode()),timeout=180).read().decode()
repo=pathlib.Path(__file__).resolve().parents[1]
for s in (repo/'schema.sql').read_text().replace('{db}','cliproxy').split('\n;\n'):
 if s.strip(): q(s)
q("TRUNCATE TABLE cliproxy.requests")
for start in range(0,2400,100):
 sql='''INSERT INTO cliproxy.requests (request_id, trace_id, started_at, completed_at, ingested_at, source_format, request_body, response_chunks) SELECT toString(number), toString(number), now64(9)-INTERVAL 20 HOUR+toIntervalSecond(number*30), now64(9), now64(9)-INTERVAL 20 HOUR+toIntervalSecond(number*30), 'openai', concat('{"messages":[{"role":"user","content":"',repeat(hex(cityHash64(number)),6400+(number*7919)%%19000), '"}]}'), ['data: {"choices":[{"delta":{"content":"synthetic answer"}}]}','data: [DONE]'] FROM numbers(%d,%d)'''%(start,100)
 q(sql)
print(q('SELECT count(),sum(length(request_body)) FROM cliproxy.requests'))

assert q("SELECT dateDiff('second',min(ingested_at),max(ingested_at)) BETWEEN 71900 AND 72060 AND max(ingested_at)<=now64(9) FROM cliproxy.requests").strip() == '1'
