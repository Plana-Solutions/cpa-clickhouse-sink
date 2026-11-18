import urllib.request, json, subprocess, os, hashlib, argparse
from pathlib import Path
p = argparse.ArgumentParser(description="Measure migration query read bytes on a disposable ClickHouse.")
p.add_argument("--url", required=True)
p.add_argument("--before", required=True)
p.add_argument("--after", required=True)
p.add_argument("--output", required=True)
a = p.parse_args()
url = a.url.rstrip("/") + "/"
def q(sql):
 r=urllib.request.urlopen(urllib.request.Request(url,data=sql.encode()),timeout=90)
 return r.read().decode()
results=[]
for n in [32,64,128]:
 for label,bin in [('before', a.before), ('after', a.after)]:
  db='sink_migration_measure_' + str(os.getpid())
  q('DROP DATABASE IF EXISTS '+db+' SYNC')
  schema=(Path(__file__).resolve().parent.parent / 'schema.sql').read_text().replace('{db}',db)
  for stmt in schema.split('\n;\n'):
   if stmt.strip():q(stmt)
  # Repeated context, 512 KiB per body, in four-row sorted parts with production settings.
  content=''.join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(8192))
  body=json.dumps({'messages':[{'role':'user','content':content}]},separators=(',',':'))
  for base in range(0,n,4):
   data=[]
   for i in range(base,base+4):
    ts='2026-01-%02d 00:%02d:%02d.000000000'%(1+i//86400,(i//60)%60,i%60)
    data.append(json.dumps(dict(request_id='m-%05d'%i,started_at=ts,completed_at=ts,request_body=body,request_headers={'session-id':'measure'},model='fixture')))
   q('INSERT INTO '+db+'.requests FORMAT JSONEachRow\n'+'\n'.join(data))
  env=dict(os.environ,CLICKHOUSE_URL=url,CLICKHOUSE_DB=db,CLICKHOUSE_USER='',CLICKHOUSE_PASSWORD='',CLICKHOUSE_PASSWORD_FILE='')
  subprocess.run([a.after,'--rebuild'],env=env,check=True,stdout=subprocess.DEVNULL)
  start=q('SELECT toString(now64(6))').strip()
  output=subprocess.check_output([bin,'migrate-bodies'],env=env,text=True)
  q('SYSTEM FLUSH LOGS')
  metrics=q("SELECT multiIf(replaceRegexpAll(query,'[[:space:]]','') LIKE '%SELECTrequest_id,started_atFROM%' OR replaceRegexpAll(query,'[[:space:]]','') LIKE '%SELECTDISTINCTrequest_id,started_atFROM%','candidates',query LIKE '%base64Encode(request_body) AS body,%','inline_read',query LIKE '%SELECT base64Encode(request_body) AS body_inline%' AND query LIKE '%body_blobs%'=0,'reader',query LIKE '%SELECT hex(hash) AS hash%','known_keys',query LIKE '%body_blobs%' AND query LIKE '%SELECT lineage%','blob_read',query LIKE 'INSERT%','insert',query LIKE 'DELETE%','delete','verify') AS step,count(),sum(read_bytes),max(read_bytes) FROM system.query_log WHERE type='QueryFinish' AND event_time_microseconds>=toDateTime64('"+start+"',6,'UTC') AND query LIKE '%"+db+"%' AND query NOT LIKE '%system.query_log%' GROUP BY step ORDER BY step FORMAT JSONEachRow")
  result=dict(rows=n,side=label,started=start,report=json.loads(output),steps=[json.loads(x) for x in metrics.splitlines()])
  results.append(result)
  print(json.dumps(result),flush=True)
  q('DROP DATABASE '+db+' SYNC')
open(a.output,'w').write(json.dumps(results,indent=2))
