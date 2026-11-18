"""Compare restore memory on disposable, capped ClickHouse 25.8 containers."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

old, new = map(os.path.realpath, sys.argv[1:3])
schema = Path(sys.argv[3]).read_text().replace("{db}", "cliproxy")
prefix = "sink-memory-" + str(os.getpid())
image = "clickhouse/clickhouse-server:25.8@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77"
def docker(*args, **kwargs):
    return subprocess.check_output(["sudo", "-n", "docker", *args], **kwargs).decode().strip()
results = []
for side, binary in [("before", old), ("after", new)]:
    if os.environ.get("MEMORY_AFTER_ONLY") == "1" and side == "before":
        continue
    name = prefix + "-" + side
    network = name
    samples = []
    proc = None
    start = time.monotonic()
    def query(sql):
        return docker("exec", name, "clickhouse-client", "--query", sql)
    def memory():
        return json.loads(query("SELECT (SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident') resident,(SELECT value FROM system.asynchronous_metrics WHERE metric='TrackedMemory') tracked,(SELECT toFloat64(value) FROM system.server_settings WHERE name='max_server_memory_usage') lim,(SELECT count() FROM cliproxy.requests WHERE body_sha256=repeat(char(0),32)) restored FORMAT JSONEachRow"))
    try:
        docker("network", "create", "--internal", network)
        docker("run", "-d", "--name", name, "--network", network, "--memory", "1503238553", "--cpus", "1", "-e", "CLICKHOUSE_SKIP_USER_SETUP=1", image)
        for attempt in range(60):
            try:
                query("SELECT 1")
                break
            except subprocess.CalledProcessError:
                time.sleep(1)
        for stmt in schema.split("\n;\n"):
            if stmt.strip():
                query(stmt)
        query("INSERT INTO cliproxy.body_blobs (lineage,hash,data,first_seen) SELECT 1,substring(SHA256(body),1,16),body,now64(9) FROM (SELECT concat('{\"messages\":[{\"role\":\"user\",\"content\":\"',repeat('synthetic context ',16384),'\"}]}') AS body)")
        query("INSERT INTO cliproxy.requests (request_id,started_at,completed_at,model,lineage,body_len,body_sha256,body_residual,body_ref_offsets,body_refs) SELECT concat('memory-',toString(number)),toDateTime64('2026-01-01',9,'UTC')+toIntervalSecond(number),toDateTime64('2026-01-01',9,'UTC')+toIntervalSecond(number),'synthetic',1,length(data),SHA256(data),'',[0],[hash] FROM numbers(600) AS n CROSS JOIN cliproxy.body_blobs AS b")
        query("SYSTEM JEMALLOC PURGE")
        initial = memory()
        logpath = Path(tempfile.gettempdir()) / (name + ".log")
        with logpath.open("w") as out:
            proc = subprocess.Popen(["sudo", "-n", "docker", "run", "--name", name+"-runner", "--network", network, "-v", binary + ":/deriver:ro", "-e", "CLICKHOUSE_URL=http://"+name+":8123", "-e", "CLICKHOUSE_DB=cliproxy", "cpa-local/clickhouse-deriver:60b5048", "restore-bodies"], stdout=out, stderr=subprocess.STDOUT)
            while True:
                sample = memory()
                sample["elapsed"] = round(time.monotonic()-start, 2)
                samples.append(sample)
                print(json.dumps(dict(side=side, **sample)), flush=True)
                if proc.poll() is not None or sample["resident"] >= sample["lim"]*.85 or time.monotonic()-start > 600:
                    break
                time.sleep(10)
            if proc.poll() is None:
                docker("rm", "-fv", name+"-runner")
                proc.wait()
        if side == "after" and proc.returncode == 0:
            query("INSERT INTO cliproxy.request_facts (request_id,started_at,ingested_at) SELECT request_id,started_at,ingested_at FROM cliproxy.requests")
            migration_name = name + "-migrate"
            def migration():
                return subprocess.Popen(["sudo","-n","docker","run","--name",migration_name,"--network",network,"-v",binary+":/deriver:ro","-e","CLICKHOUSE_URL=http://"+name+":8123","-e","CLICKHOUSE_DB=cliproxy","cpa-local/clickhouse-deriver:60b5048","migrate-bodies"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            migrating = migration()
            cursor = ""
            for attempt in range(90):
                try:
                    cursor = query("SELECT request_id FROM cliproxy.body_migration_progress ORDER BY saved_at DESC LIMIT 1")
                except subprocess.CalledProcessError:
                    pass
                if cursor:
                    break
                time.sleep(1)
            assert cursor, "no verified migration checkpoint"
            docker("rm","-fv",migration_name)
            migrating.wait()
            print("migration killed after durable cursor=" + cursor,flush=True)
            migrating = migration()
            assert migrating.wait(timeout=600) == 0, "resume failed"
            docker("rm","-fv",migration_name)
            assert query("SELECT countIf(body_sha256=repeat(char(0),32)),count(),uniqExact(request_id) FROM cliproxy.requests") == "0\t600\t600"
            checker = '\nimport json,sys,urllib.request\nurl="http://"+sys.argv[1]+":8123/?http_write_exception_in_output_format=0&max_threads=1"\ndef q(sql):\n    with urllib.request.urlopen(urllib.request.Request(url,data=sql.encode()),timeout=60) as r:\n        return r.read().decode().strip()\nrows=[json.loads(line) for line in q("SELECT request_id,hex(body_sha256) sha FROM cliproxy.requests FORMAT JSONEachRow").splitlines()]\nassert len(rows)==600\nfor row in rows:\n    assert q("SELECT body_status,hex(SHA256(request_body)) FROM cliproxy.raw_request(id=\'"+row["request_id"]+"\')") == "complete\\t"+row["sha"],row["request_id"]\nprint("600/600 SQL bodies match the original SHA-256")\n'
            print(docker("run","--rm","--network",network,"python:3.12-slim","python","-c",checker,name),flush=True)
            print("migration resumed, 600/600 byte-exact request bodies verified",flush=True)
        log = logpath.read_text()
        # Logs contain only synthetic data and sanitized error messages.
        result = dict(side=side, initial=initial, samples=samples, exit=proc.returncode, log=log)
        results.append(result)
        logpath.unlink()
    finally:
        for container in [name+"-migrate", name+"-runner", name]:
            subprocess.run(["sudo","-n","docker","rm","-fv",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(["sudo","-n","docker","network","rm",network],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
print("RESULT=" + json.dumps(results), flush=True)
