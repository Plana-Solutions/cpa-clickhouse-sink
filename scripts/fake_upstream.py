import json, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_GET(self):
        self.send_response(200); self.send_header("content-type","application/json"); self.end_headers()
        self.wfile.write(json.dumps({"object":"list","data":[{"id":"fake-model","object":"model"}]}).encode())
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers.get("content-length",0))) or b"{}")
        last=body.get("messages",[{}])[-1].get("content","")
        reply="echo: "+str(last)[:80]
        usage={"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}
        if body.get("stream"):
            self.send_response(200); self.send_header("content-type","text/event-stream"); self.end_headers()
            for part in [reply[:10], reply[10:]]:
                c={"id":"c1","object":"chat.completion.chunk","model":"fake-model","choices":[{"index":0,"delta":{"content":part}}]}
                self.wfile.write(b"data: "+json.dumps(c).encode()+b"\n\n"); self.wfile.flush(); time.sleep(0.05)
            c={"id":"c1","object":"chat.completion.chunk","model":"fake-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":usage}
            self.wfile.write(b"data: "+json.dumps(c).encode()+b"\n\ndata: [DONE]\n\n")
        else:
            r={"id":"c1","object":"chat.completion","model":"fake-model","choices":[{"index":0,"message":{"role":"assistant","content":reply},"finish_reason":"stop"}],"usage":usage}
            out=json.dumps(r).encode()
            self.send_response(200); self.send_header("content-type","application/json"); self.send_header("content-length",str(len(out))); self.end_headers(); self.wfile.write(out)
ThreadingHTTPServer(("0.0.0.0",9000),H).serve_forever()
