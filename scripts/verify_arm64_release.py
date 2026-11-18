import ctypes
import json


class Buffer(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("length", ctypes.c_size_t)]


Call = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(Buffer))
Free = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_size_t)
Shutdown = ctypes.CFUNCTYPE(None)


class PluginAPI(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("call", Call), ("free", Free), ("shutdown", Shutdown)]


plugin = ctypes.CDLL("./dist/clickhouse-sink.so")
plugin.cliproxy_plugin_init.argtypes = [ctypes.c_void_p, ctypes.POINTER(PluginAPI)]
plugin.cliproxy_plugin_init.restype = ctypes.c_int
api = PluginAPI()
assert plugin.cliproxy_plugin_init(None, ctypes.byref(api)) == 0
assert api.version == 1
out = Buffer()
assert api.call(b"plugin.register", b"{}", 2, ctypes.byref(out)) == 0
try:
    result = json.loads(ctypes.string_at(out.ptr, out.length))
    assert result["ok"] is True
    assert result["result"]["schema_version"] == 7
    assert result["result"]["metadata"]["Name"] == "clickhouse-sink"
    assert result["result"]["metadata"]["Version"] == "0.1.0-plana.1"
finally:
    api.free(out.ptr, out.length)
    api.shutdown()

print("ARM64 native plugin ABI verified, clickhouse-sink schema 7")
