import ctypes
import json
import os
import sys


class Buffer(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("length", ctypes.c_size_t)]


Call = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(Buffer))
Free = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_size_t)
Shutdown = ctypes.CFUNCTYPE(None)


class PluginAPI(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("call", Call), ("free", Free), ("shutdown", Shutdown)]


plugin = ctypes.CDLL(os.path.abspath(sys.argv[1]))
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
    assert result["result"]["metadata"]["Version"] == os.environ["PLUGIN_VERSION"]
finally:
    api.free(out.ptr, out.length)
    api.shutdown()

print("Native plugin ABI verified, schema 7, version " + os.environ["PLUGIN_VERSION"])
