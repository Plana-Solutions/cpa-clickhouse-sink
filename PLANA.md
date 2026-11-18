# Plana ARM64 build

This repository starts with `plugins/clickhouse-sink` from
api-token/Cli-Proxy-API-Management-Center at commit
`60b5048e8d1827902b73ed506ef39764d7a8cdf0`.
The upstream MIT license and attribution are retained in LICENSE and README.md.
The plugin id remains `clickhouse-sink` and its host schema remains 7.

Plana changes are the module import paths, release metadata, ARM64 release automation,
and `CLICKHOUSE_PASSWORD_FILE` support in the deriver. Capture, redaction, and SQL
behavior are inherited unchanged.

## Install from the plugin store

Add this source to CPA `plugins.store-sources`, or through CPAMP's plugin store manager.

```
https://raw.githubusercontent.com/Plana-Solutions/cpa-clickhouse-sink/main/registry.json
```

Refresh the store. Select **ClickHouse Sink (Plana ARM64)** from
`Plana-Solutions/cpa-clickhouse-sink`, version `0.1.0-plana.2`.
The release ZIP contains `clickhouse-sink.so`. Its name is
`clickhouse-sink_0.1.0-plana.2_linux_arm64.zip`.
`checksums.txt` lists ZIP hashes with bare filenames for CPA's installer.
`SHA256SUMS` covers all other assets and `checksums.txt`.
The deriver is a separate linux/arm64 binary. Building or publishing does not deploy it.

## Publish an update

Update the plugin version, registry version, and workflow dispatch fallback version.
Push `main`, run the workflow manually, and verify the build.
Push a tag matching `v*-plana.*` to publish after its checks pass.
The workflow uses a native Depot ARM64 runner, Debian Bookworm, and Go from go.mod.
It runs vet and race tests in both modules and calls the plugin's native C ABI
before packaging. Download the release and run `sha256sum --check SHA256SUMS`.

For a mounted deriver password, set `CLICKHOUSE_PASSWORD_FILE` to a file readable
by the distroless image's nonroot user. See deriver/README.md for precedence and errors.
