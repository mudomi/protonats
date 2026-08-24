# Docker Usage

The image bundles `protoc`, `protoc-gen-go`, `protoc-gen-protonats`, and
`protoc-gen-protonats-ts`, so no local toolchain is needed.

```bash
docker pull ghcr.io/mudomi/protonats
```

Pin a release tag (`ghcr.io/mudomi/protonats:1.0.0`) rather than `latest` in CI.

## Generating

Mount the project and pass protoc flags directly — the entrypoint is
`protoc -I/usr/include`, so the well-known types and `protonats/options.proto`
are already on the include path.

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --go_out=/work/gen --go_opt=paths=source_relative \
  --protonats_out=/work/gen --protonats_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

Add `--protonats-ts_out=/work/gen-ts --protonats-ts_opt=paths=source_relative`
for TypeScript. The image does **not** include `protoc-gen-es`, which generates
the `_pb.ts` message types — install that separately with npm. See the
[TypeScript Library Guide](ts-library.md).

## GitHub Actions

Run the job inside the image:

```yaml
jobs:
  generate:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/mudomi/protonats:latest
    steps:
      - uses: actions/checkout@v4
      - run: |
          mkdir -p gen
          protoc \
            --go_out=gen --go_opt=paths=source_relative \
            --protonats_out=gen --protonats_opt=paths=source_relative \
            -I proto \
            proto/myapp/orders/orders.proto
```

For TypeScript as well, add `actions/setup-node`, `npm install -g
@bufbuild/protoc-gen-es`, and the two `--es_out` / `--protonats-ts_out` flags.

If you would rather keep the job on the runner, use `docker run -v
${{ github.workspace }}:/work …` as a single step instead, then
`go build ./gen/...` to verify the result compiles.
