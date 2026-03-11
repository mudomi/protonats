# Using the Docker Image

The ProtoNats Docker image bundles `protoc`, `protoc-gen-go`, and `protoc-gen-protonats` in a minimal scratch-based image. No local toolchain installation needed.

```bash
docker pull ghcr.io/mudomi/protonats
```

## Usage

Mount your project directory and pass protoc flags directly:

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --go_out=/work/gen --go_opt=paths=source_relative \
  --protonats_out=/work/gen --protonats_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

The image's entrypoint is `protoc -I/usr/include`, so the well-known protobuf types and `protonats/options.proto` are already in the include path.

## GitHub Actions

Use the image in a workflow to generate code and verify it compiles:

```yaml
name: Generate

on:
  push:
    branches: [main]
  pull_request:

jobs:
  generate:
    runs-on: ubuntu-latest

    container:
      image: ghcr.io/mudomi/protonats:latest

    steps:
      - uses: actions/checkout@v4

      - name: Generate protobuf code
        run: |
          mkdir -p gen/myapp/orders
          protoc \
            -I/usr/include \
            --go_out=gen --go_opt=paths=source_relative \
            --protonats_out=gen --protonats_opt=paths=source_relative \
            -I proto \
            proto/myapp/orders/orders.proto
```

If you only need code generation as a step (not the full job container), use `docker run` instead:

```yaml
name: Generate

on:
  push:
    branches: [main]
  pull_request:

jobs:
  generate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"

      - name: Generate protobuf code
        run: |
          mkdir -p gen/myapp/orders
          docker run --rm -v ${{ github.workspace }}:/work ghcr.io/mudomi/protonats:latest \
            --go_out=/work/gen --go_opt=paths=source_relative \
            --protonats_out=/work/gen --protonats_opt=paths=source_relative \
            -I /work/proto \
            /work/proto/myapp/orders/orders.proto

      - name: Verify generated code compiles
        run: go build ./gen/...
```

## Pinning a version

Use a specific release tag instead of `latest`:

```bash
docker pull ghcr.io/mudomi/protonats:1.0.0
```

```yaml
container:
  image: ghcr.io/mudomi/protonats:1.0.0
```
