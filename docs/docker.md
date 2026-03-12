# Using the Docker Image

The ProtoNats Docker image bundles `protoc`, `protoc-gen-go`, `protoc-gen-protonats`, and `protoc-gen-protonats-ts` in a minimal scratch-based image. No local toolchain installation needed.

```bash
docker pull ghcr.io/mudomi/protonats
```

## Usage

Mount your project directory and pass protoc flags directly.

### Go only

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --go_out=/work/gen --go_opt=paths=source_relative \
  --protonats_out=/work/gen --protonats_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

### Go + TypeScript

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --go_out=/work/gen --go_opt=paths=source_relative \
  --protonats_out=/work/gen --protonats_opt=paths=source_relative \
  --protonats-ts_out=/work/gen-ts --protonats-ts_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

### TypeScript only

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --protonats-ts_out=/work/gen-ts --protonats-ts_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

The image's entrypoint is `protoc -I/usr/include`, so the well-known protobuf types and `protonats/options.proto` are already in the include path.

**Note:** The Docker image includes `protoc-gen-protonats-ts` but does **not** include `protoc-gen-es` (the Buf team's plugin that generates the `_pb.ts` message types). You'll need to run `protoc-gen-es` separately or install it in your project. See the [TypeScript Library Guide](ts-library.md) for details.

## GitHub Actions

### Go only

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

### Go + TypeScript

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

      - uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install protoc-gen-es
        run: npm install -g @bufbuild/protoc-gen-es

      - name: Generate Go + TypeScript code
        run: |
          mkdir -p gen/myapp/orders gen-ts/myapp/orders
          protoc \
            -I/usr/include \
            --go_out=gen --go_opt=paths=source_relative \
            --protonats_out=gen --protonats_opt=paths=source_relative \
            --es_out=gen-ts --es_opt=target=ts \
            --protonats-ts_out=gen-ts --protonats-ts_opt=paths=source_relative \
            -I proto \
            proto/myapp/orders/orders.proto
```

### Using `docker run` as a step

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

For TypeScript generation via `docker run`, add `--protonats-ts_out` and `--protonats-ts_opt` flags as shown above. You'll still need `protoc-gen-es` installed on the host (or in a separate step) for the `_pb.ts` files.

## Pinning a version

Use a specific release tag instead of `latest`:

```bash
docker pull ghcr.io/mudomi/protonats:1.0.0
```

```yaml
container:
  image: ghcr.io/mudomi/protonats:1.0.0
```
