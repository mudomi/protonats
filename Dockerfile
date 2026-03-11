FROM golang:1.23-alpine AS build

ARG PROTOC_VERSION=28.3
ARG TARGETARCH

RUN apk add --no-cache curl unzip \
    && ARCH=$(case ${TARGETARCH} in amd64) echo x86_64;; arm64) echo aarch_64;; *) echo ${TARGETARCH};; esac) \
    && curl -sLO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${ARCH}.zip" \
    && unzip -o "protoc-${PROTOC_VERSION}-linux-${ARCH}.zip" -d /out bin/protoc 'include/*' \
    && rm "protoc-${PROTOC_VERSION}-linux-${ARCH}.zip"

RUN CGO_ENABLED=0 go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && cp "$GOPATH/bin/protoc-gen-go" /out/bin/

COPY . /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /out/bin/protoc-gen-protonats ./cmd/protoc-gen-protonats

COPY proto/protonats /out/include/protonats

FROM scratch
COPY --from=build /out/bin/ /usr/bin/
COPY --from=build /out/include/ /usr/include/
ENV PATH=/usr/bin
WORKDIR /work
ENTRYPOINT ["protoc", "-I/usr/include"]
