// Cross-language wire-compatibility test: a TypeScript client calling a Go
// handler and vice versa, over a real NATS server.
//
// The rest of the suite runs against an in-memory NATS double, which cannot
// catch a divergence in what the two runtimes actually put on the wire — the
// protobuf payload, the subjects, and the error headers. This is the only test
// that does, and docs/ts-library.md promises that interop works with no
// configuration.
//
// Skips unless a NATS server and the Go toolchain are both available, which is
// what CI provides. Set PROTONATS_REQUIRE_NATS=1 to turn those skips into
// failures.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface, type Interface } from "node:readline";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { create, toBinary } from "@bufbuild/protobuf";
import { StringValueSchema } from "@bufbuild/protobuf/wkt";
import { connect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/nats-core";
import { ProtoNatsConn } from "./conn.js";
import { ProtoNatsError } from "./errors.js";
import { Registration } from "./registration.js";

const NATS_URL = process.env.NATS_URL ?? "nats://localhost:4222";
const REQUIRED = process.env.PROTONATS_REQUIRE_NATS !== undefined;

// Must match internal/interop/main.go.
const SUBJECT_GO_ECHO = "interop.go.echo";
const SUBJECT_GO_FAIL = "interop.go.fail";
const SUBJECT_GO_EMPTY_ERROR = "interop.go.emptyerror";
const SUBJECT_TS_ECHO = "interop.ts.echo";
const SUBJECT_TS_FAIL = "interop.ts.fail";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

function goAvailable(): boolean {
  return spawnSync("go", ["version"], { stdio: "ignore" }).status === 0;
}

async function natsReachable(): Promise<boolean> {
  try {
    const probe = await connect({ servers: NATS_URL });
    await probe.close();
    return true;
  } catch {
    return false;
  }
}

// Decided before the suite is collected, because a skip condition evaluated in
// beforeAll would come too late for skipIf. When the suite is required, it runs
// regardless so missing infrastructure fails instead of quietly vanishing.
const RUN = REQUIRED || ((await natsReachable()) && goAvailable());

/** Resolves once the Go fixture prints READY, so no call races its subscriptions. */
function startGoFixture(): Promise<{ proc: ChildProcessWithoutNullStreams; lines: Interface }> {
  const proc = spawn("go", ["run", "./internal/interop"], {
    cwd: repoRoot,
    env: { ...process.env, NATS_URL },
  });
  const lines = createInterface({ input: proc.stdout });

  return new Promise((resolvePromise, reject) => {
    let stderr = "";
    proc.stderr.on("data", (chunk) => (stderr += chunk));
    proc.on("exit", (code) =>
      reject(new Error(`go fixture exited early (code ${code}): ${stderr}`)),
    );
    lines.once("line", (line) => {
      if (line === "READY") resolvePromise({ proc, lines });
      else reject(new Error(`unexpected first line from go fixture: ${line}`));
    });
  });
}

/** Reads the next line the Go fixture prints. */
function nextLine(lines: Interface): Promise<string> {
  return new Promise((resolvePromise, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for the go fixture")), 10_000);
    lines.once("line", (line) => {
      clearTimeout(timer);
      resolvePromise(line);
    });
  });
}

describe.skipIf(!RUN)("Go interop", () => {
  let nc: NatsConnection;
  let pn: ProtoNatsConn;
  let go: { proc: ChildProcessWithoutNullStreams; lines: Interface };
  let reg: Registration;

  beforeAll(async () => {
    nc = await connect({ servers: NATS_URL });
    pn = new ProtoNatsConn(nc);

    // Register the TypeScript handlers before the Go fixture is asked to call
    // them. Both subscribe with the runtime's own subscribe(), so the queue
    // group and decoding path under test are the real ones.
    reg = new Registration();
    reg.addSubscription(
      pn.subscribe(SUBJECT_TS_ECHO, SUBJECT_TS_ECHO, undefined, StringValueSchema, async (_ctx, req) =>
        toBinary(StringValueSchema, create(StringValueSchema, { value: `${req.value} (from ts)` })),
      ),
    );
    reg.addSubscription(
      pn.subscribe(SUBJECT_TS_FAIL, SUBJECT_TS_FAIL, undefined, StringValueSchema, async () => {
        throw new ProtoNatsError(403, "denied from ts");
      }),
    );

    go = await startGoFixture();
  }, 60_000);

  afterAll(async () => {
    if (go) {
      go.proc.stdin.write("QUIT\n");
      go.proc.kill();
    }
    reg?.unsubscribe();
    await nc?.close();
  });

  it("calls a Go request/reply handler", async () => {
    const resp = await pn.request(
      SUBJECT_GO_ECHO,
      create(StringValueSchema, { value: "hello" }),
      StringValueSchema,
      StringValueSchema,
    );
    expect(resp.value).toBe("hello (from go)");
  });

  it("receives a Go handler error as a ProtoNatsError", async () => {
    await expect(
      pn.request(
        SUBJECT_GO_FAIL,
        create(StringValueSchema, { value: "x" }),
        StringValueSchema,
        StringValueSchema,
      ),
    ).rejects.toMatchObject({ code: 404, message: "gone from go" });
  });

  // Both runtimes key off the header's presence rather than its value; keying
  // off the value turns this into a successful, zero-valued response.
  it("receives a Go error carrying no message", async () => {
    const call = pn.request(
      SUBJECT_GO_EMPTY_ERROR,
      create(StringValueSchema, { value: "x" }),
      StringValueSchema,
      StringValueSchema,
    );
    await expect(call).rejects.toBeInstanceOf(ProtoNatsError);
    await expect(call).rejects.toMatchObject({ code: 418, message: "" });
  });

  it("serves a Go client in both the success and error case", async () => {
    go.proc.stdin.write("CALL\n");

    expect(await nextLine(go.lines)).toBe("ECHO OK hello (from ts)");
    expect(await nextLine(go.lines)).toBe("FAIL OK 403 denied from ts");
  }, 20_000);
});
