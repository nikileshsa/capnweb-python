// End-to-end RPC benchmarks (TS reference) over an in-process MessageChannel,
// matching benchmarks/bench_rpc.py. NOTE the transport asymmetry: the TS
// MessagePort transport is "structuredClonable" (postMessage clones the value
// tree — NO JSON stringify per frame), whereas the Python default pipe is the
// "string" level (JSON per frame). Compare TS here against Python's
// roundtrip/*_jsonCompatible variant for the fair head-to-head, and against
// the "string" variant to see the JSON-per-frame cost.

import { MessageChannel } from "node:worker_threads";
import { newMessagePortRpcSession, RpcTarget } from "capnweb";
import { benchAsync, Result } from "./harness.js";

class BenchService extends RpcTarget {
  add(a: number, b: number) { return a + b; }
  echo(x: unknown) { return x; }
  chain(n: number) { return { n: (n ?? 0) + 1 }; }
}

function makePair(): { client: any; close: () => void } {
  const { port1, port2 } = new MessageChannel();
  newMessagePortRpcSession(port2 as any, new BenchService());
  const client = newMessagePortRpcSession(port1 as any) as any;
  return { client, close: () => { port1.close(); port2.close(); } };
}

export async function run(): Promise<Result[]> {
  const results: Result[] = [];

  // ---- round-trip ----
  {
    let pair = makePair();
    results.push(await benchAsync(
      "roundtrip/add(2,3)", "rpc.roundtrip",
      async () => { await pair.client.add(2, 3); },
      { inner: 200, samples: 40,
        setup: async () => { pair = makePair(); },
        teardown: async () => { pair.close(); } },
    ));
    results.push(await benchAsync(
      "roundtrip/echo(nested)", "rpc.roundtrip",
      async () => { await pair.client.echo({ id: 1, tags: [1, 2, 3], ok: true }); },
      { inner: 200, samples: 40,
        setup: async () => { pair = makePair(); },
        teardown: async () => { pair.close(); } },
    ));
  }

  // ---- pipelining vs sequential ----
  {
    let pair = makePair();
    results.push(await benchAsync(
      "pipeline/5deep_pipelined", "rpc.pipeline",
      async () => {
        const c = pair.client;
        let p = c.chain(0);
        p = c.chain(p.n);
        p = c.chain(p.n);
        p = c.chain(p.n);
        p = c.chain(p.n);
        await p;
      },
      { inner: 100, samples: 40,
        setup: async () => { pair = makePair(); },
        teardown: async () => { pair.close(); } },
    ));
    results.push(await benchAsync(
      "pipeline/5deep_sequential", "rpc.pipeline",
      async () => {
        const c = pair.client;
        let n = 0;
        for (let i = 0; i < 5; i++) { const r = await c.chain(n); n = r.n; }
      },
      { inner: 100, samples: 40,
        setup: async () => { pair = makePair(); },
        teardown: async () => { pair.close(); } },
    ));
  }

  // ---- batch: 1000 calls in one turn ----
  {
    const N = 1000;
    let pair = makePair();
    const r = await benchAsync(
      "batch/1000_calls_msgport", "rpc.batch",
      async () => {
        const c = pair.client;
        const ps: Promise<number>[] = [];
        for (let i = 0; i < N; i++) ps.push(c.add(i, 1));
        await Promise.all(ps);
      },
      { inner: 1, samples: 30,
        setup: async () => { pair = makePair(); },
        teardown: async () => { pair.close(); } },
    );
    r.extra["calls_per_sec"] = (N * 1e9) / r.ns_median;
    r.extra["n_calls"] = N;
    results.push(r);
  }

  // ---- fan-out: 100 sessions x 20 calls ----
  {
    const SESSIONS = 100, CALLS = 20;
    let pairs: { client: any; close: () => void }[] = [];
    const r = await benchAsync(
      "fanout/100sess_x20calls", "rpc.fanout",
      async () => {
        await Promise.all(pairs.map(async (p) => {
          for (let i = 0; i < CALLS; i++) await p.client.add(i, 1);
        }));
      },
      { inner: 1, samples: 25, warmup: 5,
        setup: async () => { pairs = Array.from({ length: SESSIONS }, makePair); },
        teardown: async () => { for (const p of pairs) p.close(); } },
    );
    const total = SESSIONS * CALLS;
    r.extra["calls_per_sec"] = (total * 1e9) / r.ns_median;
    r.extra["total_calls"] = total;
    r.extra["sessions"] = SESSIONS;
    results.push(r);
  }

  return results;
}
