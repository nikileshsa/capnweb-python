// Stream throughput (TS reference) over the in-process MessageChannel,
// matching benchmarks/bench_streams.py: a server method returns a
// ReadableStream of 64 KiB byte chunks; the client drains it. Reports MB/s.

import { MessageChannel } from "node:worker_threads";
import { newMessagePortRpcSession, RpcTarget } from "capnweb";
import { Result } from "./harness.js";

const CHUNK = new Uint8Array(64 * 1024).fill(0xab);
const TOTAL = 64 * 1024 * 1024;

class StreamService extends RpcTarget {
  produce(): ReadableStream {
    const nChunks = TOTAL / CHUNK.length;
    let i = 0;
    return new ReadableStream({
      pull(controller) {
        if (i < nChunks) { controller.enqueue(CHUNK); i++; }
        else controller.close();
      },
    });
  }
}

async function pumpOnce(): Promise<number> {
  const { port1, port2 } = new MessageChannel();
  newMessagePortRpcSession(port2 as any, new StreamService());
  const client = newMessagePortRpcSession(port1 as any) as any;
  const t0 = process.hrtime.bigint();
  const stream: ReadableStream = await client.produce();
  const reader = stream.getReader();
  let received = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    received += (value as Uint8Array).length;
  }
  const t1 = process.hrtime.bigint();
  port1.close(); port2.close();
  if (received !== TOTAL) throw new Error(`got ${received} != ${TOTAL}`);
  return Number(t1 - t0);
}

export async function run(): Promise<Result[]> {
  await pumpOnce(); // warmup
  const samples: number[] = [];
  for (let i = 0; i < 7; i++) samples.push(await pumpOnce());
  samples.sort((a, b) => a - b);
  const median = samples[Math.floor(samples.length / 2)];
  const p95 = samples[Math.min(samples.length - 1, Math.round(0.95 * (samples.length - 1)))];
  const mbps = (TOTAL / (1024 * 1024)) / (median / 1e9);
  return [{
    name: "stream/64MiB_msgport", group: "stream.throughput",
    samples: samples.length, inner: 1, bytes: TOTAL,
    ns_median: median, ns_p95: p95,
    ns_mean: samples.reduce((a, b) => a + b, 0) / samples.length,
    ns_stddev: 0, ns_min: samples[0], ops_per_sec: 0,
    extra: { MB_per_sec: Math.round(mbps * 10) / 10, MiB_total: TOTAL / (1024 * 1024), chunk_bytes: CHUNK.length },
  }];
}
