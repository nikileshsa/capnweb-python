// Timing harness mirroring benchmarks/_harness.py: warmup + samples,
// median/p95/stddev, process.hrtime.bigint() (nanoseconds, monotonic).

export interface Result {
  name: string;
  group: string;
  ns_median: number;
  ns_p95: number;
  ns_mean: number;
  ns_stddev: number;
  ns_min: number;
  ops_per_sec: number;
  samples: number;
  inner: number;
  bytes: number;
  extra: Record<string, unknown>;
}

function stats(perOpNs: number[]) {
  const s = [...perOpNs].sort((a, b) => a - b);
  const n = s.length;
  const median = n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
  const p95 = s[Math.min(n - 1, Math.round(0.95 * (n - 1)))];
  const mean = s.reduce((a, b) => a + b, 0) / n;
  const variance = s.reduce((a, b) => a + (b - mean) ** 2, 0) / n;
  return {
    ns_median: median,
    ns_p95: p95,
    ns_mean: mean,
    ns_stddev: Math.sqrt(variance),
    ns_min: s[0],
    ops_per_sec: median ? 1e9 / median : 0,
  };
}

export function bench(
  name: string,
  group: string,
  fn: () => void,
  opts: { inner?: number; samples?: number; warmup?: number; bytes?: number; extra?: Record<string, unknown> } = {},
): Result {
  const inner = opts.inner ?? 1;
  const samples = opts.samples ?? 60;
  const warmup = opts.warmup ?? 10;
  for (let i = 0; i < warmup; i++) fn();
  const perOp: number[] = [];
  for (let s = 0; s < samples; s++) {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < inner; i++) fn();
    const t1 = process.hrtime.bigint();
    perOp.push(Number(t1 - t0) / inner);
  }
  return {
    name, group, samples, inner, bytes: opts.bytes ?? 0,
    extra: opts.extra ?? {}, ...stats(perOp),
  };
}

export async function benchAsync(
  name: string,
  group: string,
  factory: () => Promise<void>,
  opts: {
    inner?: number; samples?: number; warmup?: number; bytes?: number;
    extra?: Record<string, unknown>;
    setup?: () => Promise<void>; teardown?: () => Promise<void>;
  } = {},
): Promise<Result> {
  const inner = opts.inner ?? 1;
  const samples = opts.samples ?? 40;
  const warmup = opts.warmup ?? 8;
  if (opts.setup) await opts.setup();
  for (let i = 0; i < warmup; i++) await factory();
  const perOp: number[] = [];
  for (let s = 0; s < samples; s++) {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < inner; i++) await factory();
    const t1 = process.hrtime.bigint();
    perOp.push(Number(t1 - t0) / inner);
  }
  if (opts.teardown) await opts.teardown();
  return {
    name, group, samples, inner, bytes: opts.bytes ?? 0,
    extra: opts.extra ?? {}, ...stats(perOp),
  };
}

export function fmtNs(ns: number): string {
  if (ns < 1000) return `${ns.toFixed(0)} ns`;
  if (ns < 1e6) return `${(ns / 1e3).toFixed(2)} us`;
  return `${(ns / 1e6).toFixed(3)} ms`;
}

export function printTable(results: Result[]): void {
  let group: string | null = null;
  for (const r of results) {
    if (r.group !== group) {
      group = r.group;
      console.log(`\n== ${group} ==`);
      console.log(
        `${"workload".padEnd(40)} ${"median".padStart(12)} ${"p95".padStart(12)} ${"ops/sec".padStart(14)}`,
      );
    }
    console.log(
      `${r.name.padEnd(40)} ${fmtNs(r.ns_median).padStart(12)} ${fmtNs(r.ns_p95).padStart(12)} ${r.ops_per_sec.toFixed(0).padStart(14)}`,
    );
  }
}
