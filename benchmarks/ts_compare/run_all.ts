// Run the TS reference suite and emit JSON + a table, mirroring
// benchmarks/run_all.py. Run from this directory (node_modules is symlinked to
// the interop install of capnweb 0.9.0 + tsx):
//
//   npx tsx run_all.ts               # all
//   npx tsx run_all.ts serialize     # subset by keyword
//   npx tsx run_all.ts --json out.json

import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import { execSync } from "node:child_process";
import * as os from "node:os";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __here = dirname(fileURLToPath(import.meta.url));
import { Result, printTable } from "./harness.js";
import { run as runSerialize } from "./bench_serialize.js";
import { run as runRpc } from "./bench_rpc.js";
import { run as runStreams } from "./bench_streams.js";

function env(): Record<string, unknown> {
  let cpu = "";
  try { cpu = execSync("sysctl -n machdep.cpu.brand_string").toString().trim(); } catch { /* non-macos */ }
  let capnwebVersion = "?";
  try {
    capnwebVersion = JSON.parse(
      readFileSync(join(__here, "node_modules/capnweb/package.json"), "utf8"),
    ).version;
  } catch { /* ignore */ }
  return {
    timestamp: new Date().toISOString(),
    node_version: process.version,
    v8_version: process.versions.v8,
    platform: `${os.type()} ${os.release()}`,
    arch: os.arch(),
    cpu_brand: cpu,
    ncpu: os.cpus().length,
    capnweb_version: capnwebVersion,
  };
}

async function main() {
  const args = process.argv.slice(2);
  let outPath = join(__here, "..", "results", "ts_baseline.json");
  const ji = args.indexOf("--json");
  if (ji >= 0) { outPath = args[ji + 1]; args.splice(ji, 2); }
  const filter = args[0];

  const groups: Record<string, () => Result[] | Promise<Result[]>> = {
    serialize: runSerialize,
    rpc: runRpc,
    streams: runStreams,
  };

  let results: Result[] = [];
  for (const [key, fn] of Object.entries(groups)) {
    if (filter && !key.includes(filter)) continue;
    process.stderr.write(`[run_all] running ${key} ...\n`);
    results = results.concat(await fn());
  }

  results.sort((a, b) => (a.group + a.name).localeCompare(b.group + b.name));
  printTable(results);
  console.log();
  for (const r of results) {
    if (Object.keys(r.extra).length) console.log(`  ${r.name}: ${JSON.stringify(r.extra)}`);
  }

  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify({ env: env(), results }, null, 2));
  process.stderr.write(`\n[run_all] wrote ${outPath}\n`);
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
