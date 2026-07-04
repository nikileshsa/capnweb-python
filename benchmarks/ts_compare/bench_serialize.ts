// Serialize + parse micro-benchmarks (TS reference), matching
// benchmarks/bench_serialize.py. Uses capnweb's standalone serialize/
// deserialize (JSON string both directions) so the comparison is fair.

import { serialize, deserialize } from "capnweb";
import { bench, Result } from "./harness.js";
import { payloads, INNER } from "./payloads.js";

export function run(): Result[] {
  const results: Result[] = [];
  const shapes = payloads();

  for (const [name, value] of Object.entries(shapes)) {
    const inner = INNER[name];
    const wire = serialize(value);
    const nbytes = Buffer.byteLength(wire, "utf8");

    results.push(
      bench(`serialize/${name}`, "codec.serialize", () => { serialize(value); },
        { inner, bytes: nbytes, extra: { wire_bytes: nbytes } }),
    );
    results.push(
      bench(`deserialize/${name}`, "codec.deserialize", () => { deserialize(wire); },
        { inner, bytes: nbytes, extra: { wire_bytes: nbytes } }),
    );
    results.push(
      bench(`roundtrip/${name}`, "codec.roundtrip", () => { deserialize(serialize(value)); },
        { inner, bytes: nbytes, extra: { wire_bytes: nbytes } }),
    );
  }
  return results;
}
