/**
 * Golden wire-format fixture generator (Cap'n Web parity plan, stream S0).
 *
 * Generates canonical wire strings from the REFERENCE TypeScript implementation
 * (capnweb, installed in this directory's node_modules) using the public
 * standalone `serialize()` / `deserialize()` helpers (serialize.ts:541,1034).
 *
 * Output: fixtures/golden_wire.json — committed; consumed by
 * packages/capnweb/tests/test_golden_conformance.py.
 *
 * Entry semantics:
 *   expect="roundtrip"    — Python must decode `wire` without error and re-encode
 *                           to something semantically equal to `reencoded_wire`
 *                           (the TS canonical serialize(deserialize(wire)) output).
 *   expect="decode_error" — the TS reference deserialize() THROWS on `wire`;
 *                           Python must also reject it.
 *   expect="encode_error" — the TS reference serialize() THROWS for the input
 *                           described by `input_description` (wire is ""); Python
 *                           must reject the equivalent input on encode.
 *
 * NOTE: stubs/streams/pipes cannot pass through the standalone helpers
 * (NullExporter/NullImporter throw) — those are covered by live interop tests,
 * not fixtures. Entries whose *reference* behavior is an incidental
 * NullImporter/NullExporter throw are annotated in `notes`.
 *
 * Run:  npx tsx gen_golden_fixtures.ts   (from packages/capnweb/tests/interop/)
 */

import { serialize, deserialize } from "capnweb";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));

const capnwebVersion: string = JSON.parse(
  readFileSync(join(HERE, "node_modules", "capnweb", "package.json"), "utf8"),
).version;

interface Entry {
  name: string;
  category: string;
  input_description: string;
  wire: string;
  expect: "roundtrip" | "decode_error" | "encode_error";
  notes: string;
  /** TS canonical re-encode: serialize(deserialize(wire)). Absent when the
   * re-encode itself throws (e.g. revived Request bodies become streams). */
  reencoded_wire?: string;
}

const entries: Entry[] = [];

function errMsg(e: unknown): string {
  return e instanceof Error ? `${e.constructor.name}: ${e.message}` : String(e);
}

/** Try TS canonical re-encode of an already-decoded value; annotate notes. */
function withReencode(entry: Entry, decoded: unknown): Entry {
  try {
    const re = serialize(decoded);
    entry.reencoded_wire = re;
    // Idempotence check: canonical form must be a fixed point.
    try {
      const re2 = serialize(deserialize(re));
      if (re2 !== re) {
        entry.notes += ` [WARNING: TS re-encode not idempotent: ${re} -> ${re2}]`;
      }
    } catch (e) {
      entry.notes += ` [WARNING: TS re-decode of canonical form threw: ${errMsg(e)}]`;
    }
  } catch (e) {
    entry.notes += ` [TS re-encode of decoded value threw: ${errMsg(e)}; no canonical re-encode]`;
  }
  return entry;
}

/** Add an entry by encoding a live JS value with the reference serialize(). */
function addValue(
  name: string,
  category: string,
  input_description: string,
  makeValue: () => unknown,
  notes = "",
): void {
  let value: unknown;
  try {
    value = makeValue();
  } catch (e) {
    entries.push({
      name,
      category,
      input_description,
      wire: "",
      expect: "encode_error",
      notes: `${notes} [constructing the input itself threw: ${errMsg(e)}]`.trim(),
    });
    return;
  }
  let wire: string;
  try {
    wire = serialize(value);
  } catch (e) {
    entries.push({
      name,
      category,
      input_description,
      wire: "",
      expect: "encode_error",
      notes: `${notes} [TS serialize() threw: ${errMsg(e)}]`.trim(),
    });
    return;
  }
  const entry: Entry = {
    name,
    category,
    input_description,
    wire,
    expect: "roundtrip",
    notes,
  };
  // Verify the reference can decode its own output, and capture canonical re-encode.
  try {
    const decoded = deserialize(wire);
    withReencode(entry, decoded);
  } catch (e) {
    entry.notes += ` [WARNING: TS deserialize() of own output threw: ${errMsg(e)}]`;
  }
  entries.push(entry);
}

/** Add an entry from a hand-crafted wire string; classify by TS deserialize(). */
function addWire(
  name: string,
  category: string,
  input_description: string,
  wire: string,
  notes = "",
): void {
  let decoded: unknown;
  try {
    decoded = deserialize(wire);
  } catch (e) {
    entries.push({
      name,
      category,
      input_description,
      wire,
      expect: "decode_error",
      notes: `${notes} [TS deserialize() threw: ${errMsg(e)}]`.trim(),
    });
    return;
  }
  const entry: Entry = {
    name,
    category,
    input_description,
    wire,
    expect: "roundtrip",
    notes: `${notes} [TS deserialize() accepted; decoded typeof=${typeof decoded}]`.trim(),
  };
  withReencode(entry, decoded);
  entries.push(entry);
}

function nest(depth: number): unknown {
  let v: unknown = 42;
  for (let i = 0; i < depth; i++) v = [v];
  return v;
}

// ---------------------------------------------------------------------------
// primitives
// ---------------------------------------------------------------------------
addValue("null", "primitives", "null", () => null);
addValue("true", "primitives", "true", () => true);
addValue("false", "primitives", "false", () => false);

// numbers
addValue("int_zero", "numbers", "0", () => 0);
addValue("int_positive", "numbers", "42", () => 42);
addValue("int_negative", "numbers", "-7", () => -7);
addValue("float_pi", "numbers", "3.14", () => 3.14);
addValue("float_negative", "numbers", "-2.718", () => -2.718);
addValue(
  "int_max_safe", "numbers", "Number.MAX_SAFE_INTEGER (2^53-1)",
  () => Number.MAX_SAFE_INTEGER,
);
addValue(
  "int_min_safe", "numbers", "Number.MIN_SAFE_INTEGER (-(2^53-1))",
  () => Number.MIN_SAFE_INTEGER,
);
addValue(
  "int_2pow53_as_number", "numbers", "2**53 as a JS number (just past the safe range)",
  () => 2 ** 53,
  "TS emits a raw JSON number; Python policy (matrix 02 row 8) auto-promotes |int|>2^53" +
    " to [\"bigint\",...] on emit — byte parity is NOT expected here, semantic parity is.",
);
addValue(
  "int_neg_2pow53_as_number", "numbers", "-(2**53) as a JS number",
  () => -(2 ** 53),
  "Same bigint auto-promotion caveat as int_2pow53_as_number.",
);
addValue("sentinel_inf", "numbers", "Infinity", () => Infinity);
addValue("sentinel_neg_inf", "numbers", "-Infinity", () => -Infinity);
addValue("sentinel_nan", "numbers", "NaN", () => NaN);

// strings
addValue("string_empty", "strings", "empty string", () => "");
addValue("string_ascii", "strings", "'hello'", () => "hello");
addValue("string_unicode", "strings", "unicode + emoji", () => "Hello, 世界! 🌍");
addValue(
  "string_escapes", "strings", "quotes/backslash/newline/tab control chars",
  () => "\"quoted\" 'single' back\\slash\nnewline\ttab",
);

// objects
addValue("object_empty", "objects", "{}", () => ({}));
addValue("object_simple", "objects", "{key: 'value'}", () => ({ key: "value" }));
addValue(
  "object_nested", "objects", "3-level nested object with mixed values",
  () => ({ a: { b: { c: [1, "two", 3.0], d: null }, e: true } }),
);
addValue(
  "object_with_undefined_prop", "objects", "{a: undefined, b: 1}",
  () => ({ a: undefined, b: 1 }),
  "Records how TS encodes an own-property whose value is undefined.",
);

// escaped arrays
addValue("array_empty", "arrays", "[] (empty literal array)", () => []);
addValue("array_flat", "arrays", "[1, 2, 3]", () => [1, 2, 3]);
addValue(
  "array_nested_depth3", "arrays", "[1, [2, [3, 'deep']]] — literal arrays nested to depth 3",
  () => [1, [2, [3, "deep"]]],
);
addValue(
  "array_of_objects", "arrays", "[{a:1}, {b:[2,3]}]",
  () => [{ a: 1 }, { b: [2, 3] }],
);
addValue(
  "array_stringlike_tag_shape", "arrays",
  "['bigint', '123'] as a LITERAL application array (must be escaped on wire)",
  () => ["bigint", "123"],
  "Escaping is what stops application data from being read as a special form.",
);

// depth edges (matrix 02 row 4: TS devaluate throws at depth >= 64; decode has no limit)
addValue("array_nest_depth63", "depth", "42 wrapped in 63 nested arrays", () => nest(63));
addValue("array_nest_depth64", "depth", "42 wrapped in 64 nested arrays", () => nest(64));
addValue("array_nest_depth65", "depth", "42 wrapped in 65 nested arrays", () => nest(65));

// sentinels
addValue("undefined", "sentinels", "undefined", () => undefined);

// bigint
addValue(
  "bigint_huge", "bigint", "123456789012345678901234567890n",
  () => 123456789012345678901234567890n,
);
addValue("bigint_negative", "bigint", "-42n", () => -42n);
addValue("bigint_2pow53", "bigint", "BigInt(2**53)", () => BigInt(2 ** 53));

// dates
addValue("date_valid", "dates", "new Date(1720000000000)", () => new Date(1720000000000));
addValue(
  "date_invalid", "dates", "new Date(NaN) — invalid date",
  () => new Date(NaN),
  "PR #152 (v0.8.0): invalid date encodes as [\"date\", null]. Matrix 02 row 9.",
);
addWire(
  "date_null_decode", "dates", "hand-crafted [\"date\", null] wire",
  '["date",null]',
  "Decode direction of the invalid-date form. Matrix 02 row 9.",
);

// bytes
{
  const hello = new TextEncoder().encode("hello");
  addValue("bytes_hello", "bytes", "Uint8Array of 'hello' (5 bytes)", () => hello);
  addValue("bytes_empty", "bytes", "Uint8Array of 0 bytes", () => new Uint8Array(0));
  addValue(
    "bytes_255", "bytes", "Uint8Array of 255 bytes, values 0..254",
    () => Uint8Array.from({ length: 255 }, (_, i) => i),
  );
}
addWire(
  "bytes_padded_b64_decode", "bytes", "hand-crafted [\"bytes\",\"aGVsbG8=\"] (PADDED base64)",
  '["bytes","aGVsbG8="]',
  "TS decoders tolerate padding even though TS emits unpadded (matrix 02 row 10).",
);

// errors
addValue("error_plain", "errors", "new Error('boom')", () => new Error("boom"));
addValue("error_typeerror", "errors", "new TypeError('bad type')", () => new TypeError("bad type"));
addValue(
  "error_custom_props", "errors", "Error('boom') with own props {customProp: {k: 1}, count: 2}",
  () => Object.assign(new Error("boom"), { customProp: { k: 1 }, count: 2 }),
  "PR #166 (v0.8.0) 5-element form: stack slot normalized to null when props present." +
    " Matrix 02 row 16.",
);
addValue(
  "error_with_cause", "errors", "new Error('outer', {cause: new TypeError('inner')})",
  () => new Error("outer", { cause: new TypeError("inner") }),
  "cause is captured even though non-enumerable. Matrix 02 row 16.",
);
addValue(
  "error_aggregate", "errors", "new AggregateError([Error('e1'), TypeError('e2')], 'many')",
  () => new AggregateError([new Error("e1"), new TypeError("e2")], "many"),
  "AggregateError.errors captured recursively. Matrix 02 row 16 / D4.",
);
addWire(
  "error_3elem_decode", "errors", "hand-crafted legacy 3-element error",
  '["error","RangeError","out of range"]',
  "Legacy 3-form. Name must survive decode (matrix 02 row 15).",
);
addWire(
  "error_4elem_stack_decode", "errors", "hand-crafted 4-element error with a stack string",
  '["error","Error","with stack","Error: with stack\\n    at <anonymous>:1:1"]',
  "4-form with stack.",
);
addWire(
  "error_unknown_name_decode", "errors", "hand-crafted error with a non-allowlisted name",
  '["error","SomeCustomError","custom"]',
  "TS revives non-ERROR_TYPES names as plain Error but PRESERVES .name — check reencode.",
);

// http types (PR #135, v0.9.0)
addValue(
  "headers_multi_mixed_case", "http",
  "new Headers([['X-Multi','a'],['X-multi','b'],['Content-Type','text/plain']])",
  () => new Headers([
    ["X-Multi", "a"],
    ["X-multi", "b"],
    ["Content-Type", "text/plain"],
  ]),
  "Multi-value + mixed-case keys; records TS's canonical case/merge behavior." +
    " Matrix 02 row 12.",
);
addValue(
  "request_get_default", "http", "new Request('https://example.com/') — default GET, no body",
  () => new Request("https://example.com/"),
  "Only non-default init fields are emitted (matrix 02 row 13).",
);
addValue(
  "request_post_body_headers", "http",
  "new Request POST with string body 'hello body' and headers {X-Custom: 1, Content-Type: text/plain}",
  () => new Request("https://example.com/api", {
    method: "POST",
    body: "hello body",
    headers: { "X-Custom": "1", "Content-Type": "text/plain" },
  }),
  "If the platform exposes the body only as a stream, standalone serialize() may throw" +
    " (pipes need a session) — actual behavior recorded by expect/notes.",
);
addWire(
  "request_post_string_body_decode", "http",
  "hand-crafted request wire with a plain-string body",
  '["request","https://example.com/api",{"method":"POST","headers":[["content-type","text/plain"],["x-custom","1"]],"body":"hello body"}]',
  "Decode-direction coverage of a string body (body whitelist: null|string|bytes|stream).",
);
addValue(
  "response_200_default", "http", "new Response(null) — 200, no body",
  () => new Response(null),
);
addValue(
  "response_404_statustext", "http", "new Response(null, {status: 404, statusText: 'Not Found'})",
  () => new Response(null, { status: 404, statusText: "Not Found" }),
);

// encode-rejects
addValue("function_encode", "unsupported", "() => 1 (a function)", () => () => 1);
addValue("symbol_encode", "unsupported", "Symbol('x')", () => Symbol("x"));
addValue(
  "class_instance_encode", "unsupported", "instance of a plain custom class (class Foo { x = 1 })",
  () => new (class Foo { x = 1 })(),
  "typeForRpc matches exact prototypes; custom classes are unsupported (matrix 02 row 5).",
);
addValue(
  "map_object_encode", "unsupported", "new Map([['a',1]])",
  () => new Map([["a", 1]]),
);

// decode-rejects / malformed special forms
addWire("bare_empty_array", "malformed", "bare [] (unescaped empty array)", "[]",
  "Matrix 02 row 3: neither an escaped array nor a recognized tag.");
addWire("bare_number_array", "malformed", "bare [1,2] (unescaped array)", "[1,2]",
  "Matrix 02 row 3.");
addWire("unknown_tag", "malformed", "unknown tag [\"zzz\", 1]", '["zzz",1]',
  "Matrix 02 row 3: unknown tags must be a hard error, not a plain list.");
addWire("date_bad_arg", "malformed", "[\"date\", \"x\"] (non-numeric date payload)",
  '["date","x"]', "Matrix 02 rows 3/9.");
addWire("error_bad_name_type", "malformed", "[\"error\", 123, \"m\"] (numeric name)",
  '["error",123,"m"]', "Matrix 02 rows 3/16.");
addWire("bigint_bad_payload", "malformed", "[\"bigint\", \"not-a-number\"]",
  '["bigint","not-a-number"]', "BigInt(str) must reject non-numeric strings.");

// security / prototype pollution
addWire(
  "pollution_plain_values", "security",
  "object with keys __proto__, toJSON, constructor mapped to plain values, plus a safe key",
  '{"__proto__":1,"toJSON":2,"constructor":3,"safe":4}',
  "TS drops keys in Object.prototype plus toJSON but still evaluates their values" +
    " (serialize.ts:1003-1023). Matrix 02 row 23. reencoded_wire records what survives.",
);
addWire(
  "pollution_key_with_export", "security",
  "{\"__proto__\": [\"export\", 0]} — dangerous key whose value embeds a capability",
  '{"__proto__":["export",0]}',
  "TS drops the key but STILL evaluates the value; with the standalone NullImporter that" +
    " import throws, so the reference classifies this as decode_error here. In a live" +
    " session the stub would be imported then released. Matrix 02 row 23.",
);
addWire(
  "export_without_session", "security", "[\"export\", 0] with no session",
  '["export",0]',
  "Standalone deserialize() must not fabricate stubs (NullImporter throws)." +
    " Session-path Python behavior differs; this pins the STANDALONE contract only.",
);

// ---------------------------------------------------------------------------

const out = {
  generator: "packages/capnweb/tests/interop/gen_golden_fixtures.ts",
  reference: "cloudflare/capnweb (TypeScript) — standalone serialize()/deserialize()",
  capnweb_version: capnwebVersion,
  generated_at: new Date().toISOString(),
  entry_count: entries.length,
  entries,
};

const fixturesDir = join(HERE, "fixtures");
mkdirSync(fixturesDir, { recursive: true });
const outPath = join(fixturesDir, "golden_wire.json");
writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");

const byExpect: Record<string, number> = {};
for (const e of entries) byExpect[e.expect] = (byExpect[e.expect] ?? 0) + 1;
console.log(`capnweb version: ${capnwebVersion}`);
console.log(`wrote ${entries.length} entries to ${outPath}`);
console.log(`by expect: ${JSON.stringify(byExpect)}`);
