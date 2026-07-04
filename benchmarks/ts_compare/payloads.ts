// Canonical payload shapes — MUST match benchmarks/_payloads.py so the
// serialize/parse numbers are apples-to-apples.

import { randomBytes } from "node:crypto";

function nestedObject() {
  return {
    id: 12345,
    name: "Ada Lovelace",
    active: true,
    score: 98.6,
    tags: ["alpha", "beta", "gamma"],
    address: {
      street: "1 Analytical Engine Way",
      city: "London",
      geo: { lat: 51.5074, lng: -0.1278 },
    },
    roles: [{ k: "admin", w: 5 }, { k: "user", w: 1 }],
  };
}

const BLOB_RAW = new Uint8Array(randomBytes(1024 * 1024));

export function payloads(): Record<string, unknown> {
  return {
    small_scalar: 42,
    short_string: "hello world",
    nested_object: nestedObject(),
    large_array_10k_ints: Array.from({ length: 10000 }, (_, i) => i),
    large_string_1mb: "x".repeat(1024 * 1024),
    bytes_blob_1mb: BLOB_RAW,
    error_value: new Error("something went wrong at layer 7"),
  };
}

// The inner counts mirror the Python harness for comparable amortization.
export const INNER: Record<string, number> = {
  small_scalar: 20000,
  short_string: 20000,
  nested_object: 4000,
  large_array_10k_ints: 40,
  large_string_1mb: 200,
  bytes_blob_1mb: 200,
  error_value: 8000,
};
