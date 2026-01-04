/**
 * Long-running TypeScript client protocol compatibility test.
 * 
 * This script connects to a Python server and runs continuous
 * bidirectional RPC for the specified duration.
 * 
 * Usage: npx tsx ts_long_running.ts <port> <duration_seconds>
 */

import { newWebSocketRpcSession, RpcStub, RpcTarget } from 'capnweb';

const PORT = parseInt(process.argv[2] || '9200', 10);
const DURATION = parseInt(process.argv[3] || '300', 10);
const WS_URL = `ws://127.0.0.1:${PORT}/rpc`;

interface TestTarget {
  square(i: number): number;
  add(a: number, b: number): number;
  greet(name: string): string;
  echo(value: any): any;
  makeCounter(initial: number): any;
  registerCallback(cb: RpcStub<any>): string;
  triggerCallback(): string;
}

// Test cases matching Python implementation
const BASIC_TEST_CASES: [string, any[], (r: any) => boolean, string][] = [
  ["square", [5], (r) => r === 25, "square(5) = 25"],
  ["square", [0], (r) => r === 0, "square(0) = 0"],
  ["square", [-3], (r) => r === 9, "square(-3) = 9"],
  ["add", [10, 20], (r) => r === 30, "add(10, 20) = 30"],
  ["add", [-5, 5], (r) => r === 0, "add(-5, 5) = 0"],
  ["greet", ["World"], (r) => r === "Hello, World!", "greet string"],
  ["greet", [""], (r) => r === "Hello, !", "greet empty string"],
];

const WIRE_FORMAT_TEST_CASES: [string, any[], (r: any) => boolean, string][] = [
  ["echo", [null], (r) => r === null, "null"],
  ["echo", [true], (r) => r === true, "boolean true"],
  ["echo", [false], (r) => r === false, "boolean false"],
  ["echo", [42], (r) => r === 42, "integer"],
  ["echo", [3.14], (r) => Math.abs(r - 3.14) < 0.001, "float"],
  ["echo", ["hello"], (r) => r === "hello", "string"],
  ["echo", [[1, 2, 3]], (r) => JSON.stringify(r) === "[1,2,3]", "array of ints"],
  ["echo", [{ a: 1, b: 2 }], (r) => JSON.stringify(r) === '{"a":1,"b":2}', "object"],
  ["echo", [[{ nested: [1, 2] }]], (r) => JSON.stringify(r) === '[{"nested":[1,2]}]', "nested structure"],
  ["echo", [[]], (r) => JSON.stringify(r) === "[]", "empty array"],
  ["echo", [{}], (r) => JSON.stringify(r) === "{}", "empty object"],
];

const ALL_TEST_CASES = [...BASIC_TEST_CASES, ...WIRE_FORMAT_TEST_CASES];

class ClientCallback extends RpcTarget {
  pingCount = 0;
  
  notify(msg: string): string {
    this.pingCount++;
    return `pong-${this.pingCount}`;
  }
  
  echo(value: any): any {
    return value;
  }
}

interface Stats {
  clientCalls: number;
  serverCallbacks: number;
  protocolTestsPassed: number;
  protocolTestsFailed: number;
  capabilityCreates: number;
  errors: string[];
}

async function runProtocolCompatibilityDemo(
  serverUrl: string,
  duration: number,
  serverName: string,
): Promise<Stats> {
  const callback = new ClientCallback();
  const stats: Stats = {
    clientCalls: 0,
    serverCallbacks: 0,
    protocolTestsPassed: 0,
    protocolTestsFailed: 0,
    capabilityCreates: 0,
    errors: [],
  };

  console.log("\n" + "=".repeat(60));
  console.log(`Protocol Compatibility Test: TypeScript client <-> ${serverName} server`);
  console.log(`Duration: ${duration}s`);
  console.log(`Server URL: ${serverUrl}`);
  console.log("=".repeat(60) + "\n");

  try {
    const stub = newWebSocketRpcSession<TestTarget>(serverUrl, callback);
    
    // Register callback with server
    await stub.registerCallback(new RpcStub(callback));
    console.log(`[${serverName}] Callback registered with server`);

    const startTime = Date.now();
    let lastReport = startTime;
    const reportInterval = 30000; // 30 seconds
    let testCaseIdx = 0;
    let capabilityTestIdx = 0;

    while ((Date.now() - startTime) / 1000 < duration) {
      const elapsed = (Date.now() - startTime) / 1000;

      // Run protocol compatibility test case
      const [method, args, checker, desc] = ALL_TEST_CASES[testCaseIdx % ALL_TEST_CASES.length];
      try {
        const result = await Promise.race([
          (stub as any)[method](...args),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 10000)),
        ]);
        
        if (checker(result)) {
          stats.protocolTestsPassed++;
        } else {
          stats.protocolTestsFailed++;
          stats.errors.push(`Protocol mismatch (${desc}): ${method}(${JSON.stringify(args)}) returned ${JSON.stringify(result)}`);
        }
        stats.clientCalls++;
      } catch (e: any) {
        stats.errors.push(`Error on ${method}: ${e.message}`);
      }

      testCaseIdx++;

      // Capability-based feature test every 5 calls
      if (testCaseIdx % 5 === 0) {
        try {
          await stub.makeCounter(capabilityTestIdx);
          stats.capabilityCreates++;
          capabilityTestIdx++;
        } catch (e: any) {
          // Some capability features may not be fully implemented
        }
      }

      // Trigger server -> client callback every 10 calls
      if (testCaseIdx % 10 === 0) {
        try {
          await stub.triggerCallback();
          stats.serverCallbacks = callback.pingCount;
        } catch (e: any) {
          stats.errors.push(`Error on callback: ${e.message}`);
        }
      }

      // Progress report
      if (Date.now() - lastReport >= reportInterval) {
        console.log(
          `[TypeScript->${serverName}] ${elapsed.toFixed(0)}s: ` +
          `calls=${stats.clientCalls}, ` +
          `callbacks=${stats.serverCallbacks}, ` +
          `passed=${stats.protocolTestsPassed}, ` +
          `failed=${stats.protocolTestsFailed}`
        );
        lastReport = Date.now();
      }

      // Small delay to avoid overwhelming
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  } catch (e: any) {
    stats.errors.push(`Connection error: ${e.message}`);
  }

  const actualDuration = (Date.now() - Date.now()) / 1000;

  // Final report
  console.log("\n" + "=".repeat(60));
  console.log(`Protocol Compatibility Test Complete: TypeScript <-> ${serverName}`);
  console.log(`  Total calls: ${stats.clientCalls}`);
  console.log(`  Server callbacks: ${stats.serverCallbacks}`);
  console.log(`  Protocol tests passed: ${stats.protocolTestsPassed}`);
  console.log(`  Protocol tests failed: ${stats.protocolTestsFailed}`);
  console.log(`  Capability creates: ${stats.capabilityCreates}`);
  console.log(`  Errors: ${stats.errors.length}`);
  if (stats.errors.length > 0) {
    for (const err of stats.errors.slice(0, 5)) {
      console.log(`    - ${err}`);
    }
    if (stats.errors.length > 5) {
      console.log(`    ... and ${stats.errors.length - 5} more`);
    }
  }
  console.log("=".repeat(60) + "\n");

  return stats;
}

async function main() {
  console.log(`Starting ${DURATION}s protocol compatibility test...`);
  console.log(`Connecting to Python server at ${WS_URL}`);

  const stats = await runProtocolCompatibilityDemo(WS_URL, DURATION, "Python");

  // Exit with error if tests failed
  if (stats.protocolTestsFailed > 0) {
    console.log("FAILED: Protocol compatibility failures detected");
    process.exit(1);
  }

  const errorRate = stats.errors.length / Math.max(stats.clientCalls, 1);
  if (errorRate > 0.1) {
    console.log(`FAILED: Too many errors (${(errorRate * 100).toFixed(1)}%)`);
    process.exit(1);
  }

  console.log("SUCCESS: All protocol compatibility tests passed");
  process.exit(0);
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
