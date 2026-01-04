/**
 * TypeScript client test script for interop testing.
 * 
 * This script connects to a Python server and runs the test matrix.
 * 
 * Usage: npx tsx ts_client_test.ts <port>
 */

import { newWebSocketRpcSession, RpcStub, RpcTarget } from 'capnweb';

const PORT = parseInt(process.argv[2] || '9200', 10);
const WS_URL = `ws://localhost:${PORT}/rpc`;

interface TestTarget {
  square(i: number): number;
  add(a: number, b: number): number;
  greet(name: string): string;
  echo(value: any): any;
  generateFibonacci(length: number): number[];
  getList(): number[];
  returnNull(): null;
  returnNumber(i: number): number;
  throwError(): never;
  registerCallback(cb: RpcStub<any>): string;
  triggerCallback(): string;
}

class ClientCallback extends RpcTarget {
  notifications: string[] = [];
  
  notify(msg: string): string {
    this.notifications.push(msg);
    return `Got: ${msg}`;
  }
}

async function runTests(): Promise<void> {
  const results: { name: string; passed: boolean; error?: string }[] = [];
  
  async function test(name: string, fn: () => Promise<void>): Promise<void> {
    try {
      await fn();
      results.push({ name, passed: true });
      console.log(`✓ ${name}`);
    } catch (e: any) {
      results.push({ name, passed: false, error: e.message });
      console.log(`✗ ${name}: ${e.message}`);
    }
  }
  
  // Test 1: Simple square
  await test('square', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.square(5);
    if (result !== 25) throw new Error(`Expected 25, got ${result}`);
  });
  
  // Test 2: Add
  await test('add', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.add(3, 7);
    if (result !== 10) throw new Error(`Expected 10, got ${result}`);
  });
  
  // Test 3: Greet
  await test('greet', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.greet("World");
    if (result !== "Hello, World!") throw new Error(`Expected "Hello, World!", got ${result}`);
  });
  
  // Test 4: Echo string
  await test('echo string', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.echo("test message");
    if (result !== "test message") throw new Error(`Expected "test message", got ${result}`);
  });
  
  // Test 5: Echo number
  await test('echo number', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.echo(42);
    if (result !== 42) throw new Error(`Expected 42, got ${result}`);
  });
  
  // Test 6: Echo array
  await test('echo array', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.echo([1, 2, 3]);
    if (JSON.stringify(result) !== JSON.stringify([1, 2, 3])) {
      throw new Error(`Expected [1,2,3], got ${JSON.stringify(result)}`);
    }
  });
  
  // Test 7: Echo nested object
  await test('echo nested object', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const obj = { foo: { bar: 123 }, baz: [1, 2, 3] };
    const result = await stub.echo(obj);
    if (JSON.stringify(result) !== JSON.stringify(obj)) {
      throw new Error(`Expected ${JSON.stringify(obj)}, got ${JSON.stringify(result)}`);
    }
  });
  
  // Test 8: Generate Fibonacci
  await test('generateFibonacci', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.generateFibonacci(10);
    const expected = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34];
    if (JSON.stringify(result) !== JSON.stringify(expected)) {
      throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(result)}`);
    }
  });
  
  // Test 9: Get list
  await test('getList', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.getList();
    const expected = [1, 2, 3, 4, 5];
    if (JSON.stringify(result) !== JSON.stringify(expected)) {
      throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(result)}`);
    }
  });
  
  // Test 10: Return null
  await test('returnNull', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.returnNull();
    if (result !== null) throw new Error(`Expected null, got ${result}`);
  });
  
  // Test 11: Return number
  await test('returnNumber', async () => {
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL);
    const result = await stub.returnNumber(123);
    if (result !== 123) throw new Error(`Expected 123, got ${result}`);
  });
  
  // Test 12: Callback roundtrip
  await test('callback roundtrip', async () => {
    const callback = new ClientCallback();
    const stub = newWebSocketRpcSession<TestTarget>(WS_URL, callback);
    
    // Register callback
    const regResult = await stub.registerCallback(new RpcStub(callback));
    if (regResult !== "registered") {
      throw new Error(`Expected "registered", got ${regResult}`);
    }
    
    // Trigger callback
    const triggerResult = await stub.triggerCallback();
    if (triggerResult !== "Got: ping") {
      throw new Error(`Expected "Got: ping", got ${triggerResult}`);
    }
    
    if (callback.notifications.length !== 1 || callback.notifications[0] !== "ping") {
      throw new Error(`Expected ["ping"], got ${JSON.stringify(callback.notifications)}`);
    }
  });
  
  // Summary
  console.log('\n--- Summary ---');
  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;
  console.log(`Passed: ${passed}/${results.length}`);
  console.log(`Failed: ${failed}/${results.length}`);
  
  if (failed === 0) {
    console.log('\nALL TESTS PASSED');
    process.exit(0);
  } else {
    console.log('\nSOME TESTS FAILED');
    for (const r of results.filter(r => !r.passed)) {
      console.log(`  - ${r.name}: ${r.error}`);
    }
    process.exit(1);
  }
}

runTests().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
