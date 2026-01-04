/**
 * TypeScript test server for interop testing.
 * 
 * This server exposes a TestTarget that matches the Python implementation
 * for cross-language protocol compliance testing.
 * 
 * Usage: npx tsx tests/interop/ts_server.ts <port>
 */

import { WebSocketServer } from 'ws';
import http from 'node:http';
import { newWebSocketRpcSession, nodeHttpBatchRpcResponse, RpcTarget, RpcStub } from 'capnweb';

const PORT = parseInt(process.argv[2] || '9100', 10);

class Counter extends RpcTarget {
  private i: number;
  
  constructor(initial: number = 0) {
    super();
    this.i = initial;
  }
  
  increment(amount: number = 1): number {
    this.i += amount;
    return this.i;
  }
  
  get value(): number {
    return this.i;
  }
}

class TestTarget extends RpcTarget {
  private callback?: RpcStub<any>;
  
  square(i: number): number {
    return i * i;
  }
  
  callSquare(self: RpcStub<TestTarget>, i: number): { result: any } {
    return { result: self.square(i) };
  }
  
  async callFunction(func: RpcStub<(i: number) => number>, i: number): Promise<{ result: number }> {
    return { result: await func(i) };
  }
  
  throwError(): never {
    throw new RangeError("test error");
  }
  
  makeCounter(i: number): Counter {
    return new Counter(i);
  }
  
  incrementCounter(c: RpcStub<Counter>, i: number = 1): any {
    return c.increment(i);
  }
  
  generateFibonacci(length: number): number[] {
    if (length <= 0) return [];
    if (length === 1) return [0];
    const result = [0, 1];
    while (result.length < length) {
      result.push(result[result.length - 1] + result[result.length - 2]);
    }
    return result;
  }
  
  returnNull(): null {
    return null;
  }
  
  returnUndefined(): undefined {
    return undefined;
  }
  
  returnNumber(i: number): number {
    return i;
  }
  
  echo(value: any): any {
    return value;
  }
  
  // Special forms support
  echoBytes(data: Uint8Array): Uint8Array {
    return data;
  }
  
  echoDate(date: Date): Date {
    return date;
  }
  
  echoBigInt(value: bigint): bigint {
    return value;
  }
  
  returnInfinity(): number {
    return Infinity;
  }
  
  returnNegativeInfinity(): number {
    return -Infinity;
  }
  
  returnNaN(): number {
    return NaN;
  }
  
  makeBytes(base64: string): Uint8Array {
    return Buffer.from(base64, 'base64');
  }
  
  makeDate(timestamp: number): Date {
    return new Date(timestamp);
  }
  
  makeBigInt(value: string): bigint {
    return BigInt(value);
  }
  
  getTimestamp(date: Date): number {
    return date.getTime();
  }
  
  getBytesLength(data: Uint8Array): number {
    return data.length;
  }
  
  getBigIntString(value: bigint): string {
    return value.toString();
  }
  
  add(a: number, b: number): number {
    return a + b;
  }
  
  async slowMethod(delayMs: number): Promise<string> {
    await new Promise(resolve => setTimeout(resolve, delayMs));
    return `slow result after ${delayMs}ms`;
  }
  
  greet(name: string): string {
    return `Hello, ${name}!`;
  }
  
  getList(): number[] {
    return [1, 2, 3, 4, 5];
  }
  
  registerCallback(cb: RpcStub<any>): string {
    // Must dup() to keep the callback alive after the method returns
    // Otherwise the RPC system will release it when registerCallback returns
    this.callback = cb.dup();
    return "registered";
  }
  
  async triggerCallback(): Promise<string> {
    if (!this.callback) {
      return "no callback";
    }
    return await this.callback.notify("ping");
  }
}

// Create HTTP server
const httpServer = http.createServer((request, response) => {
  if (request.headers.upgrade?.toLowerCase() === 'websocket') {
    return;
  }
  
  nodeHttpBatchRpcResponse(request, response, new TestTarget(), {
    headers: { "Access-Control-Allow-Origin": "*" }
  });
});

// Create WebSocket server
const wsServer = new WebSocketServer({ server: httpServer });
wsServer.on('connection', (ws) => {
  newWebSocketRpcSession(ws as any, new TestTarget());
});

httpServer.listen(PORT, '127.0.0.1', () => {
  console.log(`TypeScript interop server listening on 127.0.0.1:${PORT}`);
  console.log(`WebSocket: ws://127.0.0.1:${PORT}/`);
  console.log(`HTTP Batch: http://127.0.0.1:${PORT}/`);
});

// Handle shutdown
process.on('SIGINT', () => {
  console.log('Shutting down...');
  wsServer.close();
  httpServer.close();
  process.exit(0);
});
