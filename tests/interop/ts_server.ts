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

// B1 streams: source with cancel tracking (design doc 03-streams.md §4).
class StreamSource extends RpcTarget {
  cancelReason: string | null = null;

  makeTrackedStream(count: number): ReadableStream {
    let i = 0;
    const self = this;
    return new ReadableStream({
      pull(controller) {
        if (i < count) {
          controller.enqueue(i++);
        } else {
          controller.close();
        }
      },
      cancel(reason) {
        self.cancelReason = String(reason);
      },
    });
  }

  makeInfiniteStream(): ReadableStream {
    let i = 0;
    const self = this;
    return new ReadableStream({
      pull(controller) {
        controller.enqueue(i++);
      },
      cancel(reason) {
        self.cancelReason = String(reason);
      },
    });
  }

  getCancelReason(): string | null {
    return this.cancelReason;
  }
}

// B1 streams: WritableStream factory with observable state.
class StreamCollector extends RpcTarget {
  chunks: any[] = [];
  closed = false;
  errorMessage: string | null = null;

  getWritable(): WritableStream {
    const self = this;
    return new WritableStream({
      write(chunk) {
        self.chunks.push(chunk);
      },
      close() {
        self.closed = true;
      },
      abort(reason) {
        self.errorMessage = String(reason);
      },
    });
  }

  getFailingWritable(failAfter: number): WritableStream {
    const self = this;
    let n = 0;
    return new WritableStream({
      write(chunk) {
        if (++n > failAfter) {
          throw new Error("collector sink failed");
        }
        self.chunks.push(chunk);
      },
      close() {
        self.closed = true;
      },
      abort(reason) {
        self.errorMessage = String(reason);
      },
    });
  }

  getState(): { chunks: any[]; closed: boolean; error: string | null } {
    return { chunks: this.chunks, closed: this.closed, error: this.errorMessage };
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

  // Parity-adversarial helpers (parity plan stream S0)
  throwTypeError(): never {
    throw new TypeError("adversarial type error");
  }

  makeHeaders(): Headers {
    // Serialized by capnweb >= 0.9 as ["headers", [[k, v], ...]] — a tag the
    // Python parser does not know yet (matrix 02 rows 3/12).
    return new Headers([
      ["X-Multi", "a"],
      ["X-multi", "b"],
      ["Content-Type", "text/plain"],
    ]);
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
  
  // ---- B1 streams (design doc 03-streams.md §4; additive) ----

  makeStream(chunks: any[]): ReadableStream {
    let i = 0;
    return new ReadableStream({
      pull(controller) {
        if (i < chunks.length) {
          controller.enqueue(chunks[i++]);
        } else {
          controller.close();
        }
      },
    });
  }

  makeByteStream(totalBytes: number, chunkSize: number): ReadableStream {
    let sent = 0;
    return new ReadableStream({
      pull(controller) {
        if (sent < totalBytes) {
          const n = Math.min(chunkSize, totalBytes - sent);
          const buf = new Uint8Array(n);
          for (let j = 0; j < n; j++) buf[j] = (sent + j) & 0xff;
          sent += n;
          controller.enqueue(buf);
        } else {
          controller.close();
        }
      },
    });
  }

  makeLargeChunkStream(size: number): ReadableStream {
    // Single chunk larger than INITIAL_WINDOW (256 KiB).
    const buf = new Uint8Array(size);
    for (let j = 0; j < size; j++) buf[j] = j & 0xff;
    let done = false;
    return new ReadableStream({
      pull(controller) {
        if (!done) {
          done = true;
          controller.enqueue(buf);
        } else {
          controller.close();
        }
      },
    });
  }

  makeErrorStream(okChunks: number): ReadableStream {
    let i = 0;
    return new ReadableStream({
      pull(controller) {
        if (i < okChunks) {
          controller.enqueue(i++);
        } else {
          controller.error(new Error("ts source exploded"));
        }
      },
    });
  }

  makeStreamSource(): StreamSource {
    return new StreamSource();
  }

  async collectStream(stream: ReadableStream): Promise<any[]> {
    const out: any[] = [];
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.push(value);
    }
    return out;
  }

  async sumByteStream(stream: ReadableStream): Promise<number> {
    let total = 0;
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += (value as Uint8Array).byteLength;
    }
    return total;
  }

  async collectStreamSlow(stream: ReadableStream, delayMs: number): Promise<number> {
    let count = 0;
    const reader = stream.getReader();
    for (;;) {
      const { done } = await reader.read();
      if (done) break;
      count++;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    return count;
  }

  async readStreamPartial(stream: ReadableStream, n: number): Promise<any[]> {
    const out: any[] = [];
    const reader = stream.getReader();
    for (let i = 0; i < n; i++) {
      const { done, value } = await reader.read();
      if (done) break;
      out.push(value);
    }
    await reader.cancel(new Error("reader had enough"));
    return out;
  }

  makeCollector(): StreamCollector {
    return new StreamCollector();
  }

  async writeToWritable(w: WritableStream, chunks: any[]): Promise<string> {
    const writer = w.getWriter();
    for (const chunk of chunks) {
      await writer.write(chunk);
    }
    await writer.close();
    return "ok";
  }

  makeBlob(text: string, type: string): Blob {
    return new Blob([text], { type });
  }

  async readBlob(blob: Blob): Promise<{ type: string; text: string; size: number }> {
    return { type: blob.type, text: await blob.text(), size: blob.size };
  }

  // ---- B2 map/remap: TS-client-direction drivers (additive) ----
  // Each takes a stub to a PY-hosted source target and runs a TS-side
  // .map() over a promise pointing back at the Python peer, so the REMAP
  // expression is emitted by the canonical TS recorder and evaluated by the
  // Python receiver.

  async mapArrayRemote(src: RpcStub<any>): Promise<any> {
    const p = (src as any).getNumbers();
    return await p.map((x: any) => (src as any).double(x));
  }

  async mapNullRemote(src: RpcStub<any>): Promise<any> {
    const p = (src as any).returnNull();
    return await p.map((x: any) => (src as any).double(x));
  }

  async mapSingleRemote(src: RpcStub<any>, n: number): Promise<any> {
    const p = (src as any).returnNumber(n);
    return await p.map((x: any) => (src as any).double(x));
  }

  async mapNestedRemote(src: RpcStub<any>): Promise<any> {
    const p = (src as any).getNumbers();
    return await p.map((x: any) =>
      (src as any).makeList(x).map((y: any) => (src as any).double(y)));
  }

  async mapCounterRemote(src: RpcStub<any>): Promise<any> {
    // Captured-stub scenario: the counter lives on the Python side; each
    // element increments it (order-dependent cumulative results).
    using counter = await (src as any).makeCounter(10);
    const p = (src as any).getNumbers();
    return await p.map((x: any) => counter.increment(x));
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
