export interface TerminalWriteTarget {
  write(data: string, callback?: () => void): void;
}

export const TERMINAL_RESET_SEQUENCE = "\u001bc";
export const TERMINAL_WRITE_CHUNK_SIZE = 16 * 1024;
export const TERMINAL_FULL_RESYNC_DELTA_THRESHOLD = 1024 * 1024;

export function terminalBufferNeedsFullResync(input: {
  readonly currentBuffer: string;
  readonly previousBuffer: string;
  readonly force: boolean;
  readonly largeDeltaThreshold?: number;
}): boolean {
  if (input.force) return true;
  if (input.currentBuffer.length < input.previousBuffer.length) return true;
  if (!input.currentBuffer.startsWith(input.previousBuffer)) return true;
  return (
    input.currentBuffer.length - input.previousBuffer.length >
    (input.largeDeltaThreshold ?? TERMINAL_FULL_RESYNC_DELTA_THRESHOLD)
  );
}

export function terminalBufferDelta(input: {
  readonly currentBuffer: string;
  readonly previousBuffer: string;
}): string {
  if (input.currentBuffer.length < input.previousBuffer.length) return input.currentBuffer;
  if (!input.currentBuffer.startsWith(input.previousBuffer)) return input.currentBuffer;
  return input.currentBuffer.slice(input.previousBuffer.length);
}

export class TerminalWriteQueue {
  private readonly target: TerminalWriteTarget;
  private readonly chunkSize: number;
  private readonly onWriteFailure: (error: unknown) => void;
  private readonly scheduleDrain: (drain: () => void) => void;
  private chunks: string[] = [];
  private draining = false;
  private disposed = false;

  constructor(
    target: TerminalWriteTarget,
    options: {
      readonly chunkSize?: number;
      readonly onWriteFailure?: (error: unknown) => void;
      readonly scheduleDrain?: (drain: () => void) => void;
    } = {},
  ) {
    this.target = target;
    this.chunkSize = Math.max(1, Math.floor(options.chunkSize ?? TERMINAL_WRITE_CHUNK_SIZE));
    this.onWriteFailure = options.onWriteFailure ?? (() => undefined);
    this.scheduleDrain =
      options.scheduleDrain ??
      ((drain) => {
        if (typeof queueMicrotask === "function") {
          queueMicrotask(drain);
          return;
        }
        window.setTimeout(drain, 0);
      });
  }

  enqueue(data: string): void {
    if (this.disposed || data.length === 0) return;
    for (let offset = 0; offset < data.length; offset += this.chunkSize) {
      this.chunks.push(data.slice(offset, offset + this.chunkSize));
    }
    this.drain();
  }

  writeTerminalBuffer(buffer: string): void {
    if (this.disposed) return;
    this.chunks = [];
    this.enqueue(TERMINAL_RESET_SEQUENCE);
    this.enqueue(buffer);
  }

  dispose(): void {
    this.disposed = true;
    this.chunks = [];
  }

  private drain(): void {
    if (this.disposed || this.draining) return;
    const chunk = this.chunks.shift();
    if (chunk === undefined) return;

    this.draining = true;
    try {
      this.target.write(chunk, () => {
        this.scheduleDrain(() => {
          this.draining = false;
          this.drain();
        });
      });
    } catch (error) {
      this.draining = false;
      this.chunks = [];
      this.onWriteFailure(error);
    }
  }
}
