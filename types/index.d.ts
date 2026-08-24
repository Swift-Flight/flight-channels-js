// Type declarations for @swift-flight/channels — the JS reference client
// ( ).

/** Any JSON value — the envelope's opaque payload. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export type SocketState = "closed" | "connecting" | "connected" | "disconnected";

export const RESERVED: Readonly<{
  join: "flight:join";
  leave: "flight:leave";
  reply: "flight:reply";
  error: "flight:error";
  heartbeat: "flight:heartbeat";
  close: "flight:close";
}>;

export const CONTROL_TOPIC: "flight";

export class ChannelError extends Error {
  readonly name: "ChannelError";
  readonly reason: string;
  constructor(reason: string);
}

export class TimeoutError extends Error {
  readonly name: "TimeoutError";
}

export class DisconnectedError extends Error {
  readonly name: "DisconnectedError";
}

export class NotConnectedError extends Error {
  readonly name: "NotConnectedError";
}

export function exponentialBackoff(options?: {
  initialMs?: number;
  maxMs?: number;
  maxAttempts?: number | null;
}): (attempt: number) => number | null;

export interface FlightSocketOptions {
  /** WebSocket constructor injection (tests, Node without a global). */
  webSocket?: typeof WebSocket;
  /** Default 25 000 — well inside the server's 60 s timeout. */
  heartbeatIntervalMs?: number;
  /** Default deadline for join/push replies. Default 10 000. */
  pushTimeoutMs?: number;
  /** Delay before reconnect attempt n (1-based); null gives up. */
  reconnectDelayMs?: (attempt: number) => number | null;
}

export class FlightSocket {
  constructor(url: string, options?: FlightSocketOptions);
  readonly url: string;
  readonly state: SocketState;
  connect(): Promise<void>;
  disconnect(): void;
  channel(topic: string): FlightChannel;
  onStateChange(listener: (state: SocketState) => void): () => void;
}

export class FlightChannel {
  readonly topic: string;
  readonly joined: boolean;
  join(timeoutMs?: number): Promise<Json>;
  leave(timeoutMs?: number): Promise<void>;
  push(event: string, payload?: Json, timeoutMs?: number): Promise<Json>;
  send(event: string, payload?: Json): void;
  on(event: string, listener: (payload: Json, event: string) => void): () => void;
}
