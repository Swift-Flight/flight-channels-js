// Type declarations for @swift-flight/channels/presence — the presence helper
// ( ).

import type { Json } from "./index.js";

export const PRESENCE_EVENTS: Readonly<{
  state: "flight:presence_state";
  diff: "flight:presence_diff";
}>;

/** One meta as it travels on the wire: the ref plus the flattened payload. */
export interface PresenceMeta {
  ref: string;
  [key: string]: Json;
}

export interface PresenceEntry {
  key: string;
  metas: PresenceMeta[];
}

export interface PresenceChange {
  /** The full list after applying the message, sorted by key. */
  list: PresenceEntry[];
  /** Net joins — updated metas (same ref, new payload) appear here only. */
  joins: Record<string, PresenceMeta[]>;
  /** Net leaves — a genuinely departed meta, never an update flap. */
  leaves: Record<string, PresenceMeta[]>;
}

/** The `on` shape FlightPresence needs — satisfied by FlightChannel. */
export interface PresenceChannelLike {
  on(event: string, listener: (payload: Json, event: string) => void): () => void;
}

export class FlightPresence {
  constructor(channel: PresenceChannelLike);
  list(): PresenceEntry[];
  onChange(listener: (change: PresenceChange) => void): () => void;
  destroy(): void;
}
