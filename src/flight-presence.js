// Flight Presence — JS/TS client helper.
//
// Applies flight:presence_state / flight:presence_diff messages from a
// FlightChannel to a maintained key → metas map, so application code sees
// a list, not diff plumbing. The rules mirror the Swift `PresenceSync`
// state machine exactly (its test suite asserts the same cases):
//
// - Within one diff, leaves apply before joins: a meta-only update travels
//   as a leave of the old meta and a join of the new one for the same ref,
//   and leaves-first makes that an in-place replacement.
// - Joins upsert by ref — a re-delivered join replaces, never duplicates,
//   so overlap between the initial state and a concurrent diff is harmless.
// - Leaves remove by ref; removing an unknown ref is a no-op.
// - Reported changes are the *net* effect: an updated ref appears in joins
//   only (no leave/join flap), and a leave that removed nothing is silent.

/** The two reserved presence events. */
export const PRESENCE_EVENTS = Object.freeze({
  state: "flight:presence_state",
  diff: "flight:presence_diff",
});

function shallowEqual(a, b) {
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);
  if (aKeys.length !== bKeys.length) return false;
  return aKeys.every((key) => a[key] === b[key]);
}

/** @param {any} entries wire `entries` shape → Map<string, object[]> */
function parseEntries(entries) {
  const result = new Map();
  if (!entries || typeof entries !== "object") return result;
  for (const [key, value] of Object.entries(entries)) {
    const metas = Array.isArray(value?.metas)
      ? value.metas.filter((meta) => meta && typeof meta.ref === "string")
      : [];
    if (metas.length > 0) result.set(key, metas);
  }
  return result;
}

/**
 * Maintains one topic's presence from state + diff messages.
 *
 *     const room = socket.channel("room:42");
 *     const presence = new FlightPresence(room);
 *     presence.onChange(({ list, joins, leaves }) => render(list));
 *     await room.join();   // server sends state, then diffs
 */
export class FlightPresence {
  /**
   * @param {{on: (event: string, listener: (payload: any) => void) => () => void}} channel
   *   a FlightChannel (or anything with its `on` shape).
   */
  constructor(channel) {
    /** @type {Map<string, object[]>} key → metas, each meta {ref, ...payload} */
    this._entries = new Map();
    /** @type {Set<Function>} */
    this._listeners = new Set();
    this._unsubscribes = [
      channel.on(PRESENCE_EVENTS.state, (payload) => this._applyState(payload)),
      channel.on(PRESENCE_EVENTS.diff, (payload) => this._applyDiff(payload)),
    ];
  }

  /** Stop listening; the map stops updating. */
  destroy() {
    for (const unsubscribe of this._unsubscribes) unsubscribe();
    this._unsubscribes = [];
    this._listeners.clear();
  }

  /**
   * The current list, sorted by key.
   * @returns {{key: string, metas: object[]}[]}
   */
  list() {
    return [...this._entries.keys()]
      .sort()
      .map((key) => ({ key, metas: [...this._entries.get(key)] }));
  }

  /**
   * Observe changes. Called with `{list, joins, leaves}` — the full list
   * after the message, plus the net joins/leaves it caused (updated metas
   * appear in joins only). Returns an unsubscribe function.
   * @param {(change: {list: {key: string, metas: object[]}[], joins: Record<string, object[]>, leaves: Record<string, object[]>}) => void} listener
   */
  onChange(listener) {
    this._listeners.add(listener);
    return () => this._listeners.delete(listener);
  }

  // ---- internals -------------------------------------------------------

  _applyState(payload) {
    const incoming = parseEntries(payload);
    const joins = {};
    const leaves = {};

    for (const [key, newMetas] of incoming) {
      const known = new Map((this._entries.get(key) ?? []).map((meta) => [meta.ref, meta]));
      const joined = newMetas.filter((meta) => {
        const existing = known.get(meta.ref);
        return !existing || !shallowEqual(existing, meta);
      });
      if (joined.length > 0) joins[key] = joined;
    }
    for (const [key, oldMetas] of this._entries) {
      const incomingRefs = new Set((incoming.get(key) ?? []).map((meta) => meta.ref));
      const left = oldMetas.filter((meta) => !incomingRefs.has(meta.ref));
      if (left.length > 0) leaves[key] = left;
    }

    this._entries = incoming;
    this._notify(joins, leaves);
  }

  _applyDiff(payload) {
    const diffJoins = parseEntries(payload?.joins);
    const diffLeaves = parseEntries(payload?.leaves);
    const joins = {};
    const leaves = {};

    // Leaves first (update normalization — see the module comment).
    for (const [key, leftMetas] of diffLeaves) {
      const metas = this._entries.get(key);
      if (!metas) continue;
      const leavingRefs = new Set(leftMetas.map((meta) => meta.ref));
      const removed = metas.filter((meta) => leavingRefs.has(meta.ref));
      if (removed.length === 0) continue;
      const remaining = metas.filter((meta) => !leavingRefs.has(meta.ref));
      if (remaining.length === 0) this._entries.delete(key);
      else this._entries.set(key, remaining);
      const rejoined = new Set((diffJoins.get(key) ?? []).map((meta) => meta.ref));
      const net = removed.filter((meta) => !rejoined.has(meta.ref));
      if (net.length > 0) leaves[key] = net;
    }

    for (const [key, joinedMetas] of diffJoins) {
      const metas = this._entries.get(key) ?? [];
      const applied = [];
      for (const meta of joinedMetas) {
        const index = metas.findIndex((existing) => existing.ref === meta.ref);
        if (index >= 0) {
          if (shallowEqual(metas[index], meta)) continue;
          metas[index] = meta;
        } else {
          metas.push(meta);
        }
        applied.push(meta);
      }
      if (metas.length > 0) this._entries.set(key, metas);
      if (applied.length > 0) joins[key] = applied;
    }

    this._notify(joins, leaves);
  }

  _notify(joins, leaves) {
    if (Object.keys(joins).length === 0 && Object.keys(leaves).length === 0) return;
    const change = { list: this.list(), joins, leaves };
    for (const listener of this._listeners) listener(change);
  }
}
