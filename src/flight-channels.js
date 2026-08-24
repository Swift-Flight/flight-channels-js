// Flight Channels — JS/TS reference client.
//
// Deliberately small and dependency-free: WebSocket management, the envelope
// protocol, ref/reply correlation (push() returns a promise resolving
// on the matching flight:reply), the heartbeat, and automatic
// reconnect-with-backoff-and-rejoin. Protocol plumbing, not a framework.
//
// The wire contract is the same one the server and the Swift client are
// versioned with: one envelope both directions —
//   { ref: "7" | null, topic: "room:42", event: "new_msg", payload: {...} }

/** Reserved lifecycle events. */
export const RESERVED = Object.freeze({
  join: "flight:join",
  leave: "flight:leave",
  reply: "flight:reply",
  error: "flight:error",
  heartbeat: "flight:heartbeat",
  close: "flight:close",
});

/** The topic socket-level control events travel on. Can never be joined. */
export const CONTROL_TOPIC = "flight";

/** The server answered with flight:error (join rejected, handler error…). */
export class ChannelError extends Error {
  /** @param {string} reason */
  constructor(reason) {
    super(`channel error: ${reason}`);
    this.name = "ChannelError";
    this.reason = reason;
  }
}

/** No reply before the deadline. */
export class TimeoutError extends Error {
  constructor() {
    super("no reply before the deadline");
    this.name = "TimeoutError";
  }
}

/** The connection dropped while a push awaited its reply. */
export class DisconnectedError extends Error {
  constructor() {
    super("the connection dropped while awaiting a reply");
    this.name = "DisconnectedError";
  }
}

/** Not connected — call connect() first. */
export class NotConnectedError extends Error {
  constructor() {
    super("not connected — call connect() first");
    this.name = "NotConnectedError";
  }
}

/**
 * Doubling backoff from `initialMs`, capped at `maxMs`; null after
 * `maxAttempts` (never, by default).
 *
 * @param {{initialMs?: number, maxMs?: number, maxAttempts?: number|null}} [options]
 * @returns {(attempt: number) => number | null}
 */
export function exponentialBackoff({ initialMs = 100, maxMs = 10_000, maxAttempts = null } = {}) {
  return (attempt) => {
    if (maxAttempts !== null && attempt > maxAttempts) return null;
    return Math.min(initialMs * 2 ** (attempt - 1), maxMs);
  };
}

/**
 * One client WebSocket connection to a Flight server — the "Socket" noun
 *. Holds many channels.
 */
export class FlightSocket {
  /**
   * @param {string} url
   * @param {{
   *   webSocket?: typeof WebSocket,
   *   heartbeatIntervalMs?: number,
   *   pushTimeoutMs?: number,
   *   reconnectDelayMs?: (attempt: number) => number | null,
   * }} [options] `webSocket` injects the constructor (tests, Node without a
   * global). Heartbeat default 25s — well inside the server's 60s timeout.
   */
  constructor(url, options = {}) {
    this.url = url;
    this._WebSocket = options.webSocket ?? globalThis.WebSocket;
    if (!this._WebSocket) {
      throw new Error("no WebSocket implementation: pass options.webSocket");
    }
    this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 25_000;
    this._pushTimeoutMs = options.pushTimeoutMs ?? 10_000;
    this._reconnectDelayMs = options.reconnectDelayMs ?? exponentialBackoff();

    /** @type {"closed"|"connecting"|"connected"|"disconnected"} */
    this.state = "closed";
    this._ws = null;
    this._refCounter = 0;
    /** @type {Map<string, {resolve: Function, reject: Function, timer: any}>} */
    this._pending = new Map();
    /** @type {Map<string, FlightChannel>} */
    this._channels = new Map();
    /** @type {Set<(state: string) => void>} */
    this._stateListeners = new Set();
    this._heartbeatTimer = null;
    this._reconnectTimer = null;
    this._intentionalClose = false;
  }

  /**
   * Dials the server. Resolves once connected; rejects if the first dial
   * fails (later drops reconnect automatically per the backoff policy).
   * @returns {Promise<void>}
   */
  connect() {
    if (this.state === "connected" || this.state === "connecting") {
      return Promise.resolve();
    }
    this._intentionalClose = false;
    return this._dial(/* rejectOnFailure */ true);
  }

  /**
   * Graceful teardown: best-effort flight:close, then the transport
   * close. Terminal — no reconnection until connect() is called again.
   * Channel membership intent survives: a later connect() rejoins.
   */
  disconnect() {
    this._intentionalClose = true;
    this._clearReconnect();
    this._stopHeartbeat();
    if (this._ws && this.state === "connected") {
      this._sendEnvelope({ ref: null, topic: CONTROL_TOPIC, event: RESERVED.close, payload: {} });
      this._ws.close();
    } else if (this._ws) {
      this._ws.close();
    }
    this._ws = null;
    this._failAllPending(new DisconnectedError());
    for (const channel of this._channels.values()) channel._joined = false;
    this._setState("closed");
  }

  /**
   * A handle for one topic. Cheap; does not join. One instance per
   * topic per socket — repeated calls return the same handle.
   * @param {string} topic
   * @returns {FlightChannel}
   */
  channel(topic) {
    let channel = this._channels.get(topic);
    if (!channel) {
      channel = new FlightChannel(topic, this);
      this._channels.set(topic, channel);
    }
    return channel;
  }

  /**
   * Observe connection state ("connecting"/"connected"/"disconnected"/
   * "closed"). Returns an unsubscribe function.
   * @param {(state: string) => void} listener
   */
  onStateChange(listener) {
    this._stateListeners.add(listener);
    return () => this._stateListeners.delete(listener);
  }

  // ---- internals -------------------------------------------------------

  _dial(rejectOnFailure, attempt = 0) {
    this._setState("connecting");
    return new Promise((resolve, reject) => {
      let settled = false;
      const ws = new this._WebSocket(this.url);
      this._ws = ws;

      ws.onopen = () => {
        settled = true;
        this._setState("connected");
        this._startHeartbeat();
        this._rejoinAll().then(resolve, resolve); // connect() resolves on open
      };
      ws.onmessage = (event) => this._receive(event.data);
      ws.onclose = () => {
        if (this._ws !== ws) return; // superseded connection
        if (!settled) {
          settled = true;
          if (rejectOnFailure) {
            this._setState("closed");
            reject(new NotConnectedError());
            return;
          }
          this._scheduleReconnect(attempt); // failed re-dial: keep backing off
          resolve(undefined);
          return;
        }
        this._connectionDropped();
        resolve(undefined);
      };
      ws.onerror = () => {
        // onclose follows onerror in every implementation; handled there.
      };
    });
  }

  _connectionDropped() {
    if (this._intentionalClose) return;
    this._ws = null;
    this._stopHeartbeat();
    this._failAllPending(new DisconnectedError());
    for (const channel of this._channels.values()) channel._joined = false;
    this._setState("disconnected");
    this._scheduleReconnect(0);
  }

  _scheduleReconnect(previousAttempt) {
    const attempt = previousAttempt + 1;
    const delay = this._reconnectDelayMs(attempt);
    if (delay === null || delay === undefined) {
      this._setState("closed");
      return;
    }
    this._setState("disconnected");
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      if (this._intentionalClose) return;
      this._dial(false, attempt);
    }, delay);
  }

  _clearReconnect() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }

  /** Rejoin every desired channel after a (re)connect. */
  async _rejoinAll() {
    for (const channel of this._channels.values()) {
      if (!channel._desired || channel._joined) continue;
      try {
        const state = await this._request(channel.topic, RESERVED.join, {}, this._pushTimeoutMs);
        channel._joined = true;
        // Parity with the Swift client: the fresh state arrives on the
        // message listeners as a flight:join message.
        channel._deliver(RESERVED.join, state);
      } catch (error) {
        if (error instanceof ChannelError) {
          channel._desired = false; // the gate closed; stop retrying
          channel._deliver(RESERVED.error, { reason: error.reason });
        }
        // Disconnected/timeout: the drop machinery is already re-driving.
      }
    }
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    const beat = () => {
      this._heartbeatTimer = setTimeout(async () => {
        if (this.state !== "connected") return;
        try {
          // Deadline = interval: an unanswered heartbeat means the
          // connection is dead in exactly the way heartbeats detect.
          await this._request(CONTROL_TOPIC, RESERVED.heartbeat, {}, this._heartbeatIntervalMs);
          beat();
        } catch {
          if (this.state === "connected" && this._ws) this._ws.close();
        }
      }, this._heartbeatIntervalMs);
    };
    beat();
  }

  _stopHeartbeat() {
    if (this._heartbeatTimer) {
      clearTimeout(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
  }

  /** @returns {Promise<any>} the flight:reply payload */
  _request(topic, event, payload, timeoutMs) {
    if (this.state !== "connected" || !this._ws) {
      return Promise.reject(new NotConnectedError());
    }
    const ref = String(++this._refCounter);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(ref);
        reject(new TimeoutError());
      }, timeoutMs);
      this._pending.set(ref, { resolve, reject, timer });
      this._sendEnvelope({ ref, topic, event, payload });
    });
  }

  _sendEnvelope(envelope) {
    // All four keys always on the wire.
    this._ws.send(JSON.stringify({
      ref: envelope.ref ?? null,
      topic: envelope.topic,
      event: envelope.event,
      payload: envelope.payload ?? {},
    }));
  }

  _receive(text) {
    let envelope;
    try {
      envelope = JSON.parse(text);
    } catch {
      // Flight owns both ends: an undecodable server frame is a
      // version-skew bug, not a compatibility case. Drop it.
      return;
    }
    const { ref, topic, event, payload } = envelope;
    switch (event) {
      case RESERVED.reply: {
        const pending = ref != null && this._pending.get(ref);
        if (pending) {
          this._pending.delete(ref);
          clearTimeout(pending.timer);
          pending.resolve(payload);
        }
        return;
      }
      case RESERVED.error: {
        const pending = ref != null && this._pending.get(ref);
        if (pending) {
          this._pending.delete(ref);
          clearTimeout(pending.timer);
          pending.reject(new ChannelError(payload?.reason ?? "unknown"));
          return;
        }
        this._channels.get(topic)?._deliver(event, payload);
        return;
      }
      case RESERVED.close:
        // Server-initiated graceful teardown: terminal, no reconnect.
        this.disconnect();
        return;
      case RESERVED.join:
      case RESERVED.leave:
      case RESERVED.heartbeat:
        return; // server never initiates these; tolerate and ignore
      default:
        this._channels.get(topic)?._deliver(event, payload);
    }
  }

  _failAllPending(error) {
    for (const { reject, timer } of this._pending.values()) {
      clearTimeout(timer);
      reject(error);
    }
    this._pending.clear();
  }

  _setState(state) {
    if (this.state === state) return;
    this.state = state;
    for (const listener of this._stateListeners) listener(state);
  }
}

/**
 * A client's membership in one topic on one socket — the "Channel" noun
 *, client side.
 */
export class FlightChannel {
  /**
   * @param {string} topic
   * @param {FlightSocket} socket
   */
  constructor(topic, socket) {
    this.topic = topic;
    this._socket = socket;
    this._desired = false;
    this._joined = false;
    /** @type {Map<string, Set<(payload: any, event: string) => void>>} */
    this._listeners = new Map();
  }

  /** Currently joined on the server (false while disconnected). */
  get joined() {
    return this._joined;
  }

  /**
   * Joins the topic (the join is the gate). Resolves with the channel's
   * initial state; rejects with ChannelError when refused. Membership
   * survives reconnection: after a drop the client rejoins automatically
   * and the fresh state is delivered as a "flight:join" message.
   * @param {number} [timeoutMs]
   * @returns {Promise<any>}
   */
  async join(timeoutMs) {
    this._desired = true;
    try {
      const state = await this._socket._request(
        this.topic, RESERVED.join, {}, timeoutMs ?? this._socket._pushTimeoutMs,
      );
      this._joined = true;
      return state;
    } catch (error) {
      if (error instanceof ChannelError) this._desired = false;
      throw error;
    }
  }

  /**
   * Leaves the topic — the server runs the channel's leave and ends its
   * fan-out. Clears the rejoin intent.
   * @param {number} [timeoutMs]
   */
  async leave(timeoutMs) {
    this._desired = false;
    this._joined = false;
    await this._socket._request(
      this.topic, RESERVED.leave, {}, timeoutMs ?? this._socket._pushTimeoutMs,
    );
  }

  /**
   * Sends an application event and awaits its flight:reply. Rejects
   * with TimeoutError if the handler chose not to reply, ChannelError if it
   * answered flight:error, DisconnectedError if the connection dropped.
   * @param {string} event
   * @param {any} [payload]
   * @param {number} [timeoutMs]
   * @returns {Promise<any>} the reply payload
   */
  push(event, payload = {}, timeoutMs) {
    return this._socket._request(
      this.topic, event, payload, timeoutMs ?? this._socket._pushTimeoutMs,
    );
  }

  /**
   * Fire-and-forget: ref null, so no reply ever comes.
   * @param {string} event
   * @param {any} [payload]
   */
  send(event, payload = {}) {
    if (this._socket.state !== "connected" || !this._socket._ws) {
      throw new NotConnectedError();
    }
    this._socket._sendEnvelope({ ref: null, topic: this.topic, event, payload });
  }

  /**
   * Listen for pushes on this channel. `event` may be "*" for everything
   * (including the synthetic "flight:join" rejoin notification and
   * uncorrelated "flight:error"s). Returns an unsubscribe function.
   * @param {string} event
   * @param {(payload: any, event: string) => void} listener
   */
  on(event, listener) {
    let set = this._listeners.get(event);
    if (!set) {
      set = new Set();
      this._listeners.set(event, set);
    }
    set.add(listener);
    return () => set.delete(listener);
  }

  _deliver(event, payload) {
    for (const listener of this._listeners.get(event) ?? []) listener(payload, event);
    for (const listener of this._listeners.get("*") ?? []) listener(payload, event);
  }
}
