export type OutboundDeliverFn = () => Promise<void>;

export type SignalOutboundBuffer = {
  /** Called when a typing event arrives for a conversation. */
  notifyTyping: (conversationId: string, action: "STARTED" | "STOPPED" | string) => void;
  /**
   * Enqueue a delivery. If typing is active for `target`, queues the fn and
   * returns immediately (fire-and-forget). Otherwise calls fn immediately and
   * awaits it. Fire-and-forget mode is intentional: the dispatcher upstream
   * considers delivery done when this resolves; the actual send follows when queue flushes.
   */
  enqueue: (target: string, deliverFn: OutboundDeliverFn, text?: string) => Promise<void>;
  /** Clear any queued replies for `target` (e.g. when a new inbound message arrives). */
  clearQueue: (target: string) => void;
};

/**
 * Creates a per-conversation outbound buffer that holds replies while the
 * remote peer is actively typing.
 *
 * @param holdMs   - How long after each typing-start event to hold the queue (default 10000).
 *                   Should match or slightly exceed the inbound debounce window.
 * @param maxHoldMs - Hard cap from first enqueue, so a stuck typing indicator
 *                    cannot block replies forever (default 30000).
 */
export function createSignalOutboundBuffer(opts?: {
  holdMs?: number;
  maxHoldMs?: number;
  log?: (msg: string) => void;
}): SignalOutboundBuffer {
  const holdMs = opts?.holdMs ?? 10_000;
  const maxHoldMs = opts?.maxHoldMs ?? 30_000;
  const log = opts?.log;

  // conversationId → timestamp of last STARTED event
  const lastTypingStarted = new Map<string, number>();
  // target → queued delivery fns
  const queues = new Map<string, { fn: OutboundDeliverFn; text: string }[]>();
  // target → timestamp of first enqueue (for maxHoldMs cap)
  const queuedSince = new Map<string, number>();
  // target → active flush timer
  const flushTimers = new Map<string, ReturnType<typeof setTimeout>>();

  function isTypingActive(target: string): boolean {
    const t = lastTypingStarted.get(target);
    return t !== undefined && Date.now() - t < holdMs;
  }

  async function flush(target: string): Promise<void> {
    flushTimers.delete(target);
    const items = queues.get(target);
    if (!items?.length) {
      queues.delete(target);
      queuedSince.delete(target);
      return;
    }
    queues.delete(target);
    queuedSince.delete(target);
    log?.(`[outbound-buffer] flushing ${items.length} queued reply(ies) to ${target}`);
    for (const item of items) {
      try {
        await item.fn();
      } catch (err) {
        log?.(`[outbound-buffer] flush error for ${target}: ${String(err)}`);
      }
    }
  }

  function scheduleFlush(target: string): void {
    const existing = flushTimers.get(target);
    if (existing) clearTimeout(existing);

    const since = queuedSince.get(target) ?? Date.now();
    const capDeadline = since + maxHoldMs;
    const typingDeadline = isTypingActive(target) ? Date.now() + holdMs : Date.now();
    const deadline = Math.min(typingDeadline, capDeadline);
    const delay = Math.max(0, deadline - Date.now());

    const timer = setTimeout(() => void flush(target), delay);
    timer.unref?.();
    flushTimers.set(target, timer);
  }

  function notifyTyping(conversationId: string, action: "STARTED" | "STOPPED" | string): void {
    if (action === "STARTED") {
      lastTypingStarted.set(conversationId, Date.now());
      if (queues.has(conversationId)) {
        scheduleFlush(conversationId);
      }
    } else if (action === "STOPPED") {
      lastTypingStarted.delete(conversationId);
      if (queues.has(conversationId)) {
        scheduleFlush(conversationId);
      }
    }
  }

  async function enqueue(
    target: string,
    deliverFn: OutboundDeliverFn,
    text?: string,
  ): Promise<void> {
    if (!isTypingActive(target)) {
      await deliverFn();
      return;
    }
    log?.(`[outbound-buffer] holding reply to ${target} (peer is typing)`);
    if (!queues.has(target)) {
      queues.set(target, []);
      queuedSince.set(target, Date.now());
    }
    queues.get(target)!.push({ fn: deliverFn, text: text ?? "" });
    scheduleFlush(target);
    // Return immediately — delivery is async. The upstream dispatcher treats
    // this as "dispatched" and continues; actual send follows when queue flushes.
  }

  function clearQueue(target: string): void {
    const timer = flushTimers.get(target);
    if (timer) clearTimeout(timer);
    flushTimers.delete(target);
    if (queues.has(target)) {
      log?.(`[outbound-buffer] clearing queued reply(ies) to ${target} (new inbound arrived)`);
    }
    queues.delete(target);
    queuedSince.delete(target);
  }

  return { notifyTyping, enqueue, clearQueue };
}
