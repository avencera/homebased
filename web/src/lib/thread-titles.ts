// When each thread title is due for a read. Plain state: the reactive store
// must not depend on its own bookkeeping.

import { MAX_THREAD_TITLES, type ThreadRef } from './api';

/** How long a title shows before it is read again, so a renamed thread catches up. */
export const TITLE_TTL_MS = 30_000;

/** Wait after a failed read. Titles are labels, so a failure only delays them. */
export const RETRY_MS = 10_000;

/** Map key of one thread on one machine. */
export function threadKey(ref: ThreadRef): string {
	return `${ref.machine ?? ''}/${ref.thread}`;
}

/** Read times and in-flight reads of thread titles. */
export class TitleReadSchedule {
	#nextRead = new Map<string, number>();
	#loading = new Set<string>();

	/** Threads due for a read, at most one batch, now marked in flight. */
	take(refs: readonly ThreadRef[], now: number): ThreadRef[] {
		const due = new Map<string, ThreadRef>();
		for (const ref of refs) {
			if (due.size >= MAX_THREAD_TITLES) break;
			const key = threadKey(ref);
			if (this.#loading.has(key) || (this.#nextRead.get(key) ?? 0) > now) continue;
			due.set(key, ref);
		}
		for (const key of due.keys()) this.#loading.add(key);
		return [...due.values()];
	}

	/** End the read of `refs`; read them again at `nextRead`. */
	settle(refs: readonly ThreadRef[], nextRead: number): void {
		for (const ref of refs) {
			const key = threadKey(ref);
			this.#loading.delete(key);
			this.#nextRead.set(key, nextRead);
		}
	}
}
