// Thread titles for the rows on screen. Construct during component
// initialisation: the store owns an `$effect`.

import { untrack } from 'svelte';
import { SvelteMap } from 'svelte/reactivity';
import { fetchThreadTitles, type ThreadRef } from './api';
import { RETRY_MS, TITLE_TTL_MS, TitleReadSchedule, threadKey } from './thread-titles';

/** Titles of the threads that the page shows, read in batches as rows appear. */
export class ThreadTitleStore {
	#titles = new SvelteMap<string, string | null>();
	#schedule = new TitleReadSchedule();

	constructor(refs: () => readonly ThreadRef[]) {
		$effect(() => {
			const wanted = refs();
			untrack(() => void this.#load(wanted));
		});
	}

	/** Title of one thread, or null while unknown or when no store names it. */
	title = (ref: ThreadRef): string | null => this.#titles.get(threadKey(ref)) ?? null;

	async #load(refs: readonly ThreadRef[]): Promise<void> {
		const now = Date.now();
		const due = this.#schedule.take(refs, now);
		if (due.length === 0) return;
		try {
			for (const entry of await fetchThreadTitles(due)) {
				this.#titles.set(threadKey(entry), entry.title);
			}
			this.#schedule.settle(due, now + TITLE_TTL_MS);
		} catch {
			this.#schedule.settle(due, now + RETRY_MS);
		}
	}
}
