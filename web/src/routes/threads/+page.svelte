<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import { useInterval } from 'runed';
	import {
		compactClaudeThread,
		fetchLargeClaudeThreads,
		type CompactResult,
		type LargeClaudeThread
	} from '$lib/api';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import { shortId } from '$lib/format';

	// each read scans the tail of every open thread's transcript, so poll slowly
	const REFRESH_MS = 60_000;

	let threads = $state<readonly LargeClaudeThread[]>([]);
	let minTokens = $state(200_000);
	let error = $state<string | null>(null);
	let lastFetched = $state<number | null>(null);
	let pending = $state<string | null>(null);
	/** Result of the last compaction request per session. */
	let results = $state<Record<string, CompactResult>>({});
	/** Thread the confirmation dialog names. */
	let target = $state<LargeClaudeThread | null>(null);
	let confirming = $state(false);

	async function refresh() {
		try {
			const answer = await fetchLargeClaudeThreads();
			threads = answer.threads;
			minTokens = answer.min_tokens;
			error = null;
			lastFetched = Date.now();
		} catch (cause) {
			error = cause instanceof Error ? cause.message : String(cause);
		}
	}

	useInterval(() => REFRESH_MS, { callback: () => void refresh() });
	$effect(() => {
		void refresh();
	});

	async function compact(thread: LargeClaudeThread) {
		pending = thread.session;
		try {
			results[thread.session] = await compactClaudeThread(thread.session);
		} catch (cause) {
			error = cause instanceof Error ? cause.message : String(cause);
		} finally {
			pending = null;
		}
	}

	function tokens(count: number): string {
		return `${Math.round(count / 1000)}k`;
	}

	function resultText(result: CompactResult): string {
		if (result.outcome === 'started') return 'Compacting in T3';
		return result.detail ?? result.outcome.replace('_', ' ');
	}
</script>

<div class="mx-auto max-w-5xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			all workers
		</a>
		<h1 class="text-base font-semibold tracking-tight">Large Claude threads</h1>
		<span class="ml-auto text-muted-foreground">
			{#if lastFetched}
				updated <Elapsed from={lastFetched} suffix="ago" />
			{:else}
				loading
			{/if}
		</span>
	</header>

	<p class="mt-2 text-muted-foreground">
		Open T3 threads whose Claude context is at least {tokens(minTokens)} tokens. Compacting while the
		cache is warm reads the context at the cached price; a cold thread pays one full read, which its next
		message would pay anyway.
	</p>

	{#if error}
		<p
			class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
			role="alert"
		>
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span class="min-w-0 flex-1 wrap-anywhere">{error}</span>
		</p>
	{/if}

	<section
		aria-label="Threads"
		class="mt-3 overflow-hidden rounded-lg border border-border bg-card"
	>
		<div
			class="hidden grid-cols-[minmax(0,1fr)_5rem_7rem_5rem_6rem] gap-x-3 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase sm:grid"
			aria-hidden="true"
		>
			<span>Thread</span>
			<span>Context</span>
			<span>Idle</span>
			<span>Cache</span>
			<span></span>
		</div>
		{#if lastFetched !== null && threads.length === 0}
			<p class="px-3 py-6 text-center text-muted-foreground">
				No open thread has {tokens(minTokens)} tokens of context
			</p>
		{/if}
		<ul>
			{#each threads as thread (thread.session)}
				{@const result = results[thread.session]}
				<li
					class="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-1 border-b border-border/70 px-3 py-2 last:border-b-0 sm:grid-cols-[minmax(0,1fr)_5rem_7rem_5rem_6rem]"
				>
					<span class="flex min-w-0 flex-col">
						<span class="truncate font-medium" title={thread.title}>{thread.title}</span>
						<span class="font-mono text-[11px] text-muted-foreground" title={thread.session}>
							{shortId(thread.session)}
							{#if result}
								<span
									class={result.outcome === 'started'
										? 'text-emerald-700 dark:text-emerald-300'
										: 'text-red-700 dark:text-red-300'}
								>
									· {resultText(result)}
								</span>
							{/if}
						</span>
					</span>
					<span class="font-mono">{tokens(thread.tokens)}</span>
					<span class="text-muted-foreground"><Elapsed from={thread.last_active} /></span>
					<span>
						{#if thread.cache_warm}
							<span class="text-emerald-700 dark:text-emerald-300">warm</span>
						{:else}
							<span class="text-muted-foreground">cold</span>
						{/if}
					</span>
					<span class="flex justify-end">
						<button
							type="button"
							disabled={pending !== null}
							onclick={() => {
								target = thread;
								confirming = true;
							}}
							class="rounded border border-border px-2 py-0.5 text-[11px] leading-5 font-medium hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:opacity-40"
						>
							{pending === thread.session ? 'Sending' : 'Compact'}
						</button>
					</span>
				</li>
			{/each}
		</ul>
	</section>
</div>

<ConfirmDialog
	bind:open={confirming}
	title={`Compact ${target?.title ?? 'thread'}?`}
	confirmLabel="Compact"
	onConfirm={() => {
		if (target) void compact(target);
	}}
>
	Claude replaces the thread's history with a summary. Messages sent during the compaction wait
	until it finishes.
	{#if target && !target.cache_warm}
		The cache is cold, so the compaction reads the full {tokens(target.tokens)} tokens once.
	{/if}
</ConfirmDialog>
