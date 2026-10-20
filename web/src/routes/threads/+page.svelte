<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import CircleCheck from '@lucide/svelte/icons/circle-check';
	import CornerDownRight from '@lucide/svelte/icons/corner-down-right';
	import Cpu from '@lucide/svelte/icons/cpu';
	import GitBranch from '@lucide/svelte/icons/git-branch';
	import GitPullRequest from '@lucide/svelte/icons/git-pull-request';
	import Hourglass from '@lucide/svelte/icons/hourglass';
	import LoaderCircle from '@lucide/svelte/icons/loader-circle';
	import { untrack } from 'svelte';
	import { useInterval } from 'runed';
	import {
		compactClaudeThread,
		fetchLargeClaudeThreads,
		type CompactResult,
		type LargeClaudeThread
	} from '$lib/api';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import { formatDuration, shortId } from '$lib/format';

	// each read scans the tail of every open thread's transcript, so poll slowly
	const REFRESH_MS = 60_000;
	// settled threads idle longer than this are left out, even behind "show more"
	const SETTLED_LIMIT_MS = 48 * 60 * 60 * 1000;
	// matches the daemon's one-hour prompt cache lifetime
	const CACHE_MS = 60 * 60 * 1000;
	// warm threads with less cache life than this go in the expiring block
	const EXPIRING_MS = 15 * 60 * 1000;
	// only contexts above this size are worth a warning, whatever the listing size
	const EXPIRING_MIN_TOKENS = 200_000;
	// a compaction takes about a minute, so poll faster until it lands
	const COMPACTING_REFRESH_MS = 5_000;
	// far past the minutes a large compaction takes, so a lost one stops spinning
	const COMPACTING_TIMEOUT_MS = 10 * 60 * 1000;

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
	let showSettled = $state(false);
	let showStale = $state(false);
	/** Whether to list threads below the listing size too. */
	let showSmall = $state(false);
	let now = $state(Date.now());
	/** Threads T3 is compacting, as they were when the request started. */
	let compacting = $state<Record<string, { before: LargeClaudeThread; startedAt: number }>>({});
	/**
	 * Compactions that landed since the page loaded, newest first. `after` follows the thread's
	 * size, an estimate until its next request, and is null once the thread closed.
	 */
	let compacted = $state<{ before: LargeClaudeThread; after: LargeClaudeThread | null }[]>([]);

	// sessions this page asks for below the listing size are for `compacted` only
	const listed = $derived(
		showSmall ? threads : threads.filter((thread) => thread.tokens >= minTokens)
	);
	const warmCount = $derived(listed.filter(warm).length);

	function cacheLeft(thread: LargeClaudeThread): number {
		return CACHE_MS - (now - Date.parse(thread.last_active));
	}

	// the server answers warm as of its last read, so let a countdown that hit
	// zero since then count as cold
	function warm(thread: LargeClaudeThread): boolean {
		return thread.cache_warm && cacheLeft(thread) > 0;
	}

	function expiring(thread: LargeClaudeThread): boolean {
		return warm(thread) && thread.tokens > EXPIRING_MIN_TOKENS && cacheLeft(thread) < EXPIRING_MS;
	}

	const expiringThreads = $derived(
		listed.filter(expiring).toSorted((a, b) => cacheLeft(a) - cacheLeft(b))
	);

	/** One block of the list, in the order the page shows them. */
	interface Tier {
		key: string;
		title: string;
		note: string;
		threads: readonly LargeClaudeThread[];
		stale: boolean;
	}

	// cold threads cost a full read to compact whatever their size, so they list newest first
	function newestFirst(list: readonly LargeClaudeThread[]): LargeClaudeThread[] {
		return list.toSorted((a, b) => Date.parse(b.last_active) - Date.parse(a.last_active));
	}

	const tiers = $derived.by((): Tier[] => {
		const pick = (settled: boolean, isWarm: boolean) =>
			listed.filter(
				(thread) => thread.settled === settled && warm(thread) === isWarm && !expiring(thread)
			);
		return [
			{
				key: 'active',
				title: 'Active, last hour',
				note: 'cache warm',
				threads: pick(false, true),
				stale: false
			},
			{
				key: 'settled',
				title: 'Settled in T3, last hour',
				note: 'cache warm',
				threads: pick(true, true),
				stale: false
			},
			{
				key: 'stale',
				title: 'Active, idle over an hour',
				note: 'cache cold, so compacting reads the full context once',
				threads: newestFirst(pick(false, false)),
				stale: true
			}
		];
	});

	const settledCold = $derived(
		newestFirst(
			listed.filter(
				(thread) =>
					thread.settled &&
					!warm(thread) &&
					(lastFetched ?? Date.now()) - Date.parse(thread.last_active) <= SETTLED_LIMIT_MS
			)
		)
	);

	async function refresh() {
		try {
			const followed = [
				...Object.keys(compacting),
				...compacted.map((done) => done.before.session)
			];
			const answer = await fetchLargeClaudeThreads({ all: showSmall, include: followed });
			settleCompactions(answer.threads);
			threads = answer.threads;
			minTokens = answer.min_tokens;
			error = null;
			lastFetched = Date.now();
		} catch (cause) {
			error = cause instanceof Error ? cause.message : String(cause);
		}
	}

	/**
	 * Move compactions that landed into `compacted`, and follow the size of those that did. A
	 * finished compaction shows as a smaller context, and a thread that closed is no longer listed
	 */
	function settleCompactions(fresh: readonly LargeClaudeThread[]) {
		const find = (session: string) => fresh.find((thread) => thread.session === session) ?? null;
		for (const done of compacted) {
			done.after = find(done.before.session) ?? done.after;
		}
		for (const [session, { before, startedAt }] of Object.entries(compacting)) {
			const after = find(session);
			if (after === null || after.tokens < before.tokens) {
				delete compacting[session];
				compacted.unshift({ before, after });
			} else if (Date.now() - startedAt > COMPACTING_TIMEOUT_MS) {
				delete compacting[session];
				error = `${before.title} has not finished compacting after 10 minutes. Check the thread in T3`;
			}
		}
	}

	const compactingCount = $derived(Object.keys(compacting).length);

	useInterval(() => (compactingCount > 0 ? COMPACTING_REFRESH_MS : REFRESH_MS), {
		callback: () => void refresh()
	});
	useInterval(() => 1000, { callback: () => (now = Date.now()) });
	// refresh reads the compaction state, which must not rerun this first load
	$effect(() => {
		untrack(() => void refresh());
	});

	async function compact(thread: LargeClaudeThread) {
		pending = thread.session;
		try {
			const result = await compactClaudeThread(thread.session);
			results[thread.session] = result;
			if (result.outcome === 'started') {
				compacting[thread.session] = { before: thread, startedAt: Date.now() };
			}
		} catch (cause) {
			error = cause instanceof Error ? cause.message : String(cause);
		} finally {
			pending = null;
		}
	}

	function tokens(count: number): string {
		return `${Math.round(count / 1000)}k`;
	}

	/** Size of `thread`, marked when it is an estimate after a compaction. */
	function size(thread: LargeClaudeThread): string {
		return `${thread.tokens_estimated ? '~' : ''}${tokens(thread.tokens)}`;
	}

	const ESTIMATE_NOTE =
		"Estimated from the summary until the thread's next message reports the size";

	function resultText(result: CompactResult): string {
		return result.detail ?? result.outcome.replace('_', ' ');
	}
</script>

<div class="mx-auto max-w-page px-4 py-4 lg:px-6">
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

	<p class="mt-2 max-w-4xl text-muted-foreground">
		{showSmall
			? 'Every open T3 Claude thread.'
			: `Open T3 threads whose Claude context is at least ${tokens(minTokens)} tokens.`} Compacting while
		the cache is warm reads the context at the cached price. A cold thread pays one full read, which its
		next message would pay anyway, and every later request then reads the short summary.
	</p>

	<div class="mt-3 flex flex-wrap items-center gap-x-4 gap-y-2">
		{#if lastFetched !== null}
			<span class="text-muted-foreground tabular-nums">
				{listed.length}
				{listed.length === 1 ? 'thread' : 'threads'}
				<span aria-hidden="true">·</span>
				<span class="text-emerald-700 dark:text-emerald-300">{warmCount} warm</span>
				{#if expiringThreads.length > 0}
					<span aria-hidden="true">·</span>
					<span class="font-medium text-amber-700 dark:text-amber-300">
						{expiringThreads.length} expiring soon
					</span>
				{/if}
			</span>
		{/if}
		<button
			type="button"
			role="switch"
			aria-checked={showSmall}
			onclick={() => {
				showSmall = !showSmall;
				void refresh();
			}}
			class="ml-auto inline-flex items-center gap-2 rounded-full py-0.5 pr-1 select-none focus-visible:outline-2 focus-visible:outline-primary"
		>
			<span
				class={[
					'relative inline-flex h-4 w-7 shrink-0 items-center rounded-full transition-colors',
					showSmall ? 'bg-primary' : 'bg-muted-foreground/30'
				]}
			>
				<span
					class={[
						'absolute size-3 rounded-full bg-white shadow-sm transition-transform',
						showSmall ? 'translate-x-3.5' : 'translate-x-0.5'
					]}
				></span>
			</span>
			Show all threads
		</button>
	</div>

	{#if error}
		<p
			class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
			role="alert"
		>
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span class="min-w-0 flex-1 wrap-anywhere">{error}</span>
		</p>
	{/if}

	{#if compacted.length > 0}
		<ul class="mt-3 flex flex-col gap-1" aria-live="polite">
			{#each compacted as done (done.before.session + done.before.last_active)}
				<li
					class="flex items-center gap-2 rounded border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-emerald-800 dark:text-emerald-300"
				>
					<CircleCheck class="size-4 shrink-0" />
					<span class="min-w-0 flex-1 truncate">
						Compacted <span class="font-medium">{done.before.title}</span>:
						{tokens(done.before.tokens)} →
						{#if done.after}
							<span
								class="font-semibold tabular-nums"
								title={done.after.tokens_estimated ? ESTIMATE_NOTE : undefined}
							>
								{size(done.after)}
							</span>
							{#if done.after.tokens_estimated}
								<span class="text-[11px] opacity-80">estimate</span>
							{/if}
						{:else}
							thread closed
						{/if}
					</span>
				</li>
			{/each}
		</ul>
	{/if}

	{#if expiringThreads.length > 0}
		<section
			aria-label="Cache expiring soon"
			class="mt-3 overflow-hidden rounded-lg border-2 border-amber-500 bg-card"
		>
			<h2
				class="flex flex-wrap items-center gap-x-2 gap-y-0.5 bg-amber-500/15 px-3 py-2 text-amber-900 lg:px-4 dark:text-amber-200"
			>
				<Hourglass class="size-4 shrink-0" />
				<span class="font-semibold">
					Cache expiring soon · {expiringThreads.length}
					{expiringThreads.length === 1 ? 'thread' : 'threads'}
				</span>
				<span class="text-[12px]">
					Compact before the countdown ends to read the context at the cached price
				</span>
			</h2>
			<ul>
				{#each expiringThreads as thread (thread.session)}
					{@render row(thread, true)}
				{/each}
			</ul>
		</section>
	{/if}

	<section
		aria-label="Threads"
		class="mt-3 overflow-hidden rounded-lg border border-border bg-card"
	>
		<div
			class="hidden grid-cols-[minmax(0,1fr)_5rem_7rem_6rem_7rem] gap-x-3 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase sm:grid lg:grid-cols-[minmax(0,1fr)_6rem_8rem_8rem_7rem] lg:px-4"
			aria-hidden="true"
		>
			<span>Thread</span>
			<span>Context</span>
			<span>Idle</span>
			<span>Cache</span>
			<span></span>
		</div>
		{#if lastFetched !== null && expiringThreads.length === 0 && tiers.every((tier) => tier.threads.length === 0) && settledCold.length === 0}
			<p class="px-3 py-6 text-center text-muted-foreground">
				{showSmall
					? 'No open Claude thread'
					: `No open thread has ${tokens(minTokens)} tokens of context`}
			</p>
		{/if}
		{#each tiers as tier (tier.key)}
			{#if tier.threads.length > 0 && (!tier.stale || showStale)}
				{@render heading(tier.title, tier.note, tier.stale)}
				<ul>
					{#each tier.threads as thread (thread.session)}
						{@render row(thread, false)}
					{/each}
				</ul>
			{/if}
			<!-- cold threads cost a full read to compact, so they stay behind a click -->
			{#if tier.stale && tier.threads.length > 0}
				<button
					type="button"
					onclick={() => (showStale = !showStale)}
					class="w-full border-t border-border px-3 py-2 text-left text-primary hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary lg:px-4"
				>
					{showStale ? 'Hide' : 'Show'}
					{tier.threads.length} active {tier.threads.length === 1 ? 'thread' : 'threads'} idle over an
					hour, cache cold
				</button>
			{/if}
		{/each}
		{#if settledCold.length > 0}
			{#if showSettled}
				{@render heading('Settled in T3, idle 1 to 48 hours', 'cache cold', false)}
				<ul>
					{#each settledCold as thread (thread.session)}
						{@render row(thread, false)}
					{/each}
				</ul>
			{/if}
			<button
				type="button"
				onclick={() => (showSettled = !showSettled)}
				class="w-full border-t border-border px-3 py-2 text-left text-primary hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary lg:px-4"
			>
				{showSettled ? 'Hide' : 'Show'}
				{settledCold.length} settled {settledCold.length === 1 ? 'thread' : 'threads'} idle up to 48 hours
			</button>
		{/if}
	</section>
</div>

{#snippet heading(title: string, note: string, stale: boolean)}
	<h2
		class={[
			'border-b border-border px-3 py-1.5 text-[11px] font-semibold tracking-wide uppercase lg:px-4',
			stale
				? 'bg-amber-500/10 text-amber-800 dark:text-amber-300'
				: 'bg-muted/50 text-muted-foreground'
		]}
	>
		{title}
		<span class="font-normal tracking-normal normal-case">· {note}</span>
	</h2>
{/snippet}

{#snippet row(thread: LargeClaudeThread, urgent: boolean)}
	{@const result = results[thread.session]}
	{@const busy = compacting[thread.session] !== undefined}
	{@const left = cacheLeft(thread)}
	{@const details = thread.details}
	<li
		class={[
			'flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border/70 px-3 py-2 last:border-b-0 sm:grid sm:grid-cols-[minmax(0,1fr)_5rem_7rem_6rem_7rem] lg:grid-cols-[minmax(0,1fr)_6rem_8rem_8rem_7rem] lg:px-4',
			busy && 'bg-sky-500/10'
		]}
	>
		<span class="flex min-w-0 basis-full flex-col gap-0.5 sm:basis-auto">
			<span class="flex min-w-0 items-center gap-1.5">
				{#if details?.project}
					<span
						class="shrink-0 rounded bg-muted px-1.5 text-[11px] leading-5 font-medium text-foreground/80"
						title={details.workspace ?? undefined}
					>
						{details.project}
					</span>
				{/if}
				<span class="truncate font-medium" title={thread.title}>{thread.title}</span>
			</span>
			<span
				class="flex min-w-0 flex-wrap items-center gap-x-2.5 gap-y-0.5 text-[11px] text-muted-foreground"
			>
				{#if details?.branch}
					<span
						class="inline-flex min-w-0 items-center gap-1"
						title={details.workspace ?? undefined}
					>
						<GitBranch class="size-3 shrink-0" />
						<span class="max-w-56 truncate font-mono lg:max-w-sm 2xl:max-w-md"
							>{details.branch}</span
						>
					</span>
				{/if}
				{#if details?.model}
					<span class="inline-flex items-center gap-1">
						<Cpu class="size-3 shrink-0" />
						{details.model.replace(/^claude-/, '')}{details.effort ? ` · ${details.effort}` : ''}
					</span>
				{/if}
				{#if details?.parent_title}
					<span class="inline-flex min-w-0 items-center gap-1" title="Started by this thread">
						<CornerDownRight class="size-3 shrink-0" />
						<span class="max-w-56 truncate lg:max-w-sm 2xl:max-w-md">{details.parent_title}</span>
					</span>
				{/if}
				{#if details?.pull_request}
					<!-- eslint-disable svelte/no-navigation-without-resolve -- external pull request page, not an app route -->
					<a
						href={details.pull_request.url}
						target="_blank"
						rel="noreferrer"
						class="inline-flex items-center gap-1 text-primary hover:underline"
					>
						<GitPullRequest class="size-3 shrink-0" />
						#{details.pull_request.number}{details.pull_request.state
							? ` ${details.pull_request.state}`
							: ''}
					</a>
					<!-- eslint-enable svelte/no-navigation-without-resolve -->
				{/if}
				{#if details?.created_at}
					<span title="Thread created">
						<Elapsed from={details.created_at} suffix="old" />
					</span>
				{/if}
				<span class="font-mono" title={thread.session}>{shortId(thread.session)}</span>
				{#if result && result.outcome !== 'started'}
					<span class="text-red-700 dark:text-red-300">{resultText(result)}</span>
				{/if}
			</span>
		</span>
		<span class="font-mono" title={thread.tokens_estimated ? ESTIMATE_NOTE : undefined}>
			{size(thread)}
		</span>
		<span class="text-muted-foreground"><Elapsed from={thread.last_active} /></span>
		<span class="tabular-nums">
			{#if !warm(thread)}
				<span class="text-muted-foreground">cold</span>
			{:else if urgent}
				<span
					class={[
						'font-semibold',
						left <= 5 * 60 * 1000
							? 'text-red-700 dark:text-red-300'
							: 'text-amber-700 dark:text-amber-300'
					]}
					title="Time until the prompt cache expires"
				>
					{formatDuration(left)} left
				</span>
			{:else}
				<span
					class="text-emerald-700 dark:text-emerald-300"
					title="Time until the prompt cache expires"
				>
					warm · {formatDuration(left).split(' ')[0]}
				</span>
			{/if}
		</span>
		<span class="ml-auto flex justify-end sm:ml-0">
			{#if busy}
				<span
					class="inline-flex items-center gap-1 rounded border border-sky-500/50 bg-sky-500/15 px-2 py-0.5 text-[11px] leading-5 font-medium text-sky-800 dark:text-sky-200"
					role="status"
				>
					<LoaderCircle class="size-3 animate-spin" />
					Compacting
				</span>
			{:else}
				<button
					type="button"
					disabled={pending !== null}
					onclick={() => {
						target = thread;
						confirming = true;
					}}
					class={[
						'inline-flex items-center gap-1 rounded border px-2 py-0.5 text-[11px] leading-5 font-medium focus-visible:outline-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:opacity-40',
						urgent
							? 'border-amber-600 bg-amber-500 text-white hover:bg-amber-600'
							: 'border-border hover:bg-accent'
					]}
				>
					{#if pending === thread.session}
						<LoaderCircle class="size-3 animate-spin" />
						Sending
					{:else}
						Compact
					{/if}
				</button>
			{/if}
		</span>
	</li>
{/snippet}

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
	{#if target && !warm(target)}
		The cache is cold, so the compaction reads the full {tokens(target.tokens)} tokens once.
	{/if}
</ConfirmDialog>
