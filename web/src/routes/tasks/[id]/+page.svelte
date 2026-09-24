<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	const outcomeBadge = tv({
		base: 'rounded px-1.5 py-0.5 font-mono text-[11px] leading-4 ring-1 ring-inset',
		variants: {
			outcome: {
				succeeded: 'bg-emerald-500/10 text-emerald-700 ring-emerald-500/40 dark:text-emerald-300',
				failed: 'bg-red-500/10 text-red-700 ring-red-500/40 dark:text-red-300',
				blocked: 'bg-amber-500/10 text-amber-700 ring-amber-500/40 dark:text-amber-300'
			}
		}
	});
</script>

<script lang="ts">
	import { untrack } from 'svelte';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import { isInFlight } from '$lib/api';
	import CallbackBadge from '$lib/components/CallbackBadge.svelte';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import { LOG_TAIL_LINES, TaskStore } from '$lib/daemon.svelte';
	import {
		EM_DASH,
		exitReasonText,
		formatCommandArgv,
		formatDuration,
		formatTimestamp,
		shortId,
		shortenHome,
		workloadLabel
	} from '$lib/format';

	const store = new TaskStore(() => page.params.id ?? '');
	const task = $derived(store.task);
	const live = $derived(task !== null && isInFlight(task.status));

	let logBox = $state<HTMLElement | null>(null);
	// Auto-follow starts on and stops as soon as the reader scrolls up.
	let following = $state(true);

	function onLogScroll(event: Event) {
		const element = event.currentTarget as HTMLElement;
		following = element.scrollHeight - element.scrollTop - element.clientHeight < 24;
	}

	$effect(() => {
		const log = store.logTail?.log;
		if (log === undefined || logBox === null) return;
		if (!untrack(() => following) || !untrack(() => live)) return;
		logBox.scrollTop = logBox.scrollHeight;
	});
</script>

<div class="mx-auto max-w-5xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			all workers
		</a>
		<a href={resolve('/resources')} class="text-primary hover:underline">resources</a>
		{#if task}
			<h1 class="text-base font-semibold tracking-tight" title={task.display_name}>
				{task.display_name}
			</h1>
			<CopyPath value={page.params.id ?? ''} label={shortId(page.params.id ?? '', 13)} />
			<StatusBadge status={task.status} />
			<span class="text-muted-foreground">
				updated {formatTimestamp(task.updated_at)}
			</span>
			{#if task.cancel_requested_at}
				<span class="font-mono text-[11px] text-amber-600 dark:text-amber-400">
					cancel requested {formatTimestamp(task.cancel_requested_at)}
				</span>
			{/if}
		{:else}
			<CopyPath value={page.params.id ?? ''} label={shortId(page.params.id ?? '', 13)} />
		{/if}
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				refreshed <Elapsed from={store.lastFetched} suffix="ago" />
			{:else}
				loading
			{/if}
		</span>
	</header>

	{#if store.error}
		<p
			class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
		>
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span>
				<span class="font-mono">{store.error.code}</span>
				&mdash; {store.error.message}
			</span>
		</p>
	{/if}

	{#if task}
		<dl
			class="mt-3 grid grid-cols-[7rem_minmax(0,1fr)] gap-x-3 gap-y-1 rounded border border-border bg-card px-3 py-2 sm:grid-cols-[7rem_minmax(0,1fr)_7rem_minmax(0,1fr)]"
		>
			<dt class="text-muted-foreground">workload</dt>
			<dd class="font-mono">{workloadLabel(task)}</dd>

			<dt class="text-muted-foreground">pid</dt>
			<dd class="font-mono">{task.pid ?? EM_DASH}</dd>

			<dt class="text-muted-foreground">cwd</dt>
			<dd class="flex min-w-0 items-center gap-1">
				<a
					href={resolve(`/files?path=${encodeURIComponent(task.cwd)}`)}
					class="truncate font-mono text-primary hover:underline"
					title={task.cwd}
				>
					{shortenHome(task.cwd)}
				</a>
				<CopyPath value={task.cwd} iconOnly class="relative z-10" />
			</dd>

			<dt class="text-muted-foreground">thread</dt>
			<dd class="flex min-w-0 items-center gap-1">
				<CopyPath value={task.thread} label={shortId(task.thread, 13)} class="-ml-1" />
				<a
					href={resolve(`/?thread=${task.thread}`)}
					class="text-primary hover:underline"
					title="filter the dashboard by this thread"
				>
					filter
				</a>
			</dd>

			<dt class="text-muted-foreground">created</dt>
			<dd>
				{formatTimestamp(task.created_at)}
				<span class="text-muted-foreground">
					(<Elapsed from={task.created_at} suffix="ago" />)
				</span>
			</dd>

			<dt class="text-muted-foreground">{live ? 'elapsed' : 'duration'}</dt>
			<dd>
				<Elapsed from={task.created_at} to={live ? null : task.updated_at} />
			</dd>

			<dt class="text-muted-foreground">updated</dt>
			<dd>{formatTimestamp(task.updated_at)}</dd>

			<dt class="text-muted-foreground">check timeout</dt>
			<dd class="tabular-nums">
				{formatDuration(task.timeout_secs * 1000)}
				<span class="ml-1 text-muted-foreground">({task.check_timeout})</span>
			</dd>

			{#if task.workload.type === 'task'}
				<dt class="text-muted-foreground">command</dt>
				<dd class="font-mono whitespace-pre-wrap sm:col-span-3">
					{formatCommandArgv(task.workload.command)}
				</dd>
			{/if}

			<dt class="text-muted-foreground">exit</dt>
			<dd class="font-mono">{exitReasonText(task.exit_reason)}</dd>

			<dt class="text-muted-foreground">callback</dt>
			<dd><CallbackBadge callback={task.callback} /></dd>

			<dt class="text-muted-foreground">evidence</dt>
			<dd class="flex min-w-0 items-center gap-1">
				<a
					href={resolve(`/files?path=${encodeURIComponent(task.evidence)}`)}
					class="truncate font-mono text-primary hover:underline"
					title={task.evidence}
				>
					{shortenHome(task.evidence)}
				</a>
				<CopyPath value={task.evidence} iconOnly class="relative z-10" />
			</dd>

			<dt class="text-muted-foreground">output log</dt>
			<dd class="flex min-w-0 items-center gap-1 sm:col-span-3">
				<a
					href={resolve(`/files?path=${encodeURIComponent(task.output_log)}`)}
					target="_blank"
					rel="noopener noreferrer"
					class="truncate font-mono text-primary hover:underline"
					title={task.output_log}
				>
					{shortenHome(task.output_log)}
				</a>
				<CopyPath value={task.output_log} iconOnly class="relative z-10" />
			</dd>
		</dl>

		<section class="mt-4">
			<h2 class="mb-1 text-[11px] tracking-wide text-muted-foreground uppercase">
				reports ({task.reports.length})
			</h2>
			{#if task.reports.length === 0}
				<p class="rounded border border-border bg-card px-3 py-2 text-muted-foreground">
					No reports yet
				</p>
			{:else}
				<ol class="divide-y divide-border rounded border border-border bg-card">
					{#each task.reports as report (report.seq)}
						<li class="px-3 py-2">
							<div class="flex flex-wrap items-center gap-2">
								<span class="font-mono text-muted-foreground">#{report.seq}</span>
								<span class={outcomeBadge({ outcome: report.outcome })}>{report.outcome}</span>
								<span class="text-muted-foreground">{formatTimestamp(report.reported_at)}</span>
								{#if report.notified_at}
									<span class="text-muted-foreground">
										notified {formatTimestamp(report.notified_at)}
									</span>
								{/if}
							</div>
							<p class="mt-1 break-words whitespace-pre-wrap">{report.summary}</p>
						</li>
					{/each}
				</ol>
			{/if}
		</section>

		<section class="mt-4">
			<div class="mb-1 flex flex-wrap items-center gap-2">
				<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">
					output log (last {LOG_TAIL_LINES} lines)
				</h2>
				{#if store.logTail?.truncated}
					<span class="text-[11px] text-muted-foreground">earlier lines dropped</span>
				{/if}
				{#if live}
					<span class="text-[11px] text-muted-foreground">
						{following ? 'following' : 'paused (scroll to bottom to follow)'}
					</span>
				{/if}
			</div>
			<pre
				id="output-log"
				bind:this={logBox}
				onscroll={onLogScroll}
				class="max-h-96 overflow-auto rounded border border-border bg-card px-3 py-2 font-mono text-[12px] leading-5 whitespace-pre-wrap">{store
					.logTail?.log || 'No output yet'}</pre>
		</section>

		{#if task.last_event}
			<details class="mt-4 rounded border border-border bg-card px-3 py-2">
				<summary class="cursor-pointer text-[11px] tracking-wide text-muted-foreground uppercase">
					last event &middot; {task.last_event.event}
				</summary>
				<pre class="mt-2 overflow-auto font-mono text-[12px] leading-5">{JSON.stringify(
						task.last_event,
						null,
						2
					)}</pre>
			</details>
		{/if}
	{/if}
</div>
