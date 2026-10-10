<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import X from '@lucide/svelte/icons/x';
	import { useInterval } from 'runed';
	import { fetchUsage, type UsageGroup, type UsageReport, type UsageTask } from '$lib/api';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import PartialNote from '$lib/components/PartialNote.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import ThreadLabel from '$lib/components/ThreadLabel.svelte';
	import { EM_DASH, formatTimestamp, shortId } from '$lib/format';
	import { ThreadTitleStore } from '$lib/thread-titles.svelte';
	import {
		DEFAULT_USAGE_WINDOW,
		TASK_NOTE,
		USAGE_WINDOWS,
		formatCost,
		formatTokens,
		groupNote,
		modelLabel,
		windowStart,
		type UsageWindowKey
	} from '$lib/usage-view';

	// the daemon reads every finished task's accounting, so poll slowly
	const REFRESH_MS = 60_000;
	// the costliest tasks are the useful ones; the rest sit behind "show all"
	const TASK_LIMIT = 25;

	let report = $state<UsageReport | null>(null);
	let windowKey = $state<UsageWindowKey>(DEFAULT_USAGE_WINDOW);
	/** Thread the report is narrowed to, or null for every thread. */
	let threadFilter = $state<string | null>(null);
	let error = $state<string | null>(null);
	let lastFetched = $state<number | null>(null);
	let showAllTasks = $state(false);
	// a slow read of an old window must not overwrite the read of the current one
	let latestRead = 0;

	const visibleTasks = $derived(
		report === null ? [] : showAllTasks ? report.tasks : report.tasks.slice(0, TASK_LIMIT)
	);
	// by_thread keys are unique, so only the filter can repeat one
	const threadTitles = new ThreadTitleStore(() => {
		const threads = (report?.by_thread ?? []).map((group) => group.key);
		if (threadFilter && !threads.includes(threadFilter)) threads.push(threadFilter);
		return threads.map((thread) => ({ thread }));
	});
	const totals = $derived(report?.totals ?? null);

	async function refresh(key: UsageWindowKey, thread: string | null) {
		const read = ++latestRead;
		try {
			const answer = await fetchUsage(windowStart(key, Date.now()), thread ?? undefined);
			if (read !== latestRead) return;
			report = answer;
			error = null;
			lastFetched = Date.now();
		} catch (cause) {
			if (read !== latestRead) return;
			error = cause instanceof Error ? cause.message : String(cause);
		}
	}

	useInterval(() => REFRESH_MS, { callback: () => void refresh(windowKey, threadFilter) });
	$effect(() => {
		void refresh(windowKey, threadFilter);
	});

	function selectWindow(key: UsageWindowKey) {
		windowKey = key;
		showAllTasks = false;
	}

	function threadName(thread: string): string {
		return threadTitles.title({ thread }) ?? shortId(thread);
	}

	function models(task: UsageTask): string {
		const names = task.usage.models.map((entry) => modelLabel(entry.model));
		return names.length > 0 ? names.join(', ') : EM_DASH;
	}

	const GROUP_COLUMNS = [
		{ label: 'Cost', align: 'right' },
		{ label: 'Tasks', align: 'right' },
		{ label: 'Input', align: 'right' },
		{ label: 'Output', align: 'right' },
		{ label: 'Cache read', align: 'right' },
		{ label: 'Cache write', align: 'right' }
	] as const;
	const th =
		'border-b border-border bg-muted px-3 py-1.5 text-[11px] font-normal tracking-wide whitespace-nowrap text-muted-foreground uppercase';
	const td = 'border-b border-border/70 px-3 py-1.5 whitespace-nowrap group-last:border-b-0';
	const num = `${td} text-right font-mono tabular-nums`;
</script>

<div class="mx-auto max-w-5xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			all workers
		</a>
		<h1 class="text-base font-semibold tracking-tight">Claude usage</h1>
		<span class="ml-auto text-muted-foreground">
			{#if lastFetched}
				updated <Elapsed from={lastFetched} suffix="ago" />
			{:else}
				loading
			{/if}
		</span>
	</header>

	<p class="mt-2 text-muted-foreground">
		Tokens and cost of finished Claude worker tasks on this machine. A cost marked * is an
		undercount.
	</p>

	<div class="mt-3 flex flex-wrap items-center gap-x-3 gap-y-2">
		<div
			role="group"
			aria-label="Time window"
			class="inline-flex overflow-hidden rounded border border-border"
		>
			{#each USAGE_WINDOWS as option (option.key)}
				<button
					type="button"
					aria-pressed={windowKey === option.key}
					onclick={() => selectWindow(option.key)}
					class={[
						'px-3 py-1 font-medium focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-primary',
						windowKey === option.key
							? 'bg-primary text-primary-foreground'
							: 'bg-card hover:bg-accent'
					]}
				>
					{option.label}
				</button>
			{/each}
		</div>
		{#if threadFilter}
			<button
				type="button"
				onclick={() => (threadFilter = null)}
				class="inline-flex max-w-full items-center gap-1.5 rounded border border-border bg-card px-2 py-1 hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary"
				aria-label="Show every thread"
			>
				<span class="text-muted-foreground">thread</span>
				<span class="truncate font-medium">{threadName(threadFilter)}</span>
				<X class="size-3.5 shrink-0" aria-hidden="true" />
			</button>
		{/if}
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

	{#if totals && report}
		{@const partial = totals.partial_tasks > 0}
		<section
			aria-label="Totals"
			class="mt-3 grid grid-cols-2 gap-px overflow-hidden rounded-lg border border-border bg-border sm:grid-cols-5"
		>
			{@render stat(
				'Cost',
				formatCost(totals.cost_usd),
				partial ? groupNote(totals.partial_tasks, totals.tasks) : undefined
			)}
			{@render stat('Tasks', totals.tasks.toLocaleString())}
			{@render stat('Output', formatTokens(totals.output_tokens))}
			{@render stat('Cache read', formatTokens(totals.cache_read_tokens))}
			{@render stat('Cache write', formatTokens(totals.cache_write_tokens))}
		</section>

		<p class="mt-1 text-[11px] text-muted-foreground">
			Since {formatTimestamp(report.since)}
		</p>

		{@render groupTable('By model', 'Model', report.by_model, modelCell)}
		{@render groupTable('By day', 'Day', report.by_day, dayCell)}
		{@render groupTable('By thread', 'Thread', report.by_thread, threadCell)}

		<section aria-label="Top tasks" class="mt-4">
			<h2 class="mb-1 text-[11px] font-semibold tracking-wide text-muted-foreground uppercase">
				Top tasks
			</h2>
			<div class="overflow-x-auto rounded-lg border border-border bg-card">
				<table class="w-full border-collapse text-left">
					<thead>
						<tr>
							<th class={th}>Task</th>
							<th class="{th} text-right">Cost</th>
							<th class="{th} text-right">Turns</th>
							<th class="{th} text-right">Output</th>
							<th class="{th} text-right">Cache read</th>
							<th class="{th} text-right">Cache write</th>
							<th class={th}>Model</th>
							<th class={th}>Status</th>
							<th class={th}>Created</th>
						</tr>
					</thead>
					<tbody>
						{#each visibleTasks as task (task.task)}
							{@render taskRow(task)}
						{:else}
							<tr>
								<td colspan="9" class="px-3 py-6 text-center text-muted-foreground">
									No finished Claude tasks in this window
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
				{#if report.tasks.length > TASK_LIMIT}
					<button
						type="button"
						onclick={() => (showAllTasks = !showAllTasks)}
						class="w-full border-t border-border px-3 py-2 text-left text-primary hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary"
					>
						{showAllTasks ? `Show top ${TASK_LIMIT}` : `Show all ${report.tasks.length} tasks`}
					</button>
				{/if}
			</div>
		</section>
	{:else if lastFetched === null && !error}
		<p class="mt-6 text-center text-muted-foreground">Loading usage</p>
	{/if}
</div>

{#snippet stat(label: string, value: string, undercount?: string)}
	<div class="bg-card px-3 py-2">
		<div class="text-[11px] tracking-wide text-muted-foreground uppercase">{label}</div>
		<div class="font-mono text-lg font-semibold tabular-nums">
			{value}{#if undercount}<PartialNote text={undercount} />{/if}
		</div>
	</div>
{/snippet}

{#snippet groupTable(
	title: string,
	keyLabel: string,
	groups: readonly UsageGroup[],
	keyCell: import('svelte').Snippet<[UsageGroup]>
)}
	<section aria-label={title} class="mt-4">
		<h2 class="mb-1 text-[11px] font-semibold tracking-wide text-muted-foreground uppercase">
			{title}
		</h2>
		<div class="overflow-x-auto rounded-lg border border-border bg-card">
			<table class="w-full border-collapse text-left">
				<thead>
					<tr>
						<th class={th}>{keyLabel}</th>
						{#each GROUP_COLUMNS as column (column.label)}
							<th class="{th} text-right">{column.label}</th>
						{/each}
					</tr>
				</thead>
				<tbody>
					{#each groups as group (group.key)}
						{@const partial = group.partial_tasks > 0}
						<tr class="group">
							<td class="{td} max-w-64 min-w-0">{@render keyCell(group)}</td>
							<td class={num}>
								{formatCost(group.cost_usd)}{#if partial}<PartialNote
										text={groupNote(group.partial_tasks, group.tasks)}
									/>{/if}
							</td>
							<td class={num}>{group.tasks.toLocaleString()}</td>
							<td class={num}>{formatTokens(group.input_tokens)}</td>
							<td class={num}>{formatTokens(group.output_tokens)}</td>
							<td class={num}>{formatTokens(group.cache_read_tokens)}</td>
							<td class={num}>{formatTokens(group.cache_write_tokens)}</td>
						</tr>
					{:else}
						<tr>
							<td colspan="7" class="px-3 py-4 text-center text-muted-foreground">
								No finished Claude tasks in this window
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</section>
{/snippet}

{#snippet modelCell(group: UsageGroup)}
	<span class="block truncate font-mono" title={group.key}>{modelLabel(group.key)}</span>
{/snippet}

{#snippet dayCell(group: UsageGroup)}
	<span class="font-mono">{group.key}</span>
{/snippet}

{#snippet threadCell(group: UsageGroup)}
	<ThreadLabel
		thread={group.key}
		title={threadTitles.title({ thread: group.key })}
		filter={{
			active: threadFilter === group.key,
			toggle: () => {
				threadFilter = threadFilter === group.key ? null : group.key;
				showAllTasks = false;
			}
		}}
	/>
{/snippet}

{#snippet taskRow(task: UsageTask)}
	{@const usage = task.usage}
	<tr class="group">
		<td class="{td} max-w-64 min-w-0">
			<a
				href={resolve('/tasks/[id]', { id: task.task })}
				class="block truncate font-medium text-foreground hover:text-primary"
				title={task.name}
			>
				{task.name}
			</a>
			<span class="font-mono text-[11px] text-muted-foreground" title={task.task}>
				{shortId(task.task)}
			</span>
		</td>
		<td class={num}>
			{formatCost(usage.cost_usd)}{#if !usage.complete}<PartialNote text={TASK_NOTE} />{/if}
		</td>
		<td class={num}>{usage.turns.toLocaleString()}</td>
		<td class={num} title={`${usage.input_tokens.toLocaleString()} input`}>
			{formatTokens(usage.output_tokens)}
		</td>
		<td class={num}>{formatTokens(usage.cache_read_tokens)}</td>
		<td class={num}>{formatTokens(usage.cache_write_tokens)}</td>
		<td
			class="{td} max-w-40 truncate font-mono"
			title={usage.models.map((m) => m.model).join(', ')}
		>
			{models(task)}
		</td>
		<td class={td}><StatusBadge status={task.status} /></td>
		<td class="{td} whitespace-nowrap text-muted-foreground">{formatTimestamp(task.created_at)}</td>
	</tr>
{/snippet}
