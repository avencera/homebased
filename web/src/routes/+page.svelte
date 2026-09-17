<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import Ban from '@lucide/svelte/icons/ban';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import {
		IN_FLIGHT_STATUSES,
		PROCESS_STATUSES,
		isInFlight,
		isProcessStatus,
		type ProcessStatus,
		type TaskSummary
	} from '$lib/api';
	import CallbackBadge from '$lib/components/CallbackBadge.svelte';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import { DaemonStore } from '$lib/daemon.svelte';
	import { EM_DASH, formatTimestamp, shortId, shortenHome, workloadLabel } from '$lib/format';
	import { cn } from '$lib/utils';

	const statuses = $derived(parseStatuses(page.url.searchParams.get('status')));
	const thread = $derived(page.url.searchParams.get('thread'));
	const showFinished = $derived(statuses.some((status) => !isInFlight(status)));
	const inFlightOnly = $derived(!showFinished && statuses.length === IN_FLIGHT_STATUSES.length);

	const store = new DaemonStore(() => ({ statuses, thread }));

	function parseStatuses(raw: string | null): ProcessStatus[] {
		const parsed = (raw ?? '')
			.split(',')
			.map((part) => part.trim())
			.filter(isProcessStatus);
		return parsed.length > 0 ? parsed : [...IN_FLIGHT_STATUSES];
	}

	// The whole filter lives in the URL so a view can be bookmarked or shared
	function navigate(filters: { status: string | null; thread: string | null }) {
		const entries = Object.entries(filters).filter(
			(entry): entry is [string, string] => entry[1] !== null
		);
		const search = new URLSearchParams(entries).toString();
		void goto(resolve(search === '' ? '/' : `/?${search}`), {
			replaceState: true,
			keepFocus: true,
			noScroll: true
		});
	}

	function applyStatuses(next: ProcessStatus[]) {
		const isDefault =
			next.length === IN_FLIGHT_STATUSES.length && next.every((status) => isInFlight(status));
		const status = next.length === 0 || isDefault ? null : next.join(',');
		navigate({ status, thread });
	}

	function toggleStatus(status: ProcessStatus) {
		const next = statuses.includes(status)
			? statuses.filter((current) => current !== status)
			: [...statuses, status];
		applyStatuses(next);
	}

	function toggleFinished() {
		applyStatuses(showFinished ? [...IN_FLIGHT_STATUSES] : [...PROCESS_STATUSES]);
	}

	function clearThread() {
		navigate({ status: page.url.searchParams.get('status'), thread: null });
	}

	function rowEnd(task: TaskSummary): string | null {
		return isInFlight(task.status) ? null : task.updated_at;
	}
</script>

<div class="mx-auto max-w-7xl px-4 py-4">
	<header class="flex flex-wrap items-baseline gap-x-4 gap-y-1">
		<h1 class="text-base font-semibold tracking-tight">homebased</h1>
		<span class="flex items-center gap-1.5">
			<span
				class={cn(
					'size-2 rounded-full',
					store.online ? 'bg-emerald-500' : 'animate-pulse bg-red-500'
				)}
			></span>
			<span class="text-muted-foreground">
				{store.online ? 'socket up' : 'socket down'}
			</span>
		</span>
		<span class="text-muted-foreground">
			{store.status?.in_flight ?? 0} in flight
		</span>
		{#if store.status}
			<span class="font-mono text-muted-foreground" title={store.status.socket}>
				v{store.status.version} &middot; pid {store.status.pid}
			</span>
		{/if}
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				updated <Elapsed from={store.lastFetched} suffix="ago" />
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

	<div class="mt-3 flex flex-wrap items-center gap-1.5">
		{#each PROCESS_STATUSES as status (status)}
			{@const selected = statuses.includes(status)}
			<button
				type="button"
				onclick={() => toggleStatus(status)}
				aria-pressed={selected}
				class={cn(
					'rounded border px-2 py-0.5 font-mono text-[11px] leading-5',
					selected
						? 'border-primary/50 bg-primary/10 text-foreground'
						: 'border-border text-muted-foreground hover:bg-accent'
				)}
			>
				{status}
			</button>
		{/each}
		<button
			type="button"
			onclick={toggleFinished}
			aria-pressed={showFinished}
			class="ml-2 rounded border border-border px-2 py-0.5 text-[11px] leading-5 text-muted-foreground hover:bg-accent"
		>
			{showFinished ? 'hide finished' : 'show finished'}
		</button>
		{#if thread}
			<button
				type="button"
				onclick={clearThread}
				class="rounded border border-border px-2 py-0.5 font-mono text-[11px] leading-5 text-muted-foreground hover:bg-accent"
				title={`clear thread filter ${thread}`}
			>
				thread {shortId(thread)} &times;
			</button>
		{/if}
	</div>

	<div class="mt-3 overflow-x-auto rounded border border-border bg-card">
		<div class="min-w-[54rem]">
			<div
				class="grid grid-cols-[6rem_6.5rem_9rem_5.5rem_minmax(10rem,1fr)_7rem_5.5rem] items-center gap-2 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase"
			>
				<span>task</span>
				<span>status</span>
				<span>workload</span>
				<span>time</span>
				<span>cwd</span>
				<span>thread</span>
				<span>callback</span>
			</div>

			{#each store.tasks as task (task.id)}
				<div
					class="relative grid grid-cols-[6rem_6.5rem_9rem_5.5rem_minmax(10rem,1fr)_7rem_5.5rem] items-center gap-2 border-b border-border/60 px-3 py-1.5 last:border-b-0 hover:bg-accent/60"
				>
					<a
						href={resolve('/tasks/[id]', { id: task.id })}
						class="font-mono text-primary after:absolute after:inset-0 after:content-['']"
						title={task.id}
					>
						{shortId(task.id)}
					</a>
					<span class="flex items-center gap-1">
						<StatusBadge status={task.status} />
						{#if task.cancel_requested_at}
							<Ban
								class="size-3 text-amber-600 dark:text-amber-400"
								aria-label="cancel requested"
								title={`cancel requested ${formatTimestamp(task.cancel_requested_at)}`}
							/>
						{/if}
					</span>
					<span class="truncate font-mono" title={workloadLabel(task)}>{workloadLabel(task)}</span>
					<Elapsed from={task.created_at} to={rowEnd(task)} class="text-muted-foreground" />
					<span class="truncate font-mono text-muted-foreground" title={task.cwd}>
						{shortenHome(task.cwd)}
					</span>
					<CopyPath
						value={task.thread}
						label={shortId(task.thread)}
						class="relative z-10 text-[11px] text-muted-foreground"
					/>
					<CallbackBadge callback={task.callback} class="justify-self-start" />
				</div>
			{/each}

			{#if store.tasks.length === 0}
				<p class="px-3 py-6 text-center text-muted-foreground">
					{#if store.lastFetched === null}
						{EM_DASH}
					{:else if inFlightOnly}
						No workers in flight
					{:else}
						No tasks match this filter
					{/if}
				</p>
			{/if}
		</div>
	</div>
</div>
