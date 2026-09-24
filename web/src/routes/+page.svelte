<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import Ban from '@lucide/svelte/icons/ban';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import TriangleAlert from '@lucide/svelte/icons/triangle-alert';
	import {
		IN_FLIGHT_STATUSES,
		PROCESS_STATUSES,
		isInFlight,
		isProcessStatus,
		peerTaskHref,
		type ProcessStatus,
		type TaskSummary
	} from '$lib/api';
	import CallbackBadge from '$lib/components/CallbackBadge.svelte';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import ResourceQueuePanel from '$lib/components/ResourceQueuePanel.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import WorkloadCell from '$lib/components/WorkloadCell.svelte';
	import { DaemonStore, ResourceQueueStore } from '$lib/daemon.svelte';
	import { isBusy } from '$lib/resource-state';
	import { EM_DASH, formatTimestamp, shortId, shortenHome } from '$lib/format';
	import { cn } from '$lib/utils';

	const statuses = $derived(parseStatuses(page.url.searchParams.get('status')));
	const thread = $derived(page.url.searchParams.get('thread'));
	const showFinished = $derived(statuses.some((status) => !isInFlight(status)));
	const inFlightOnly = $derived(!showFinished && statuses.length === IN_FLIGHT_STATUSES.length);

	const store = new DaemonStore(() => ({ statuses, thread }));
	const resourceStore = new ResourceQueueStore();

	const machineById = $derived(
		new Map(store.machines.map((machine) => [machine.machine, machine]))
	);
	const unavailableMachines = $derived(
		store.machines.flatMap((machine) =>
			machine.read.state === 'unavailable'
				? [{ name: machine.name, message: machine.read.message }]
				: []
		)
	);
	const localName = $derived(
		store.machines.find((machine) => machine.location.type === 'local')?.name ?? null
	);
	const busyQueues = $derived(resourceStore.queues.filter(isBusy));
	// the queue panel takes the right column only while a resource runs or waits on work
	const split = $derived(busyQueues.length > 0);

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

	function isLocal(machine: string): boolean {
		return machineById.get(machine)?.location.type === 'local';
	}

	function machineName(id: string): string {
		return machineById.get(id)?.name ?? shortId(id);
	}

	function machineList(names: readonly string[]): string {
		return names.join(', ');
	}
</script>

<div class={cn('mx-auto px-4 py-4', split ? 'max-w-[112rem]' : 'max-w-7xl')}>
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
			{store.status?.in_flight ?? 0} in flight{localName ? ` on ${localName}` : ''}
		</span>
		{#if store.status}
			<span class="font-mono text-muted-foreground" title={store.status.socket}>
				v{store.status.version} &middot; pid {store.status.pid}
			</span>
		{/if}
		<a href={resolve('/resources')} class="text-primary hover:underline">resources</a>
		<a href={resolve('/files')} class="text-primary hover:underline">files</a>
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

	{#if unavailableMachines.length > 0}
		<div
			class="mt-3 flex items-start gap-2 rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2"
			role="status"
		>
			<TriangleAlert class="mt-0.5 size-4 shrink-0 text-amber-600 dark:text-amber-400" />
			<div class="min-w-0">
				<p>
					{machineList(unavailableMachines.map((machine) => machine.name))}
					{unavailableMachines.length === 1 ? 'is' : 'are'} offline. Tasks from the other machines are
					still shown.
				</p>
				<ul class="text-muted-foreground">
					{#each unavailableMachines as machine (machine.name)}
						<li><span class="font-medium">{machine.name}</span>: {machine.message}</li>
					{/each}
				</ul>
			</div>
		</div>
	{/if}

	{#if resourceStore.error}
		<p class="mt-3 text-muted-foreground">
			GPU queue unavailable: {resourceStore.error.message}
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

	<div class={cn('mt-3 grid gap-3', split && 'xl:grid-cols-[minmax(0,1fr)_24rem]')}>
		{#if split}
			<ResourceQueuePanel queues={busyQueues} machines={store.machines} class="xl:order-last" />
		{/if}
		<div class="min-w-0 overflow-x-auto rounded border border-border bg-card">
			<div class="min-w-[76rem]">
				<div
					class="grid grid-cols-[5.5rem_minmax(10rem,1.4fr)_minmax(9rem,0.8fr)_5.5rem_6.5rem_5.5rem_minmax(10rem,1fr)_7rem_5.5rem] items-center gap-2 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase"
				>
					<span>machine</span>
					<span>name</span>
					<span>agent</span>
					<span>id</span>
					<span>status</span>
					<span>time</span>
					<span>cwd</span>
					<span>thread</span>
					<span>callback</span>
				</div>

				{#each store.tasks as entry (entry.task.id)}
					{@const task = entry.task}
					{@const peerHref = peerTaskHref(machineById.get(entry.machine), entry.task.id)}
					<div
						class="relative grid grid-cols-[5.5rem_minmax(10rem,1.4fr)_minmax(9rem,0.8fr)_5.5rem_6.5rem_5.5rem_minmax(10rem,1fr)_7rem_5.5rem] items-center gap-2 border-b border-border/60 px-3 py-1.5 last:border-b-0 hover:bg-accent/60"
					>
						<span
							class={cn(
								'truncate font-mono text-[11px]',
								isLocal(entry.machine) ? 'text-muted-foreground' : 'text-foreground'
							)}
							title={`runs on ${machineName(entry.machine)}`}
						>
							{machineName(entry.machine)}
						</span>
						{#if isLocal(entry.machine)}
							<a
								href={resolve('/tasks/[id]', { id: task.id })}
								class="truncate font-mono text-primary after:absolute after:inset-0 after:content-['']"
								title={task.display_name}
							>
								{task.display_name}
							</a>
						{:else if peerHref}
							<a
								href={peerHref}
								rel="external"
								class="truncate font-mono text-primary after:absolute after:inset-0 after:content-['']"
								title={`${task.display_name} on ${machineName(entry.machine)}`}
							>
								{task.display_name}
							</a>
						{:else}
							<span class="truncate font-mono" title={task.display_name}>{task.display_name}</span>
						{/if}
						<!-- raised above the row link overlay so the full workload title shows on hover -->
						<WorkloadCell workload={task.workload} class="relative z-10" />
						<span class="font-mono text-muted-foreground" title={task.id}>{shortId(task.id)}</span>
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
</div>
