<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import TriangleAlert from '@lucide/svelte/icons/triangle-alert';
	import {
		IN_FLIGHT_STATUSES,
		PROCESS_STATUSES,
		isInFlight,
		isProcessStatus,
		type ProcessStatus
	} from '$lib/api';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import ResourceQueuePanel from '$lib/components/ResourceQueuePanel.svelte';
	import TaskList from '$lib/components/TaskList.svelte';
	import { DaemonStore, ResourceQueueStore } from '$lib/daemon.svelte';
	import { isBusy } from '$lib/resource-state';
	import { EM_DASH, projectName, shortId } from '$lib/format';
	import { cn } from '$lib/utils';

	const statuses = $derived(parseStatuses(page.url.searchParams.get('status')));
	const thread = $derived(page.url.searchParams.get('thread'));
	// project is a view filter over the fleet list; the daemon has no project query
	const project = $derived(page.url.searchParams.get('project'));
	const showFinished = $derived(statuses.some((status) => !isInFlight(status)));
	const inFlightOnly = $derived(!showFinished && statuses.length === IN_FLIGHT_STATUSES.length);

	const store = new DaemonStore(() => ({ statuses, thread }));
	const resourceStore = new ResourceQueueStore();

	const visibleTasks = $derived(
		project ? store.tasks.filter((entry) => projectName(entry.task) === project) : store.tasks
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
	// while a resource runs or waits on work, tasks and its queue share the screen
	const split = $derived(busyQueues.length > 0);

	interface Filters {
		status: string | null;
		thread: string | null;
		project: string | null;
	}

	const currentFilters = $derived<Filters>({
		status: page.url.searchParams.get('status'),
		thread,
		project
	});

	function parseStatuses(raw: string | null): ProcessStatus[] {
		const parsed = (raw ?? '')
			.split(',')
			.map((part) => part.trim())
			.filter(isProcessStatus);
		return parsed.length > 0 ? parsed : [...IN_FLIGHT_STATUSES];
	}

	// The whole filter lives in the URL so a view can be bookmarked or shared
	function navigate(filters: Filters) {
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
		navigate({ ...currentFilters, status });
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

	function machineList(names: readonly string[]): string {
		return names.join(', ');
	}
</script>

<div class="mx-auto flex h-dvh max-w-7xl flex-col overflow-hidden px-4 py-4">
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
				onclick={() => navigate({ ...currentFilters, thread: null })}
				class="rounded border border-primary/50 bg-primary/10 px-2 py-0.5 font-mono text-[11px] leading-5 hover:bg-accent"
				title={`clear thread filter ${thread}`}
			>
				thread {shortId(thread)} &times;
			</button>
		{/if}
		{#if project}
			<button
				type="button"
				onclick={() => navigate({ ...currentFilters, project: null })}
				class="rounded border border-primary/50 bg-primary/10 px-2 py-0.5 font-mono text-[11px] leading-5 hover:bg-accent"
				title={`clear project filter ${project}`}
			>
				project {project} &times;
			</button>
		{/if}
	</div>

	<!-- the page fits the screen; each box scrolls on its own, and with GPU work tasks and the queue split it evenly -->
	<div class="mt-3 flex min-h-0 flex-1 flex-col gap-3">
		<TaskList
			tasks={visibleTasks}
			machines={store.machines}
			activeThread={thread}
			activeProject={project}
			onThread={(next) => navigate({ ...currentFilters, thread: next })}
			onProject={(next) => navigate({ ...currentFilters, project: next })}
			class="min-h-0 flex-1 basis-0"
		>
			{#snippet empty()}
				<p class="px-3 py-6 text-center text-muted-foreground">
					{#if store.lastFetched === null}
						{EM_DASH}
					{:else if inFlightOnly && !project}
						No workers in flight
					{:else}
						No tasks match this filter
					{/if}
				</p>
			{/snippet}
		</TaskList>
		{#if split}
			<ResourceQueuePanel
				queues={busyQueues}
				machines={store.machines}
				class="scrollbar-none min-h-0 flex-1 basis-0 overflow-y-auto"
			/>
		{/if}
	</div>
</div>
