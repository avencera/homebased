<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import TriangleAlert from '@lucide/svelte/icons/triangle-alert';
	import { machineHue } from '$lib/colors';
	import {
		IN_FLIGHT_STATUSES,
		PROCESS_STATUSES,
		isInFlight,
		isProcessStatus,
		taskThread,
		type ProcessStatus
	} from '$lib/api';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import ExpandToggle from '$lib/components/ExpandToggle.svelte';
	import ResourceQueuePanel from '$lib/components/ResourceQueuePanel.svelte';
	import Capsule from '$lib/components/Capsule.svelte';
	import TaskList from '$lib/components/TaskList.svelte';
	import { DaemonStore, ResourceQueueStore } from '$lib/daemon.svelte';
	import { isBusy, queueThreads } from '$lib/resource-state';
	import { ThreadTitleStore } from '$lib/thread-titles.svelte';
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
	const threadTitles = new ThreadTitleStore(() => [
		...visibleTasks.map(taskThread),
		...busyQueues.flatMap(queueThreads)
	]);
	// the filter holds only a thread id, so its title comes from a listed task
	const threadFilterTitle = $derived.by(() => {
		const entry = thread ? store.tasks.find((candidate) => candidate.task.thread === thread) : null;
		return entry ? threadTitles.title(taskThread(entry)) : null;
	});
	// while a resource runs or waits on work, tasks and its queue share the screen
	const split = $derived(busyQueues.length > 0);

	/** Box that fills the whole content area, hiding the other one. */
	type ExpandedBox = 'tasks' | 'gpu';
	let expandedChoice = $state<ExpandedBox | null>(null);
	// only a split can expand; when the GPU work ends, the tasks fill the screen anyway
	const expanded = $derived(split ? expandedChoice : null);

	function toggleExpanded(box: ExpandedBox) {
		expandedChoice = expanded === box ? null : box;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && expanded) expandedChoice = null;
	}

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

	async function refreshDashboard(): Promise<void> {
		await Promise.all([store.refresh(), resourceStore.refresh()]);
	}
</script>

<svelte:window onkeydown={onKeydown} />

{#snippet tasksExpand()}
	<ExpandToggle
		expanded={expanded === 'tasks'}
		label="tasks"
		onToggle={() => toggleExpanded('tasks')}
	/>
{/snippet}

<!-- pinned to the viewport so the document never scrolls; only the boxes do. The padding keeps
     at least the safe-area insets the browser reports, so the boxes end above the home indicator
     and any browser bar that claims the edge -->
<div
	class="fixed inset-0 mx-auto flex max-w-7xl flex-col overflow-hidden pt-[max(0.75rem,env(safe-area-inset-top))] pr-[max(0.75rem,env(safe-area-inset-right))] pb-[max(0.75rem,env(safe-area-inset-bottom))] pl-[max(0.75rem,env(safe-area-inset-left))] sm:p-4"
>
	<header class="flex flex-wrap items-baseline gap-x-3 gap-y-1 sm:gap-x-4">
		<h1 class="text-base font-semibold tracking-tight">homebased</h1>
		<span class="flex items-center gap-1.5" title={store.online ? 'socket up' : 'socket down'}>
			<span
				class={cn(
					'size-2 rounded-full',
					store.online ? 'bg-emerald-500' : 'animate-pulse bg-red-500'
				)}
			></span>
			<span class="hidden text-muted-foreground sm:inline">
				{store.online ? 'socket up' : 'socket down'}
			</span>
		</span>
		<span class="text-muted-foreground">
			{store.status?.in_flight ?? 0} in flight{#if localName}<span class="hidden sm:inline"
					>{` on ${localName}`}</span
				>{/if}
		</span>
		{#if store.status}
			<span class="hidden font-mono text-muted-foreground sm:inline" title={store.status.socket}>
				v{store.status.version} &middot; pid {store.status.pid}
			</span>
		{/if}
		{#if store.machines.length > 0}
			<span
				class="flex max-w-full flex-wrap items-center gap-x-2 gap-y-1"
				aria-label="Fleet machine versions"
			>
				{#each store.machines as machine (machine.machine)}
					{@const offline = machine.read.state === 'unavailable'}
					{@const mismatch = store.status !== null && machine.version !== store.status.version}
					<span class={cn('inline-flex items-center gap-1', offline && 'opacity-50')}>
						<Capsule
							hue={machineHue(machine.name)}
							dot
							title={offline ? `${machine.name} is offline` : machine.name}
						>
							{machine.name}
						</Capsule>
						<span
							class={cn(
								'font-mono text-[11px]',
								mismatch ? 'text-amber-700 dark:text-amber-300' : 'text-muted-foreground'
							)}
							title={mismatch
								? `Runs a different Homebased version than this dashboard (v${store.status?.version})`
								: `Homebased v${machine.version}`}
						>
							v{machine.version}
						</span>
					</span>
				{/each}
			</span>
		{/if}
		<a href={resolve('/resources')} class="text-primary hover:underline">resources</a>
		<a href={resolve('/files')} class="text-primary hover:underline">files</a>
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				<span class="hidden sm:inline">updated</span>
				<Elapsed from={store.lastFetched} suffix="ago" />
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

	<!-- one swipeable row on phones, wrapped on wider screens -->
	<div
		class="-mx-3 mt-3 scrollbar-none flex shrink-0 items-center gap-1.5 overflow-x-auto px-3 sm:mx-0 sm:flex-wrap sm:px-0 [&>*]:shrink-0"
	>
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
				title={`clear thread filter ${threadFilterTitle ? `${threadFilterTitle} · ` : ''}${thread}`}
			>
				thread {threadFilterTitle ?? shortId(thread)} &times;
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

	<!-- the page fits the screen and each box scrolls on its own. Each box is as tall as its
	     content; when both overflow, grid sizing shares the height evenly, and a short box keeps
	     its content height while the other takes the rest. An expanded box takes the whole area -->
	<div
		class={cn(
			'mt-3 grid min-h-0 flex-1 content-start gap-3',
			expanded ? 'grid-rows-[minmax(0,1fr)]' : 'grid-rows-[minmax(0,auto)_minmax(0,auto)]'
		)}
	>
		{#if expanded !== 'gpu'}
			<TaskList
				tasks={visibleTasks}
				machines={store.machines}
				activeThread={thread}
				activeProject={project}
				onThread={(next) => navigate({ ...currentFilters, thread: next })}
				onProject={(next) => navigate({ ...currentFilters, project: next })}
				threadTitle={threadTitles.title}
				class="min-h-0"
				actions={split ? tasksExpand : undefined}
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
		{/if}
		{#if split && expanded !== 'tasks'}
			<ResourceQueuePanel
				queues={busyQueues}
				machines={store.machines}
				{resourceStore}
				{refreshDashboard}
				threadTitle={threadTitles.title}
				class="scrollbar-none min-h-0 overflow-y-auto"
			>
				{#snippet actions()}
					<ExpandToggle
						expanded={expanded === 'gpu'}
						label="GPU queue"
						onToggle={() => toggleExpanded('gpu')}
					/>
				{/snippet}
			</ResourceQueuePanel>
		{/if}
	</div>
</div>
