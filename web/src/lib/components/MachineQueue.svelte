<script lang="ts">
	import { resolve } from '$app/paths';
	import GripVertical from '@lucide/svelte/icons/grip-vertical';
	import {
		moveJob,
		peerTaskHref,
		type FleetMachine,
		type JobRecord,
		type MachineQueue,
		type Placement,
		type Priority
	} from '$lib/api';
	import { machineHue } from '$lib/colors';
	import type { QueueControl } from '$lib/daemon.svelte';
	import { EM_DASH, shortId } from '$lib/format';
	import {
		cleanupFailureText,
		dropPlacement,
		groupByLevel,
		phaseLabel,
		phaseSince,
		preemptionLabel,
		resourceNames,
		resourceState,
		stepLabel,
		targetLabel,
		waitingJobs
	} from '$lib/queue-view';
	import { cn } from '$lib/utils';
	import Capsule from './Capsule.svelte';
	import Elapsed from './Elapsed.svelte';
	import JobControls from './JobControls.svelte';
	import LevelBadge, { levelRail } from './LevelBadge.svelte';
	import QueueBadge from './QueueBadge.svelte';
	import ReleaseButton from './ReleaseButton.svelte';

	interface Props {
		machine: FleetMachine;
		/** Missing while the machine does not answer. */
		queue: MachineQueue | undefined;
		/** Name of a job, including an ended one whose run still holds a resource. */
		jobName: (id: string) => string | null;
		control: QueueControl;
	}

	let { machine, queue, jobName, control }: Props = $props();

	const resources = $derived(queue?.resources ?? []);
	const jobs = $derived(queue?.jobs ?? []);
	const groups = $derived(groupByLevel(jobs));
	const names = $derived(resourceNames(resources));
	const jobById = $derived(new Map(jobs.map((job) => [job.id, job])));
	const runByJob = $derived(
		new Map(resources.flatMap((record) => (record.run ? [[record.run.job, record]] : [])))
	);
	const busy = $derived(resources.filter((record) => record.run !== null).length);
	const waiting = $derived(waitingJobs(jobs).length);
	const isLocal = $derived(machine.location.type === 'local');
	const hue = $derived(machineHue(machine.name));

	/** The job being dragged, and where it would land. Drags never leave this machine's queue. */
	let dragging = $state<string | null>(null);
	let dropAt = $state<{ id: string; side: 'before' | 'after' } | { level: Priority } | null>(null);

	function move(job: string, placement: Placement | null) {
		const authority = jobById.get(job)?.machine;
		if (!placement || !authority) return;
		void control.run(job, (operationId) => moveJob(job, authority, operationId, placement));
	}

	function onRowDragOver(event: DragEvent, job: JobRecord) {
		if (!dragging) return;
		event.preventDefault();
		const box = (event.currentTarget as HTMLElement).getBoundingClientRect();
		const side = event.clientY < box.top + box.height / 2 ? 'before' : 'after';
		dropAt = { id: job.id, side };
	}

	function onRowDrop(event: DragEvent, job: JobRecord) {
		event.preventDefault();
		const side = dropAt && 'id' in dropAt ? dropAt.side : 'before';
		if (dragging) move(dragging, dropPlacement(dragging, job, side));
		endDrag();
	}

	function onLevelDrop(event: DragEvent, priority: Priority) {
		event.preventDefault();
		if (dragging) move(dragging, { type: 'edge', priority, end: 'front' });
		endDrag();
	}

	function endDrag() {
		dragging = null;
		dropAt = null;
	}

	function isDropTarget(job: string, side: 'before' | 'after'): boolean {
		return dropAt !== null && 'id' in dropAt && dropAt.id === job && dropAt.side === side;
	}
</script>

<section
	aria-label={`${machine.name} queue`}
	class="overflow-hidden rounded-lg border border-border bg-card"
>
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border px-3 py-2">
		<Capsule {hue} dot title={`Queue on ${machine.name}`}>{machine.name}</Capsule>
		{#if queue}
			<span class="text-muted-foreground tabular-nums">
				{busy} of {resources.length} busy
				<span aria-hidden="true">·</span>
				{waiting} waiting
			</span>
		{/if}
		{#if machine.location.type === 'peer' && machine.location.address}
			<a
				href={`${machine.location.address.replace(/\/+$/, '')}/queue`}
				rel="external"
				class="ml-auto text-[11px] text-muted-foreground hover:text-primary"
			>
				open on {machine.name}
			</a>
		{/if}
	</header>

	{#if machine.read.state === 'unavailable'}
		<p class="flex gap-2 px-3 py-3 text-amber-700 dark:text-amber-300">
			<span class="font-medium">unavailable</span>
			<span class="min-w-0 wrap-anywhere text-muted-foreground">{machine.read.message}</span>
		</p>
	{:else}
		<div
			class="hidden grid-cols-[6rem_4rem_10rem_minmax(0,1fr)_9rem_5rem] gap-x-3 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase md:grid"
			aria-hidden="true"
		>
			<span>Resource</span>
			<span>Device</span>
			<span>State</span>
			<span>Job</span>
			<span>Time</span>
			<span></span>
		</div>
		{#if resources.length === 0}
			<p class="px-3 py-3 text-muted-foreground">No resources</p>
		{/if}
		<ul class="divide-y divide-border/70" aria-label={`${machine.name} resources`}>
			{#each resources as record (record.resource.id)}
				{@const run = record.run}
				{@const name = run ? jobName(run.job) : null}
				{@const since = run ? phaseSince(run.phase) : null}
				{@const taskHref = run && !isLocal ? peerTaskHref(machine, run.task) : null}
				<li
					class="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2 md:grid md:grid-cols-[6rem_4rem_10rem_minmax(0,1fr)_9rem_5rem]"
				>
					<span class="font-mono font-medium">{record.resource.name}</span>
					<span class="font-mono text-muted-foreground">{record.resource.device ?? EM_DASH}</span>
					<span>
						<QueueBadge
							state={resourceState(record)}
							label={run ? phaseLabel(run.phase) : undefined}
						/>
					</span>
					<span class="flex min-w-0 basis-full flex-col md:basis-auto">
						{#if run}
							<span class="flex min-w-0 items-baseline gap-2">
								<a
									href={resolve(
										`/queue/jobs/${encodeURIComponent(run.job)}?machine=${machine.machine}`
									)}
									class="truncate font-medium hover:text-primary"
									title={name ?? run.job}
								>
									{name ?? shortId(run.job)}
								</a>
								{#if isLocal}
									<a
										href={resolve('/tasks/[id]', { id: run.task })}
										class="shrink-0 font-mono text-[11px] text-muted-foreground hover:text-primary"
										title={`Run ${run.run_number} task ${run.task}`}
									>
										run {run.run_number}
									</a>
								{:else if taskHref}
									<a
										href={taskHref}
										rel="external"
										class="shrink-0 font-mono text-[11px] text-muted-foreground hover:text-primary"
										title={`Run ${run.run_number} task ${run.task} on ${machine.name}`}
									>
										run {run.run_number}
									</a>
								{:else}
									<span class="shrink-0 font-mono text-[11px] text-muted-foreground">
										run {run.run_number}
									</span>
								{/if}
							</span>
							{#if run.phase.phase === 'attention'}
								<span class="text-[11px] text-red-700 dark:text-red-300">
									{cleanupFailureText(run.phase.failure)}
								</span>
							{/if}
						{:else}
							<span class="text-muted-foreground">{EM_DASH}</span>
						{/if}
					</span>
					<span class="text-muted-foreground">
						{#if run?.phase.phase === 'stopping'}
							asked <Elapsed from={run.phase.requested_at} suffix="ago" />
						{:else if since}
							<Elapsed from={since} />
						{/if}
					</span>
					<span class="flex justify-end">
						{#if run?.phase.phase === 'attention'}
							<ReleaseButton
								attention={run.phase.id}
								machine={record.machine}
								resourceName={record.resource.name}
								{control}
							/>
						{/if}
					</span>
				</li>
			{/each}
		</ul>

		{#if jobs.length === 0}
			<p class="border-t border-border px-3 py-2 text-muted-foreground">No jobs in the queue</p>
		{:else}
			<div
				class="hidden grid-cols-[1rem_1.5rem_minmax(0,1fr)_5rem_10rem_11rem_4.5rem_5.5rem] gap-x-3 border-y border-border bg-muted px-3 py-1.5 pl-4 text-[11px] tracking-wide text-muted-foreground uppercase lg:grid"
				aria-hidden="true"
			>
				<span></span>
				<span>#</span>
				<span>Job</span>
				<span>Target</span>
				<span>Preempt</span>
				<span>State</span>
				<span>Steps</span>
				<span></span>
			</div>
		{/if}
		{#each jobs.length > 0 ? groups : [] as group (group.priority)}
			<div
				role="group"
				aria-label={`${machine.name} ${group.priority} level`}
				class={cn(
					'border-b border-border/70 last:border-b-0',
					dropAt && 'level' in dropAt && dropAt.level === group.priority && 'bg-accent/60'
				)}
			>
				<div
					class="flex items-center gap-2 bg-muted/50 px-3 py-1 pl-4"
					role="presentation"
					ondragover={(event) => {
						if (!dragging) return;
						event.preventDefault();
						dropAt = { level: group.priority };
					}}
					ondrop={(event) => onLevelDrop(event, group.priority)}
				>
					<LevelBadge priority={group.priority} />
					<span class="text-[11px] text-muted-foreground tabular-nums">{group.jobs.length}</span>
				</div>
				<ol>
					{#each group.jobs as job (job.id)}
						{@const holder = runByJob.get(job.id)}
						<li
							draggable="true"
							ondragstart={(event) => {
								dragging = job.id;
								event.dataTransfer?.setData('text/plain', job.id);
							}}
							ondragend={endDrag}
							ondragover={(event) => onRowDragOver(event, job)}
							ondrop={(event) => onRowDrop(event, job)}
							class={cn(
								'relative flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-border/50 px-3 py-1.5 pl-4 hover:bg-accent/40 lg:grid lg:grid-cols-[1rem_1.5rem_minmax(0,1fr)_5rem_10rem_11rem_4.5rem_5.5rem]',
								dragging === job.id && 'opacity-40',
								isDropTarget(job.id, 'before') && 'shadow-[inset_0_2px_0_var(--primary)]',
								isDropTarget(job.id, 'after') && 'shadow-[inset_0_-2px_0_var(--primary)]'
							)}
						>
							<span
								class={cn(
									'absolute inset-y-0 left-0 w-[3px]',
									levelRail({ priority: job.priority })
								)}
								aria-hidden="true"
							></span>
							<GripVertical
								class="hidden size-3.5 cursor-grab text-muted-foreground lg:block"
								aria-hidden="true"
							/>
							<span class="font-mono text-[11px] text-muted-foreground tabular-nums">
								{job.position}
							</span>
							<a
								href={resolve(
									`/queue/jobs/${encodeURIComponent(job.id)}?machine=${machine.machine}`
								)}
								class="min-w-0 flex-1 basis-3/5 truncate font-medium hover:text-primary lg:basis-auto"
								title={job.spec.name}
								draggable="false"
							>
								{job.spec.name}
							</a>
							<span class="font-mono text-[11px]">{targetLabel(job.target, names)}</span>
							<span class="font-mono text-[11px] text-muted-foreground">
								{preemptionLabel(job.spec.preempt)}
							</span>
							<span class="flex min-w-0 items-center gap-1.5">
								{#if holder?.run}
									<QueueBadge state={resourceState(holder)} label={phaseLabel(holder.run.phase)} />
									<span class="font-mono text-[11px] text-muted-foreground">
										{holder.resource.name}
									</span>
								{:else}
									<QueueBadge state={job.state.state} />
									{#if job.state.state === 'queued' && job.state.resume}
										<span class="text-[11px] text-muted-foreground">resumes</span>
									{/if}
								{/if}
							</span>
							<span class="font-mono text-[11px] text-muted-foreground">{stepLabel(job)}</span>
							<JobControls {job} {control} {groups} class="ml-auto justify-end" />
						</li>
					{/each}
				</ol>
			</div>
		{/each}
	{/if}
</section>
