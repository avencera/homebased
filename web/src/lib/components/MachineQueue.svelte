<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	/** Panel tint of a held resource, in the queue badge palette for its run phase. */
	const runPanel = tv({
		variants: {
			phase: {
				launching: 'border-sky-500/30 bg-sky-500/5',
				executing: 'border-sky-500/40 bg-sky-500/8',
				stopping: 'border-amber-500/40 bg-amber-500/8',
				cleaning: 'border-slate-500/30 bg-slate-500/6',
				attention: 'border-red-500/50 bg-red-500/10'
			}
		}
	});

	/** Left accent of a held resource panel. */
	const runAccent = tv({
		variants: {
			phase: {
				launching: 'bg-sky-400',
				executing: 'bg-sky-500',
				stopping: 'bg-amber-500',
				cleaning: 'bg-slate-400',
				attention: 'bg-red-500'
			}
		}
	});
</script>

<script lang="ts">
	import { resolve } from '$app/paths';
	import GripVertical from '@lucide/svelte/icons/grip-vertical';
	import {
		moveJob,
		peerQueueHref,
		peerTaskHref,
		type FleetMachine,
		type JobRecord,
		type MachineQueue,
		type Placement,
		type Priority
	} from '$lib/api';
	import { machineHue } from '$lib/colors';
	import type { QueueControl } from '$lib/daemon.svelte';
	import { shortId } from '$lib/format';
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
		targetLabel
	} from '$lib/queue-view';
	import { cn } from '$lib/utils';
	import Elapsed from './Elapsed.svelte';
	import JobControls from './JobControls.svelte';
	import LevelBadge, { levelRail, levelTint } from './LevelBadge.svelte';
	import QueueBadge from './QueueBadge.svelte';
	import ReleaseButton from './ReleaseButton.svelte';

	interface Props {
		machine: FleetMachine;
		queue: MachineQueue;
		/** Name of a job, including an ended one whose run still holds a resource. */
		jobName: (id: string) => string | null;
		control: QueueControl;
	}

	let { machine, queue, jobName, control }: Props = $props();

	const jobById = $derived(new Map(queue.jobs.map((job) => [job.id, job])));
	// a running job is shown on the resource that holds it, so the list below is only what waits
	const waiting = $derived(queue.jobs.filter((job) => job.state.state === 'queued'));
	const groups = $derived(groupByLevel(waiting));
	const names = $derived(resourceNames(queue.resources));
	const held = $derived(queue.resources.filter((record) => record.run !== null));
	const idle = $derived(queue.resources.filter((record) => record.run === null));
	const isLocal = $derived(machine.location.type === 'local');
	const queueHref = $derived(peerQueueHref(machine));

	/** The job being dragged, and where it would land. Drags never leave this machine's queue. */
	let dragging = $state<string | null>(null);
	let dropAt = $state<{ id: string; side: 'before' | 'after' } | { level: Priority } | null>(null);

	// empty levels only matter as drop targets, so they show while a job is dragged
	const shownGroups = $derived(groups.filter((group) => dragging || group.jobs.length > 0));

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
	class="overflow-hidden rounded-lg border border-t-[3px] border-border border-t-[oklch(0.62_0.13_var(--hue))] bg-card shadow-sm"
	style:--hue={machineHue(machine.name)}
>
	<header class="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-3 pt-2.5 pb-2">
		<h2 class="text-sm font-semibold tracking-tight">{machine.name}</h2>
		<span class="text-muted-foreground tabular-nums">
			{held.length} of {queue.resources.length} busy
			<span aria-hidden="true">·</span>
			{waiting.length} waiting
		</span>
		{#if queueHref}
			<a
				href={queueHref}
				rel="external"
				class="ml-auto text-[11px] text-muted-foreground hover:text-primary"
			>
				open on {machine.name}
			</a>
		{/if}
	</header>

	<div class="flex flex-col gap-2.5 px-3 pb-3" aria-label={`${machine.name} running`}>
		{#each held as record (record.resource.id)}
			{@const run = record.run}
			{#if run}
				{@const job = jobById.get(run.job)}
				{@const name = jobName(run.job)}
				{@const since = phaseSince(run.phase)}
				{@const taskHref = isLocal ? null : peerTaskHref(machine, run.task)}
				<article
					class={cn(
						'relative overflow-hidden rounded-md border py-3 pr-3 pl-5',
						runPanel({ phase: run.phase.phase })
					)}
				>
					<span
						class={cn('absolute inset-y-0 left-0 w-1', runAccent({ phase: run.phase.phase }))}
						aria-hidden="true"
					></span>
					<div class="flex flex-wrap items-center gap-x-3 gap-y-1">
						<span class="font-mono font-medium">{record.resource.name}</span>
						{#if record.resource.device !== null}
							<span class="font-mono text-[11px] text-muted-foreground">
								device {record.resource.device}
							</span>
						{/if}
						<QueueBadge state={resourceState(record)} label={phaseLabel(run.phase)} />
						<span class="ml-auto flex items-center gap-3">
							{#if run.phase.phase === 'stopping'}
								<span class="text-muted-foreground">
									asked <Elapsed from={run.phase.requested_at} suffix="ago" />
								</span>
							{:else if since}
								<Elapsed
									from={since}
									class="text-base font-semibold text-foreground tabular-nums"
								/>
							{/if}
							{#if run.phase.phase === 'attention'}
								<ReleaseButton
									attention={run.phase.id}
									machine={record.machine}
									resourceName={record.resource.name}
									{control}
								/>
							{:else if job}
								<JobControls {job} {control} />
							{/if}
						</span>
					</div>
					<a
						href={resolve(`/queue/jobs/${encodeURIComponent(run.job)}?machine=${machine.machine}`)}
						class="mt-1 block truncate text-base font-semibold tracking-tight text-foreground hover:text-primary"
						title={name ?? run.job}
					>
						{name ?? shortId(run.job)}
					</a>
					<div class="mt-0.5 flex flex-wrap items-center gap-x-2 text-[11px] text-muted-foreground">
						{#if job}
							<LevelBadge priority={job.priority} class="tracking-wide uppercase" />
							<span aria-hidden="true">·</span>
						{/if}
						{#if isLocal}
							<a
								href={resolve('/tasks/[id]', { id: run.task })}
								class="font-mono hover:text-primary"
								title={`Run ${run.run_number} task ${run.task}`}
							>
								run {run.run_number}
							</a>
						{:else if taskHref}
							<a
								href={taskHref}
								rel="external"
								class="font-mono hover:text-primary"
								title={`Run ${run.run_number} task ${run.task} on ${machine.name}`}
							>
								run {run.run_number}
							</a>
						{:else}
							<span class="font-mono">run {run.run_number}</span>
						{/if}
						{#if job}
							<span aria-hidden="true">·</span>
							<span class="font-mono">{stepLabel(job)}</span>
							<span aria-hidden="true">·</span>
							<span class="font-mono">preempt {preemptionLabel(job.spec.preempt)}</span>
						{/if}
					</div>
					{#if run.phase.phase === 'attention'}
						<p class="mt-1 text-[11px] text-red-700 dark:text-red-300">
							{cleanupFailureText(run.phase.failure)}
						</p>
					{/if}
				</article>
			{/if}
		{/each}
		{#if idle.length > 0}
			<p class="flex flex-wrap items-center gap-x-3 gap-y-1 px-1 text-muted-foreground">
				{#each idle as record (record.resource.id)}
					<span class="inline-flex items-center gap-1.5">
						<span class="font-mono">{record.resource.name}</span>
						<QueueBadge state="idle" />
					</span>
				{/each}
			</p>
		{/if}
	</div>

	{#if waiting.length > 0}
		<div class="flex flex-col gap-2.5 border-t border-border bg-muted/40 px-3 pt-2 pb-3">
			<div
				class="mx-px flex items-center gap-2 px-3 text-[11px] tracking-wide text-muted-foreground uppercase lg:grid lg:grid-cols-[1rem_1.5rem_minmax(0,1fr)_5rem_10rem_4.5rem_5.5rem] lg:gap-x-3 lg:pl-4"
			>
				<span class="hidden lg:block" aria-hidden="true"></span>
				<span class="hidden lg:block" aria-hidden="true">#</span>
				<span>Waiting <span class="tabular-nums">{waiting.length}</span></span>
				<span class="hidden lg:block" aria-hidden="true">Target</span>
				<span class="hidden lg:block" aria-hidden="true">Preempt</span>
				<span class="hidden lg:block" aria-hidden="true">Steps</span>
				<span class="hidden lg:block" aria-hidden="true"></span>
			</div>
			{#each shownGroups as group (group.priority)}
				<div
					role="group"
					aria-label={`${machine.name} ${group.priority} level`}
					class={cn(
						'overflow-hidden rounded-md border border-border bg-card shadow-xs',
						dropAt &&
							'level' in dropAt &&
							dropAt.level === group.priority &&
							'ring-2 ring-primary/50'
					)}
				>
					<div
						class={cn(
							'flex items-center gap-2 border-b px-3 py-1.5 pl-4',
							levelTint({ priority: group.priority })
						)}
						role="presentation"
						ondragover={(event) => {
							if (!dragging) return;
							event.preventDefault();
							dropAt = { level: group.priority };
						}}
						ondrop={(event) => onLevelDrop(event, group.priority)}
					>
						<LevelBadge priority={group.priority} class="tracking-wide uppercase" />
						<span class="text-[11px] text-muted-foreground tabular-nums">
							{group.jobs.length} waiting
						</span>
					</div>
					<ol>
						{#each group.jobs as job, index (job.id)}
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
									'relative flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-border/50 px-3 py-1.5 pl-4 first:border-t-0 hover:bg-accent/40 lg:grid lg:grid-cols-[1rem_1.5rem_minmax(0,1fr)_5rem_10rem_4.5rem_5.5rem]',
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
									{index + 1}
								</span>
								<span class="flex min-w-0 flex-1 basis-3/5 items-baseline gap-2 lg:basis-auto">
									<a
										href={resolve(
											`/queue/jobs/${encodeURIComponent(job.id)}?machine=${machine.machine}`
										)}
										class="truncate font-medium hover:text-primary"
										title={job.spec.name}
										draggable="false"
									>
										{job.spec.name}
									</a>
									{#if job.state.state === 'queued' && job.state.resume}
										<span class="shrink-0 text-[11px] text-muted-foreground">resumes</span>
									{/if}
								</span>
								<span class="font-mono text-[11px]">{targetLabel(job.target, names)}</span>
								<span class="font-mono text-[11px] text-muted-foreground">
									{preemptionLabel(job.spec.preempt)}
								</span>
								<span class="font-mono text-[11px] text-muted-foreground">{stepLabel(job)}</span>
								<JobControls {job} {control} {groups} class="ml-auto justify-end" />
							</li>
						{/each}
					</ol>
				</div>
			{/each}
		</div>
	{/if}
</section>
