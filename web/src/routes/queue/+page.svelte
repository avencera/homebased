<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import GripVertical from '@lucide/svelte/icons/grip-vertical';
	import X from '@lucide/svelte/icons/x';
	import { moveJob, type JobRecord, type Placement, type Priority } from '$lib/api';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import JobControls from '$lib/components/JobControls.svelte';
	import LevelBadge, { levelRail } from '$lib/components/LevelBadge.svelte';
	import QueueBadge from '$lib/components/QueueBadge.svelte';
	import ReleaseButton from '$lib/components/ReleaseButton.svelte';
	import { QueueControl, QueueStore } from '$lib/daemon.svelte';
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
		targetLabel
	} from '$lib/queue-view';
	import { cn } from '$lib/utils';

	const store = new QueueStore();
	const control = new QueueControl(() => store.refresh());

	const groups = $derived(groupByLevel(store.jobs));
	const names = $derived(resourceNames(store.resources));
	const jobById = $derived(new Map(store.jobs.map((job) => [job.id, job])));
	const runByJob = $derived(
		new Map(store.resources.flatMap((record) => (record.run ? [[record.run.job, record]] : [])))
	);

	/** The job being dragged, and where it would land. */
	let dragging = $state<string | null>(null);
	let dropAt = $state<{ id: string; side: 'before' | 'after' } | { level: Priority } | null>(null);

	function move(job: string, placement: Placement | null) {
		const machine = jobById.get(job)?.machine;
		if (!placement || !machine) return;
		void control.run(job, (operationId) => moveJob(job, machine, operationId, placement));
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

<div class="mx-auto max-w-6xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			all workers
		</a>
		<h1 class="text-base font-semibold tracking-tight">GPU queue</h1>
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				updated <Elapsed from={store.lastFetched} suffix="ago" />
			{:else}
				loading
			{/if}
		</span>
	</header>

	{#each [store.error, control.error] as error, index (index)}
		{#if error}
			<p
				class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
				role="alert"
			>
				<CircleAlert class="mt-0.5 size-4 shrink-0" />
				<span class="min-w-0 flex-1 wrap-anywhere">{error.message}</span>
				{#if index === 1}
					<button
						type="button"
						class="shrink-0 rounded p-0.5 hover:bg-red-500/10"
						aria-label="Dismiss"
						onclick={() => control.dismiss()}
					>
						<X class="size-3.5" aria-hidden="true" />
					</button>
				{/if}
			</p>
		{/if}
	{/each}

	<section
		aria-label="Resources"
		class="mt-3 overflow-hidden rounded-lg border border-border bg-card"
	>
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
		<ul class="divide-y divide-border/70">
			{#each store.resources as record (record.resource.id)}
				{@const run = record.run}
				{@const name = run ? store.jobName(run.job) : null}
				{@const since = run ? phaseSince(run.phase) : null}
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
									href={resolve('/queue/jobs/[id]', { id: run.job })}
									class="truncate font-medium hover:text-primary"
									title={name ?? run.job}
								>
									{name ?? shortId(run.job)}
								</a>
								<a
									href={resolve('/tasks/[id]', { id: run.task })}
									class="shrink-0 font-mono text-[11px] text-muted-foreground hover:text-primary"
									title={`Run ${run.run_number} task ${run.task}`}
								>
									run {run.run_number}
								</a>
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
	</section>

	<section aria-label="Queue" class="mt-3 overflow-hidden rounded-lg border border-border bg-card">
		<div
			class="hidden grid-cols-[1rem_1.5rem_minmax(0,1fr)_5rem_10rem_11rem_4.5rem_5.5rem] gap-x-3 border-b border-border bg-muted px-3 py-1.5 pl-4 text-[11px] tracking-wide text-muted-foreground uppercase lg:grid"
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
		{#if store.lastFetched !== null && store.jobs.length === 0}
			<p class="px-3 py-6 text-center text-muted-foreground">No jobs in the queue</p>
		{/if}
		{#each store.jobs.length > 0 ? groups : [] as group (group.priority)}
			<div
				role="group"
				aria-label={`${group.priority} level`}
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
								href={resolve('/queue/jobs/[id]', { id: job.id })}
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
	</section>
</div>
