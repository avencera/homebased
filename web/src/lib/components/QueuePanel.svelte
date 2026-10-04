<script lang="ts">
	import { resolve } from '$app/paths';
	import type { JobRecord, ResourceRecord } from '$lib/api';
	import { shortId } from '$lib/format';
	import { phaseLabel, phaseSince, resourceState, waitingJobs } from '$lib/queue-view';
	import { cn } from '$lib/utils';
	import Elapsed from './Elapsed.svelte';
	import LevelBadge from './LevelBadge.svelte';
	import QueueBadge from './QueueBadge.svelte';

	interface Props {
		resources: readonly ResourceRecord[];
		/** Serving order. */
		jobs: readonly JobRecord[];
		/** Name of a job, including an ended one whose run still holds a resource. */
		jobName: (id: string) => string | null;
		class?: string;
	}

	let { resources, jobs, jobName, class: className }: Props = $props();

	/** Waiting jobs shown before the rest collapse into a count. */
	const NEXT_LIMIT = 5;

	const waiting = $derived(waitingJobs(jobs));
</script>

<section
	aria-label="GPU queue"
	class={cn('flex flex-col overflow-hidden rounded-lg border border-border bg-card', className)}
>
	<header
		class="flex shrink-0 items-center gap-2 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase"
	>
		<span>GPU queue</span>
		<a
			href={resolve('/queue')}
			class="ml-auto tracking-normal text-primary normal-case hover:underline">open</a
		>
	</header>
	<div class="scrollbar-none min-h-0 overflow-y-auto">
		<ul class="divide-y divide-border/70" aria-label="Resources">
			{#each resources as record (record.resource.id)}
				{@const run = record.run}
				{@const name = run ? jobName(run.job) : null}
				{@const since = run ? phaseSince(run.phase) : null}
				<li class="flex flex-col gap-1 px-3 py-2">
					<div class="flex items-center gap-2">
						<span class="font-mono font-medium">{record.resource.name}</span>
						<QueueBadge
							state={resourceState(record)}
							label={run ? phaseLabel(run.phase) : undefined}
						/>
						{#if run?.phase.phase === 'stopping'}
							<span class="ml-auto text-[11px] text-muted-foreground">
								asked <Elapsed from={run.phase.requested_at} suffix="ago" />
							</span>
						{:else if since}
							<Elapsed from={since} class="ml-auto text-[11px] text-muted-foreground" />
						{/if}
					</div>
					{#if run}
						<a
							href={resolve('/queue/jobs/[id]', { id: run.job })}
							class="truncate text-foreground hover:text-primary"
							title={name ?? run.job}
						>
							{name ?? shortId(run.job)}
						</a>
					{/if}
				</li>
			{/each}
		</ul>
		{#if waiting.length > 0}
			<div
				class="border-t border-border bg-muted px-3 py-1 text-[11px] tracking-wide text-muted-foreground uppercase"
			>
				Next
			</div>
			<ol class="divide-y divide-border/70" aria-label="Waiting jobs">
				{#each waiting.slice(0, NEXT_LIMIT) as job (job.id)}
					<li class="flex items-center gap-2 px-3 py-1.5">
						<LevelBadge priority={job.priority} class="w-12 shrink-0" />
						<a
							href={resolve('/queue/jobs/[id]', { id: job.id })}
							class="min-w-0 truncate hover:text-primary"
							title={job.spec.name}
						>
							{job.spec.name}
						</a>
					</li>
				{/each}
			</ol>
			{#if waiting.length > NEXT_LIMIT}
				<a
					href={resolve('/queue')}
					class="block border-t border-border/70 px-3 py-1.5 text-[11px] text-muted-foreground hover:text-primary"
				>
					{waiting.length - NEXT_LIMIT} more waiting
				</a>
			{/if}
		{/if}
	</div>
</section>
