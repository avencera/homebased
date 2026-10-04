<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	const stepMark = tv({
		base: 'size-2 shrink-0 rounded-full ring-1 ring-inset',
		variants: {
			progress: {
				done: 'bg-emerald-500 ring-emerald-500',
				current: 'animate-pulse bg-sky-500 ring-sky-500 motion-reduce:animate-none',
				failed: 'bg-red-500 ring-red-500',
				pending: 'bg-transparent ring-border',
				skipped: 'bg-transparent ring-border opacity-50'
			}
		}
	});
</script>

<script lang="ts">
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import X from '@lucide/svelte/icons/x';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import JobControls from '$lib/components/JobControls.svelte';
	import LevelBadge from '$lib/components/LevelBadge.svelte';
	import QueueBadge from '$lib/components/QueueBadge.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import ThreadLabel from '$lib/components/ThreadLabel.svelte';
	import { JobStore, QueueControl } from '$lib/daemon.svelte';
	import { EM_DASH, formatTimestamp, shortId, shortenHome } from '$lib/format';
	import {
		cleanupLabel,
		phaseLabel,
		phaseSince,
		preemptionLabel,
		resourceNames,
		stepLabel,
		stepProgress,
		stepText,
		stopCauseLabel,
		targetLabel
	} from '$lib/queue-view';
	import { ThreadTitleStore } from '$lib/thread-titles.svelte';
	import { cn } from '$lib/utils';

	const id = $derived(page.params.id ?? '');
	const store = new JobStore(() => id);
	const control = new QueueControl(() => store.refresh());
	const detail = $derived(store.detail);
	const job = $derived(detail?.job ?? null);
	const names = $derived(resourceNames(store.resources));
	const activeRun = $derived(detail?.active_run ?? null);
	const failedStep = $derived.by(() => {
		const state = job?.state;
		if (state?.state !== 'failed') return null;
		return detail?.runs.find((run) => run.task === state.run)?.step ?? null;
	});
	const lastRun = $derived(detail?.runs.at(-1) ?? null);
	// the submitting thread runs on the origin, which the title read omits for this machine
	const threadRef = $derived(
		job
			? {
					machine: job.origin === store.localMachine ? undefined : job.origin,
					thread: job.spec.thread
				}
			: null
	);
	const threadTitles = new ThreadTitleStore(() => (threadRef ? [threadRef] : []));

	function resourceName(resource: string | null): string {
		if (resource === null) return EM_DASH;
		return names.get(resource) ?? shortId(resource);
	}
</script>

<div class="mx-auto max-w-5xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/queue')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			GPU queue
		</a>
		{#if job}
			<h1 class="min-w-0 text-base font-semibold tracking-tight wrap-anywhere">{job.spec.name}</h1>
		{/if}
		<CopyPath value={id} label={shortId(id, 13)} />
		{#if job}
			<QueueBadge state={job.state.state} />
			{#if job.position !== null}
				<JobControls {job} {control} />
			{/if}
		{/if}
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				refreshed <Elapsed from={store.lastFetched} suffix="ago" />
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

	{#if job && detail}
		<dl
			class="mt-3 grid grid-cols-[7rem_minmax(0,1fr)] gap-x-3 gap-y-1 rounded border border-border bg-card px-3 py-2 sm:grid-cols-[7rem_minmax(0,1fr)_7rem_minmax(0,1fr)]"
		>
			<dt class="text-muted-foreground">level</dt>
			<dd class="flex items-center gap-2">
				<LevelBadge priority={job.priority} />
				{#if job.position !== null}
					<span class="font-mono text-[11px] text-muted-foreground">#{job.position}</span>
				{/if}
			</dd>

			<dt class="text-muted-foreground">run</dt>
			<dd class="flex flex-wrap items-center gap-x-2">
				{#if activeRun}
					{@const since = phaseSince(activeRun.phase)}
					<span class="font-mono">{phaseLabel(activeRun.phase)}</span>
					<span class="font-mono text-muted-foreground">{resourceName(activeRun.resource)}</span>
					{#if since}
						<Elapsed from={since} class="text-muted-foreground" />
					{/if}
				{:else if job.state.state === 'queued'}
					<span class="text-muted-foreground"
						>{job.state.resume ? 'waiting, resumes' : 'waiting'}</span
					>
				{:else}
					{EM_DASH}
				{/if}
			</dd>

			<dt class="text-muted-foreground">target</dt>
			<dd class="font-mono">{targetLabel(job.target, names)}</dd>

			<dt class="text-muted-foreground">preempt</dt>
			<dd class="font-mono">{preemptionLabel(job.spec.preempt)}</dd>

			<dt class="text-muted-foreground">steps</dt>
			<dd class="font-mono">{stepLabel(job)}</dd>

			<dt class="text-muted-foreground">runs</dt>
			<dd class="font-mono">{job.runs}</dd>

			<dt class="text-muted-foreground">last stop</dt>
			<dd class="font-mono">
				{detail.last_stop_cause ? stopCauseLabel(detail.last_stop_cause) : EM_DASH}
			</dd>

			<dt class="text-muted-foreground">cleanup</dt>
			<dd class="font-mono wrap-anywhere">{lastRun ? cleanupLabel(lastRun.cleanup) : EM_DASH}</dd>

			<dt class="text-muted-foreground">cwd</dt>
			<dd class="flex min-w-0 items-center gap-1">
				{#if store.local}
					<a
						href={resolve(`/files?path=${encodeURIComponent(job.spec.cwd)}`)}
						class="truncate font-mono text-primary hover:underline"
						title={job.spec.cwd}
					>
						{shortenHome(job.spec.cwd)}
					</a>
				{:else}
					<span class="truncate font-mono" title={job.spec.cwd}>{shortenHome(job.spec.cwd)}</span>
				{/if}
				<CopyPath value={job.spec.cwd} iconOnly class="relative z-10" />
			</dd>

			<dt class="text-muted-foreground">thread</dt>
			<dd class="flex min-w-0 items-center gap-1">
				{#if threadRef}
					<ThreadLabel
						thread={job.spec.thread}
						title={threadTitles.title(threadRef)}
						idLength={13}
					/>
				{/if}
			</dd>

			<dt class="text-muted-foreground">submitted</dt>
			<dd>
				{formatTimestamp(job.created_at)}
				<span class="text-muted-foreground">(<Elapsed from={job.created_at} suffix="ago" />)</span>
			</dd>

			<dt class="text-muted-foreground">updated</dt>
			<dd>{formatTimestamp(job.updated_at)}</dd>
		</dl>

		<section class="mt-4">
			<h2 class="mb-1 text-[11px] tracking-wide text-muted-foreground uppercase">
				steps ({job.spec.steps.length})
			</h2>
			<ol class="divide-y divide-border rounded border border-border bg-card">
				{#each job.spec.steps as step, index (index)}
					{@const progress = stepProgress(job, index, failedStep)}
					<li class="flex items-center gap-3 px-3 py-1.5">
						<span class={stepMark({ progress })} title={progress}></span>
						<span class="w-6 shrink-0 font-mono text-[11px] text-muted-foreground tabular-nums">
							{index + 1}
						</span>
						<span
							class={cn(
								'min-w-0 truncate font-mono text-[12px]',
								progress === 'skipped' && 'text-muted-foreground'
							)}
							title={stepText(step)}
						>
							{stepText(step)}
						</span>
					</li>
				{/each}
			</ol>
		</section>

		<section class="mt-4">
			<h2 class="mb-1 text-[11px] tracking-wide text-muted-foreground uppercase">
				runs ({detail.runs.length})
			</h2>
			{#if detail.runs.length === 0}
				<p class="rounded border border-border bg-card px-3 py-2 text-muted-foreground">
					Not started
				</p>
			{:else}
				<div class="overflow-x-auto rounded border border-border bg-card">
					<table class="w-full text-left">
						<thead class="bg-muted text-[11px] tracking-wide text-muted-foreground uppercase">
							<tr>
								<th class="px-3 py-1.5 font-normal">Run</th>
								<th class="px-3 py-1.5 font-normal">Step</th>
								<th class="px-3 py-1.5 font-normal">Resource</th>
								<th class="px-3 py-1.5 font-normal">Status</th>
								<th class="px-3 py-1.5 font-normal">Stop</th>
								<th class="px-3 py-1.5 font-normal">Cleanup</th>
								<th class="px-3 py-1.5 font-normal">Task</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-border/70">
							{#each detail.runs.toReversed() as run (run.task)}
								<tr>
									<td class="px-3 py-1.5 font-mono tabular-nums">{run.run_number}</td>
									<td class="px-3 py-1.5 font-mono tabular-nums">{run.step + 1}</td>
									<td class="px-3 py-1.5 font-mono">{resourceName(run.resource)}</td>
									<td class="px-3 py-1.5"><StatusBadge status={run.status} /></td>
									<td class="px-3 py-1.5 font-mono text-muted-foreground">
										{run.stop_cause ? stopCauseLabel(run.stop_cause) : EM_DASH}
									</td>
									<td
										class={cn(
											'px-3 py-1.5 font-mono',
											run.cleanup && 'Err' in run.cleanup
												? 'text-red-700 dark:text-red-300'
												: 'text-muted-foreground'
										)}
									>
										{cleanupLabel(run.cleanup)}
									</td>
									<td class="px-3 py-1.5">
										{#if store.local}
											<a
												href={resolve('/tasks/[id]', { id: run.task })}
												class="font-mono text-primary hover:underline"
												title={run.task}
											>
												{shortId(run.task, 13)}
											</a>
										{:else}
											<CopyPath value={run.task} label={shortId(run.task, 13)} />
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</section>
	{/if}
</div>
