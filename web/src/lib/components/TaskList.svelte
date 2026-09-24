<script lang="ts">
	import type { Snippet } from 'svelte';
	import { resolve } from '$app/paths';
	import Ban from '@lucide/svelte/icons/ban';
	import Funnel from '@lucide/svelte/icons/funnel';
	import { isInFlight, peerTaskHref, type FleetMachine, type FleetTask } from '$lib/api';
	import { hueFor, machineHue } from '$lib/colors';
	import { formatTimestamp, projectName, shortId, shortenHome, workloadLabel } from '$lib/format';
	import { cn } from '$lib/utils';
	import CallbackBadge from './CallbackBadge.svelte';
	import Capsule from './Capsule.svelte';
	import Elapsed from './Elapsed.svelte';
	import StatusBadge from './StatusBadge.svelte';

	interface Props {
		tasks: readonly FleetTask[];
		machines: readonly FleetMachine[];
		/** Thread the list is filtered to, highlighted in rows. */
		activeThread: string | null;
		/** Project the list is filtered to, highlighted in rows. */
		activeProject: string | null;
		onThread: (thread: string) => void;
		onProject: (project: string) => void;
		/** Rendered in place of rows when the list is empty. */
		empty: Snippet;
		class?: string;
	}

	let {
		tasks,
		machines,
		activeThread,
		activeProject,
		onThread,
		onProject,
		empty,
		class: className
	}: Props = $props();

	const machineById = $derived(new Map(machines.map((machine) => [machine.machine, machine])));

	function machineName(id: string): string {
		return machineById.get(id)?.name ?? shortId(id);
	}

	function isLocal(id: string): boolean {
		return machineById.get(id)?.location.type === 'local';
	}
</script>

<div class={cn('flex flex-col overflow-hidden rounded-lg border border-border bg-card', className)}>
	<div
		class="task-grid hidden shrink-0 gap-x-3 border-b border-border bg-muted px-3 py-1.5 pl-4 text-[11px] tracking-wide text-muted-foreground uppercase lg:grid"
		aria-hidden="true"
	>
		<span class="[grid-area:machine]">Machine</span>
		<span class="[grid-area:name]">Task</span>
		<span class="[grid-area:status]">Status</span>
		<span class="[grid-area:time]">Time</span>
		<span class="[grid-area:cwd]">Project</span>
		<span class="[grid-area:thread]">Thread</span>
		<span class="[grid-area:callback]">Callback</span>
	</div>

	{#if tasks.length === 0}
		{@render empty()}
	{:else}
		<ul
			class="scrollbar-none min-h-0 flex-1 divide-y divide-border/70 overflow-y-auto"
			aria-label="Tasks"
		>
			{#each tasks as entry (entry.task.id)}
				{@const task = entry.task}
				{@const machine = machineName(entry.machine)}
				{@const project = projectName(task)}
				{@const peerHref = peerTaskHref(machineById.get(entry.machine), task.id)}
				<li
					class="task-grid relative grid items-center gap-x-3 gap-y-1 px-3 py-2 pl-4 hover:bg-accent/50 lg:py-1.5"
					style:--hue={machineHue(machine)}
				>
					<span
						class="absolute inset-y-0 left-0 w-[3px] bg-[oklch(0.62_0.13_var(--hue))]"
						aria-hidden="true"
					></span>

					<span class="[grid-area:machine]">
						<Capsule hue={machineHue(machine)} dot title={`Runs on ${machine}`}>{machine}</Capsule>
					</span>

					<span class="flex min-w-0 flex-col [grid-area:name]">
						{#if isLocal(entry.machine)}
							<a
								href={resolve('/tasks/[id]', { id: task.id })}
								class="truncate font-medium text-foreground after:absolute after:inset-0 after:content-[''] hover:text-primary"
								title={task.display_name}
							>
								{task.display_name}
							</a>
						{:else if peerHref}
							<a
								href={peerHref}
								rel="external"
								class="truncate font-medium text-foreground after:absolute after:inset-0 after:content-[''] hover:text-primary"
								title={`${task.display_name} on ${machine}`}
							>
								{task.display_name}
							</a>
						{:else}
							<span class="truncate font-medium" title={task.display_name}>
								{task.display_name}
							</span>
						{/if}
						<span class="truncate font-mono text-[11px] text-muted-foreground">
							{workloadLabel(task)} &middot; <span title={task.id}>{shortId(task.id)}</span>
						</span>
					</span>

					<span
						class="flex items-center gap-1 justify-self-end [grid-area:status] lg:justify-self-start"
					>
						<StatusBadge status={task.status} />
						{#if task.cancel_requested_at}
							<span title={`Cancel requested ${formatTimestamp(task.cancel_requested_at)}`}>
								<Ban class="size-3 text-amber-600 dark:text-amber-400" aria-hidden="true" />
								<span class="sr-only">Cancel requested</span>
							</span>
						{/if}
					</span>

					<Elapsed
						from={task.created_at}
						to={isInFlight(task.status) ? null : task.updated_at}
						class="text-muted-foreground [grid-area:time]"
					/>

					<span class="flex min-w-0 items-center gap-1.5 [grid-area:cwd]">
						<!-- raised above the row link so a click filters instead of opening the task -->
						<button
							type="button"
							onclick={() => onProject(project)}
							class={cn(
								'relative z-10 min-w-0 shrink-0 rounded-full',
								activeProject === project && 'ring-2 ring-primary/60'
							)}
							title={`Show only tasks in ${project}`}
						>
							<Capsule hue={hueFor(project)}>{project}</Capsule>
						</button>
						<span class="truncate font-mono text-[11px] text-muted-foreground" title={task.cwd}>
							{shortenHome(task.cwd)}
						</span>
					</span>

					<button
						type="button"
						onclick={() => onThread(task.thread)}
						class={cn(
							'relative z-10 inline-flex items-center gap-1 justify-self-end rounded px-1 py-0.5 font-mono text-[11px] text-muted-foreground [grid-area:thread] hover:bg-accent hover:text-foreground lg:justify-self-start',
							activeThread === task.thread && 'text-primary'
						)}
						title={`Show only tasks from thread ${task.thread}`}
					>
						<Funnel class="size-3 opacity-60" aria-hidden="true" />
						{shortId(task.thread)}
					</button>

					<CallbackBadge
						callback={task.callback}
						class="justify-self-end [grid-area:callback] lg:justify-self-start"
					/>
				</li>
			{/each}
		</ul>
	{/if}
</div>

<style>
	/* narrow screens stack each task into three short lines; wide screens use
	   one aligned row per task under a shared header */
	.task-grid {
		grid-template-columns: auto minmax(0, 1fr) auto;
		grid-template-areas:
			'name name status'
			'machine time callback'
			'cwd cwd thread';
	}

	@media (width >= 64rem) {
		.task-grid {
			grid-template-columns:
				6rem minmax(14rem, 1.6fr) 6.5rem 4.5rem minmax(12rem, 1.2fr)
				6.5rem 5rem;
			grid-template-areas: 'machine name status time cwd thread callback';
		}
	}
</style>
