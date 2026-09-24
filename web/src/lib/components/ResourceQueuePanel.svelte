<script lang="ts">
	import { resolve } from '$app/paths';
	import Gpu from '@lucide/svelte/icons/gpu';
	import { isProcessStatus, peerTaskHref, type FleetMachine } from '$lib/api';
	import type { ResourceQueue } from '$lib/resource-state';
	import type { ResourceTaskSummary } from '$lib/resources';
	import { machineHue } from '$lib/colors';
	import { cn } from '$lib/utils';
	import Capsule from './Capsule.svelte';
	import Elapsed from './Elapsed.svelte';
	import StatusBadge from './StatusBadge.svelte';

	interface Props {
		queues: readonly ResourceQueue[];
		machines: readonly FleetMachine[];
		class?: string;
	}

	let { queues, machines, class: className }: Props = $props();

	const machineById = $derived(new Map(machines.map((machine) => [machine.machine, machine])));

	const toneClass = {
		green: 'text-emerald-700 dark:text-emerald-300',
		blue: 'text-sky-700 dark:text-sky-300',
		amber: 'text-amber-700 dark:text-amber-300',
		red: 'text-red-700 dark:text-red-300',
		neutral: 'text-muted-foreground'
	} as const;

	function machineName(id: string): string {
		return machineById.get(id)?.name ?? id.slice(0, 8);
	}

	function runsOn(task: ResourceTaskSummary, authority: string): FleetMachine | undefined {
		return machineById.get(task.execution_machine ?? authority);
	}
</script>

{#snippet holder(label: string, task: ResourceTaskSummary, authority: string)}
	{@const machine = runsOn(task, authority)}
	{@const peerHref = peerTaskHref(machine, task.id)}
	<div class="flex flex-col gap-1">
		<span class="text-[11px] tracking-wide text-muted-foreground uppercase">{label}</span>
		<div class="flex min-w-0 items-center gap-2">
			{#if machine?.location.type === 'local'}
				<a
					href={resolve('/tasks/[id]', { id: task.id })}
					class="truncate font-medium text-primary hover:underline"
					title={task.display_name}>{task.display_name}</a
				>
			{:else if peerHref}
				<a
					href={peerHref}
					rel="external"
					class="truncate font-medium text-primary hover:underline"
					title={`${task.display_name} on ${machine?.name}`}>{task.display_name}</a
				>
			{:else}
				<span class="truncate font-medium" title={task.display_name}>{task.display_name}</span>
			{/if}
		</div>
		<div class="flex items-center gap-2 text-muted-foreground">
			{#if isProcessStatus(task.status)}
				<StatusBadge status={task.status} />
			{:else}
				<span class="font-mono text-[11px]">{task.status}</span>
			{/if}
			{#if task.created_at}
				<Elapsed from={task.created_at} />
			{/if}
		</div>
	</div>
{/snippet}

<section
	aria-label="Resource queues"
	class={cn(
		'grid auto-rows-[minmax(100%,auto)] grid-cols-[repeat(auto-fit,minmax(20rem,1fr))] gap-3',
		className
	)}
>
	{#each queues as item (item.resource.id)}
		{@const authority = machineName(item.resource.authority_machine)}
		<article class="flex flex-col rounded border border-border bg-card">
			<header
				class="flex items-center gap-2 border-b border-border bg-muted px-3 py-1.5 text-[11px]"
			>
				<Gpu class="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
				<a
					href={resolve('/resources/[id]', { id: item.resource.id })}
					class="truncate font-medium text-foreground hover:text-primary"
				>
					{item.resource.display_name}
				</a>
				<Capsule hue={machineHue(authority)} dot title={`Authority ${authority}`}
					>{authority}</Capsule
				>
				<span
					class={cn('ml-auto shrink-0', toneClass[item.status.tone])}
					title={item.status.message}
				>
					{item.status.label}
				</span>
			</header>

			<div class="flex flex-1 flex-col gap-3 px-3 py-2.5">
				{#if item.current}
					{@render holder('now', item.current, item.resource.authority_machine)}
				{/if}
				{#if item.background}
					{@render holder('background', item.background, item.resource.authority_machine)}
				{/if}
				{#if !item.current && !item.background}
					<p class="text-muted-foreground">Nothing holds this resource</p>
				{/if}

				<div class="flex flex-col gap-1">
					<span class="text-[11px] tracking-wide text-muted-foreground uppercase">
						queue <span class="tabular-nums">{item.queue.length}</span>
					</span>
					{#if item.queue.length === 0}
						<p class="text-muted-foreground">Empty</p>
					{:else}
						<ol class="flex flex-col divide-y divide-border/60">
							{#each item.queue as request, index (request.request_id)}
								{@const origin = machineName(request.origin_machine)}
								<li class="flex min-w-0 items-baseline gap-2 py-1">
									<span
										class="w-4 shrink-0 text-right font-mono text-muted-foreground tabular-nums"
									>
										{index + 1}
									</span>
									<span class="min-w-0 flex-1 truncate" title={request.display_name}>
										{request.display_name}
									</span>
									{#if request.origin_machine !== item.resource.authority_machine}
										<Capsule hue={machineHue(origin)} title={`Requested from ${origin}`}
											>{origin}</Capsule
										>
									{/if}
								</li>
							{/each}
						</ol>
					{/if}
				</div>
			</div>
		</article>
	{/each}
</section>
