<script lang="ts">
	import type { Snippet } from 'svelte';
	import { resolve } from '$app/paths';
	import ArrowDown from '@lucide/svelte/icons/arrow-down';
	import ArrowUp from '@lucide/svelte/icons/arrow-up';
	import Gpu from '@lucide/svelte/icons/gpu';
	import X from '@lucide/svelte/icons/x';
	import { isProcessStatus, peerTaskHref, type FleetMachine } from '$lib/api';
	import type { ResourceQueueStore } from '$lib/daemon.svelte';
	import { queueMovePlacement, type ResourceQueue } from '$lib/resource-state';
	import { ResourceOperationManager } from '$lib/resource-operations.svelte';
	import type { BrowserResourceAction, ResourceDetail, ResourceTaskSummary } from '$lib/resources';
	import { machineHue } from '$lib/colors';
	import { cn } from '$lib/utils';
	import Capsule from './Capsule.svelte';
	import Elapsed from './Elapsed.svelte';
	import StatusBadge from './StatusBadge.svelte';

	interface Props {
		queues: readonly ResourceQueue[];
		machines: readonly FleetMachine[];
		resourceStore: ResourceQueueStore;
		refreshDashboard: () => Promise<void>;
		/** Controls shown at the end of each card header. */
		actions?: Snippet;
		class?: string;
	}

	let {
		queues,
		machines,
		resourceStore,
		refreshDashboard,
		actions,
		class: className
	}: Props = $props();

	const machineById = $derived(new Map(machines.map((machine) => [machine.machine, machine])));
	const operationManager = new ResourceOperationManager();
	/** The one queued request whose cancel waits for a second click. */
	let confirmingCancel = $state<{ resourceId: string; requestId: string } | null>(null);
	const operationEffects = {
		refresh: () => resourceStore.refresh(),
		readError: () => resourceStore.error,
		showAuthoritative: (detail: ResourceDetail) => resourceStore.showAuthoritative(detail),
		refreshAfterSuccess: () => refreshDashboard()
	};

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

	$effect(() => {
		for (const item of queues) operationManager.load(item.resource.id);
	});

	async function beginAction(resourceId: string, revision: number, action: BrowserResourceAction) {
		confirmingCancel = null;
		await operationManager.begin(resourceId, revision, action, operationEffects);
	}

	async function retryOperation(resourceId: string) {
		await operationManager.retry(resourceId, operationEffects);
	}

	function isConfirming(resourceId: string, requestId: string): boolean {
		return confirmingCancel?.resourceId === resourceId && confirmingCancel.requestId === requestId;
	}

	function commitCancel(resourceId: string, requestId: string, revision: number): void {
		if (!isConfirming(resourceId, requestId)) return;
		void beginAction(resourceId, revision, { type: 'cancel_queued', request_id: requestId });
	}

	function moveQueued(
		item: ResourceQueue,
		requestId: string,
		index: number,
		direction: 'up' | 'down'
	): void {
		const placement = queueMovePlacement(item.queue, index, direction);
		if (!placement) return;
		void beginAction(item.resource.id, item.resource.state_revision, {
			type: 'move_queued',
			request_id: requestId,
			placement
		});
	}

	function operationMessage(state: ReturnType<typeof operationManager.stateFor>): string | null {
		if (state.busy && state.operation) return 'Sending queued request action…';
		if (state.error) return state.error;
		return state.message;
	}
</script>

{#snippet iconButton(Icon: typeof X, label: string, disabled: boolean, onclick: () => void)}
	<button
		type="button"
		{onclick}
		{disabled}
		aria-label={label}
		title={label}
		class="inline-flex size-6 shrink-0 items-center justify-center rounded border border-border hover:bg-accent disabled:cursor-not-allowed disabled:opacity-40"
	>
		<Icon class="size-3.5" aria-hidden="true" />
	</button>
{/snippet}

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

<section aria-label="Resource queues" class={cn('flex flex-col gap-3', className)}>
	{#each queues as item (item.resource.id)}
		{@const authority = machineName(item.resource.authority_machine)}
		{@const operationState = operationManager.stateFor(item.resource.id)}
		{@const resourceActionDisabled =
			resourceStore.error !== null || operationState.operation !== null || operationState.busy}
		<!-- cards are as tall as their queue; the box scrolls a long one -->
		<article class="flex shrink-0 flex-col rounded border border-border bg-card">
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
				{@render actions?.()}
			</header>

			<div class="flex flex-col gap-3 px-3 py-2.5">
				{#if operationState.operation || operationState.error || operationState.message}
					<div
						class={cn(
							'flex min-w-0 flex-wrap items-center justify-between gap-2 rounded border px-2 py-1.5 text-[11px]',
							operationState.error
								? 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300'
								: operationState.operation
									? 'border-amber-500/40 bg-amber-500/10 text-amber-800 dark:text-amber-300'
									: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-300'
						)}
						role={operationState.error ? 'alert' : 'status'}
						aria-live="polite"
					>
						<span class="min-w-0 flex-1 break-words">{operationMessage(operationState)}</span>
						{#if operationState.operation}
							<button
								type="button"
								onclick={() => void retryOperation(item.resource.id)}
								disabled={operationState.busy}
								class="shrink-0 rounded border border-current/30 px-2 py-0.5 hover:bg-black/5 disabled:cursor-not-allowed disabled:opacity-50 dark:hover:bg-white/5"
							>
								Retry
							</button>
						{:else if operationState.error}
							<button
								type="button"
								onclick={() => operationManager.dismissError(item.resource.id)}
								class="shrink-0 rounded border border-current/30 px-2 py-0.5 hover:bg-black/5 dark:hover:bg-white/5"
							>
								Dismiss
							</button>
						{/if}
					</div>
				{/if}
				{#if item.current}
					{@render holder('now', item.current, item.resource.authority_machine)}
				{/if}
				{#if item.background}
					{@render holder('background', item.background, item.resource.authority_machine)}
				{/if}
				{#if !item.current && !item.background}
					<p class="text-muted-foreground">Nothing holds this resource</p>
				{/if}
				{#if item.returnPending}
					<p class="text-amber-700 dark:text-amber-300">{item.returnPending}</p>
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
								{@const confirming = isConfirming(item.resource.id, request.request_id)}
								<li class="flex min-w-0 items-center gap-1 py-1">
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
									<div class="flex shrink-0 items-center gap-0.5">
										{@render iconButton(
											ArrowUp,
											'Move up',
											index === 0 || resourceActionDisabled,
											() => moveQueued(item, request.request_id, index, 'up')
										)}
										{@render iconButton(
											ArrowDown,
											'Move down',
											index === item.queue.length - 1 || resourceActionDisabled,
											() => moveQueued(item, request.request_id, index, 'down')
										)}
										{#if confirming}
											<span class="text-[10px] text-muted-foreground">Cancel?</span>
											<button
												type="button"
												onclick={() =>
													commitCancel(
														item.resource.id,
														request.request_id,
														item.resource.state_revision
													)}
												disabled={resourceActionDisabled}
												class="rounded border border-red-500/40 px-1.5 py-0.5 text-[10px] text-red-700 hover:bg-red-500/10 disabled:cursor-not-allowed disabled:opacity-40 dark:text-red-300"
											>
												Yes
											</button>
											<button
												type="button"
												onclick={() => (confirmingCancel = null)}
												class="rounded border border-border px-1.5 py-0.5 text-[10px] hover:bg-accent"
											>
												No
											</button>
										{:else}
											{@render iconButton(
												X,
												'Cancel queued request',
												resourceActionDisabled,
												() => {
													confirmingCancel = {
														resourceId: item.resource.id,
														requestId: request.request_id
													};
												}
											)}
										{/if}
									</div>
								</li>
							{/each}
						</ol>
					{/if}
				</div>
			</div>
		</article>
	{/each}
</section>
