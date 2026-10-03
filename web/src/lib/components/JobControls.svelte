<script lang="ts">
	import { DropdownMenu } from 'bits-ui';
	import ArrowDown from '@lucide/svelte/icons/arrow-down';
	import ArrowUp from '@lucide/svelte/icons/arrow-up';
	import Check from '@lucide/svelte/icons/check';
	import Ellipsis from '@lucide/svelte/icons/ellipsis';
	import { cancelJob, moveJob, type JobRecord, type Placement } from '$lib/api';
	import type { QueueControl } from '$lib/daemon.svelte';
	import { PRIORITIES, nudgePlacement, type LevelGroup } from '$lib/queue-view';
	import { cn } from '$lib/utils';
	import ConfirmDialog from './ConfirmDialog.svelte';

	interface Props {
		job: JobRecord;
		control: QueueControl;
		/** The whole queue by level, for one-place moves; omit to offer only the menu. */
		groups?: readonly LevelGroup[];
		class?: string;
	}

	let { job, control, groups, class: className }: Props = $props();

	let confirmingCancel = $state(false);
	const busy = $derived(control.pending !== null);
	const up = $derived(groups ? nudgePlacement(groups, job.id, 'up') : null);
	const down = $derived(groups ? nudgePlacement(groups, job.id, 'down') : null);

	function move(placement: Placement) {
		void control.run(job.id, (operationId) => moveJob(job.id, job.machine, operationId, placement));
	}

	function cancel() {
		void control.run(job.id, (operationId) => cancelJob(job.id, job.machine, operationId));
	}

	const iconButton =
		'inline-flex size-6 shrink-0 items-center justify-center rounded border border-border hover:bg-accent focus-visible:outline-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:opacity-30';
	const item =
		'flex cursor-default items-center gap-2 rounded px-2 py-1 outline-none select-none data-disabled:opacity-40 data-highlighted:bg-accent';
</script>

<div class={cn('flex items-center gap-1', className)}>
	{#if groups}
		<button
			type="button"
			class={iconButton}
			disabled={busy || up === null}
			aria-label={`Move ${job.spec.name} up`}
			title="Move up"
			onclick={() => up && move(up)}
		>
			<ArrowUp class="size-3.5" aria-hidden="true" />
		</button>
		<button
			type="button"
			class={iconButton}
			disabled={busy || down === null}
			aria-label={`Move ${job.spec.name} down`}
			title="Move down"
			onclick={() => down && move(down)}
		>
			<ArrowDown class="size-3.5" aria-hidden="true" />
		</button>
	{/if}
	<DropdownMenu.Root>
		<DropdownMenu.Trigger
			class={iconButton}
			disabled={busy}
			aria-label={`More actions for ${job.spec.name}`}
			title="More"
		>
			<Ellipsis class="size-3.5" aria-hidden="true" />
		</DropdownMenu.Trigger>
		<DropdownMenu.Portal>
			<DropdownMenu.Content
				align="end"
				sideOffset={4}
				collisionPadding={8}
				class="z-50 min-w-40 rounded-md border border-border bg-card p-1 text-[13px] text-foreground shadow-lg"
			>
				<DropdownMenu.Item
					class={item}
					onSelect={() => move({ type: 'edge', priority: null, end: 'front' })}
				>
					Move to front of {job.priority}
				</DropdownMenu.Item>
				<DropdownMenu.Item
					class={item}
					onSelect={() => move({ type: 'edge', priority: null, end: 'back' })}
				>
					Move to back of {job.priority}
				</DropdownMenu.Item>
				<DropdownMenu.Separator class="my-1 h-px bg-border" />
				<DropdownMenu.Group>
					<DropdownMenu.GroupHeading class="px-2 py-0.5 text-[11px] text-muted-foreground">
						Level
					</DropdownMenu.GroupHeading>
					{#each PRIORITIES as priority (priority)}
						<DropdownMenu.Item
							class={item}
							disabled={priority === job.priority}
							onSelect={() => move({ type: 'edge', priority, end: 'back' })}
						>
							<Check
								class={cn('size-3.5', priority !== job.priority && 'invisible')}
								aria-hidden="true"
							/>
							{priority}
						</DropdownMenu.Item>
					{/each}
				</DropdownMenu.Group>
				<DropdownMenu.Separator class="my-1 h-px bg-border" />
				<DropdownMenu.Item
					class={cn(item, 'text-red-700 dark:text-red-300')}
					onSelect={() => (confirmingCancel = true)}
				>
					Cancel job…
				</DropdownMenu.Item>
			</DropdownMenu.Content>
		</DropdownMenu.Portal>
	</DropdownMenu.Root>
</div>

<ConfirmDialog
	bind:open={confirmingCancel}
	title={`Cancel ${job.spec.name}?`}
	confirmLabel="Cancel job"
	onConfirm={cancel}
>
	{#if job.state.state === 'active'}
		The running step stops and its processes are cleaned up.
	{:else}
		The job leaves the queue.
	{/if}
</ConfirmDialog>
