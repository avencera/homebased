<script lang="ts">
	import Pencil from '@lucide/svelte/icons/pencil';
	import { setResourceLabel, type ResourceRecord } from '$lib/api';
	import type { QueueControl } from '$lib/daemon.svelte';
	import { cn } from '$lib/utils';

	interface Props {
		record: ResourceRecord;
		/** Offers renaming when given; omit for a read-only title. */
		control?: QueueControl;
		/** Classes of the title, so each list keeps its own weight and tone. */
		class?: string;
	}

	let { record, control, class: className }: Props = $props();

	/** Same limit the daemon enforces, so a long nickname is cut here instead of refused. */
	const MAX_CHARS = 40;

	const resource = $derived(record.resource);
	const label = $derived(resource.label ?? null);
	let editing = $state(false);
	let draft = $state('');

	function start() {
		draft = label ?? '';
		editing = true;
	}

	function save() {
		if (!editing || !control) return;
		editing = false;
		const next = draft.trim() || null;
		if (next === label) return;
		void control.run(`label:${resource.id}`, () =>
			setResourceLabel(resource.id, record.machine, next)
		);
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') {
			event.preventDefault();
			save();
		} else if (event.key === 'Escape') {
			editing = false;
		}
	}

	function focus(input: HTMLInputElement) {
		input.focus();
		input.select();
	}
</script>

{#if editing}
	<input
		type="text"
		bind:value={draft}
		maxlength={MAX_CHARS}
		placeholder={resource.name}
		aria-label={`Nickname for ${resource.name}`}
		class="w-40 rounded border border-border bg-background px-1.5 py-0.5 text-sm focus-visible:outline-2 focus-visible:outline-primary"
		onkeydown={onKeydown}
		onblur={save}
		use:focus
	/>
{:else}
	<span class="group inline-flex items-center gap-1.5">
		{#if label}
			<span class={className}>{label}</span>
			<span class="font-mono text-[11px] text-muted-foreground">{resource.name}</span>
		{:else}
			<span class={cn('font-mono', className)}>{resource.name}</span>
		{/if}
		{#if control}
			<button
				type="button"
				disabled={control.pending !== null}
				onclick={start}
				aria-label={`Rename ${label ?? resource.name}`}
				title="Set a nickname; jobs keep using the name"
				class="inline-flex size-5 items-center justify-center rounded text-muted-foreground opacity-0 group-hover:opacity-100 hover:bg-accent hover:text-foreground focus-visible:opacity-100 focus-visible:outline-2 focus-visible:outline-primary disabled:cursor-not-allowed pointer-coarse:opacity-100"
			>
				<Pencil class="size-3" aria-hidden="true" />
			</button>
		{/if}
	</span>
{/if}
