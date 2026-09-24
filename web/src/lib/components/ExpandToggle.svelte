<script lang="ts">
	import Maximize2 from '@lucide/svelte/icons/maximize-2';
	import Minimize2 from '@lucide/svelte/icons/minimize-2';
	import { cn } from '$lib/utils';

	interface Props {
		/** Whether the box fills the screen now. */
		expanded: boolean;
		/** Name of the box, for the accessible label. */
		label: string;
		onToggle: () => void;
		class?: string;
	}

	let { expanded, label, onToggle, class: className }: Props = $props();
</script>

<!-- raised above row link overlays so a click toggles instead of opening a task -->
<button
	type="button"
	onclick={onToggle}
	aria-pressed={expanded}
	aria-label={expanded ? `Restore ${label}` : `Expand ${label}`}
	title={expanded ? `Restore ${label} (Esc)` : `Expand ${label}`}
	class={cn(
		'relative z-10 inline-flex size-6 shrink-0 items-center justify-center rounded text-muted-foreground hover:bg-accent hover:text-foreground',
		className
	)}
>
	{#if expanded}
		<Minimize2 class="size-3.5" aria-hidden="true" />
	{:else}
		<Maximize2 class="size-3.5" aria-hidden="true" />
	{/if}
</button>
