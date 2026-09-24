<script lang="ts">
	import type { Snippet } from 'svelte';
	import { cn } from '$lib/utils';

	interface Props {
		/** Hue in degrees; the same hue gives the same pill everywhere. */
		hue: number;
		/** Show a leading dot, as machine pills do. */
		dot?: boolean;
		title?: string;
		class?: string;
		children: Snippet;
	}

	let { hue, dot = false, title, class: className, children }: Props = $props();
</script>

<span
	class={cn(
		'inline-flex max-w-full min-w-0 shrink-0 items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] leading-4 font-medium ring-1 ring-inset',
		'bg-[oklch(0.62_0.13_var(--hue)/0.12)] text-[oklch(0.45_0.14_var(--hue))] ring-[oklch(0.62_0.13_var(--hue)/0.35)]',
		'dark:text-[oklch(0.8_0.11_var(--hue))]',
		className
	)}
	style:--hue={hue}
	{title}
>
	{#if dot}
		<span class="size-1.5 shrink-0 rounded-full bg-current" aria-hidden="true"></span>
	{/if}
	<span class="truncate">{@render children()}</span>
</span>
