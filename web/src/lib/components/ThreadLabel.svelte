<script lang="ts">
	import { resolve } from '$app/paths';
	import { shortId } from '$lib/format';
	import { cn } from '$lib/utils';

	interface Props {
		/** Full thread UUID. */
		thread: string;
		/** T3 Code or agent title, when one is known. */
		title: string | null;
		/** Characters of the UUID to show. */
		idLength?: number;
		/** Links to the dashboard filtered to this thread. */
		filterLink?: boolean;
		/** Set false inside a control that has its own hover text. */
		tooltip?: boolean;
		class?: string;
	}

	let {
		thread,
		title,
		idLength = 8,
		filterLink = false,
		tooltip = true,
		class: className
	}: Props = $props();

	const hover = $derived(tooltip ? (title ? `${title} · ${thread}` : thread) : undefined);
</script>

{#snippet content()}
	{#if title}
		<span class="truncate font-sans">{title}</span>
	{/if}
	<span class="shrink-0 font-mono text-[11px] text-muted-foreground">
		{shortId(thread, idLength)}
	</span>
{/snippet}

{#if filterLink}
	<a
		href={resolve(`/?thread=${thread}`)}
		class={cn('inline-flex min-w-0 items-baseline gap-1.5 hover:underline', className)}
		title={hover}
	>
		{@render content()}
	</a>
{:else}
	<span class={cn('inline-flex min-w-0 items-baseline gap-1.5', className)} title={hover}>
		{@render content()}
	</span>
{/if}
