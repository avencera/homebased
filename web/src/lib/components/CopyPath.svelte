<script lang="ts">
	import Check from '@lucide/svelte/icons/check';
	import Copy from '@lucide/svelte/icons/copy';
	import { cn } from '$lib/utils';

	interface Props {
		/** Text written to the clipboard. */
		value: string;
		/** Visible text. Defaults to the value itself. */
		label?: string;
		/** Show the icon alone. */
		iconOnly?: boolean;
		class?: string;
	}

	let { value, label, iconOnly = false, class: className }: Props = $props();

	let copied = $state(false);
	let timer: ReturnType<typeof setTimeout> | undefined;

	async function copy() {
		// loopback http is a secure context, so the clipboard API is available
		try {
			await navigator.clipboard.writeText(value);
		} catch {
			return;
		}
		copied = true;
		clearTimeout(timer);
		timer = setTimeout(() => (copied = false), 1200);
	}

	$effect(() => () => clearTimeout(timer));
</script>

<button
	type="button"
	onclick={copy}
	title={`copy ${value}`}
	aria-label={`copy ${value}`}
	class={cn(
		'group inline-flex max-w-full min-w-0 items-center gap-1 rounded px-1 py-0.5 text-left hover:bg-accent',
		className
	)}
>
	{#if !iconOnly}
		<span class="truncate font-mono">{label ?? value}</span>
	{/if}
	{#if copied}
		<Check class="size-3 shrink-0 text-emerald-500" />
	{:else}
		<Copy class="size-3 shrink-0 opacity-40 group-hover:opacity-100" />
	{/if}
</button>
