<script lang="ts">
	import { Popover } from 'bits-ui';
	import Funnel from '@lucide/svelte/icons/funnel';
	import { resolve } from '$app/paths';
	import { shortId } from '$lib/format';
	import { cn } from '$lib/utils';
	import CopyPath from './CopyPath.svelte';

	/** Thread filter that the owner applies, instead of a link to the filtered dashboard. */
	export interface ThreadFilter {
		/** Whether the list already shows only this thread. */
		active: boolean;
		/** Apply the filter, or clear it when it is active. */
		toggle: () => void;
	}

	interface Props {
		/** Full thread UUID. */
		thread: string;
		/** T3 Code or agent title, when one is known. */
		title: string | null;
		/** Characters of the UUID to show. */
		idLength?: number;
		/** Filter control for a list that filters in place. */
		filter?: ThreadFilter;
		class?: string;
	}

	let { thread, title, idLength = 8, filter, class: className }: Props = $props();

	let open = $state(false);

	function toggleFilter() {
		open = false;
		filter?.toggle();
	}
</script>

<!-- a mouse opens the full name on hover; touch has no hover, so a tap opens it -->
<Popover.Root bind:open>
	<Popover.Trigger
		openOnHover
		openDelay={250}
		class={cn(
			'inline-flex max-w-full min-w-0 cursor-pointer items-baseline gap-1.5 rounded text-left hover:text-foreground',
			filter?.active && 'text-primary',
			className
		)}
		aria-label={title ? `Thread ${title}` : `Thread ${thread}`}
	>
		{#if title}
			<span class="truncate font-sans">{title}</span>
		{/if}
		<span class="shrink-0 font-mono text-[11px] text-muted-foreground">
			{shortId(thread, idLength)}
		</span>
	</Popover.Trigger>
	<Popover.Portal>
		<Popover.Content
			side="bottom"
			align="start"
			sideOffset={4}
			collisionPadding={8}
			class="z-50 flex w-max max-w-[min(22rem,calc(100vw-1rem))] flex-col gap-1.5 rounded-md border border-border bg-card p-2.5 text-[13px] text-foreground shadow-lg"
		>
			{#if title}
				<p class="font-medium break-words">{title}</p>
			{:else}
				<p class="text-muted-foreground">No title found for this thread</p>
			{/if}
			<CopyPath value={thread} class="-ml-1 text-[11px] text-muted-foreground" />
			{#if filter}
				<button
					type="button"
					onclick={toggleFilter}
					class="inline-flex items-center gap-1 self-start text-primary hover:underline"
				>
					<Funnel class="size-3" aria-hidden="true" />
					{filter.active ? 'Show tasks from all threads' : 'Show only tasks from this thread'}
				</button>
			{:else}
				<a
					href={resolve(`/?thread=${thread}`)}
					class="inline-flex items-center gap-1 self-start text-primary hover:underline"
				>
					<Funnel class="size-3" aria-hidden="true" />
					Show tasks from this thread
				</a>
			{/if}
		</Popover.Content>
	</Popover.Portal>
</Popover.Root>
