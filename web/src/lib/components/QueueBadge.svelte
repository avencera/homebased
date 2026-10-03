<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	/** Badge classes per job state and resource state, in the task status palette. */
	export const queueBadge = tv({
		base: 'inline-flex items-center gap-1 rounded px-1.5 py-0.5 font-mono text-[11px] leading-4 whitespace-nowrap ring-1 ring-inset',
		variants: {
			state: {
				idle: 'bg-transparent text-muted-foreground ring-border',
				launching: 'bg-sky-500/5 text-sky-700 ring-sky-500/25 dark:text-sky-300',
				executing: 'bg-sky-500/10 text-sky-700 ring-sky-500/40 dark:text-sky-300',
				stopping: 'bg-amber-500/10 text-amber-700 ring-amber-500/40 dark:text-amber-300',
				cleaning: 'bg-slate-500/10 text-slate-700 ring-slate-500/30 dark:text-slate-300',
				attention: 'bg-red-500/15 text-red-700 ring-red-500/60 dark:text-red-300',
				queued: 'bg-slate-500/10 text-slate-700 ring-slate-500/30 dark:text-slate-300',
				active: 'bg-sky-500/10 text-sky-700 ring-sky-500/40 dark:text-sky-300',
				succeeded: 'bg-emerald-500/10 text-emerald-700 ring-emerald-500/40 dark:text-emerald-300',
				failed: 'bg-red-500/10 text-red-700 ring-red-500/40 dark:text-red-300',
				cancelled: 'bg-amber-500/10 text-amber-700 ring-amber-500/40 dark:text-amber-300'
			}
		}
	});

	/** Every state the badge colors. */
	export type QueueBadgeState = keyof typeof queueBadge.variants.state;
</script>

<script lang="ts">
	import { cn } from '$lib/utils';

	interface Props {
		state: QueueBadgeState;
		/** Text shown instead of the state name, such as `stopping · yield`. */
		label?: string;
		class?: string;
	}

	let { state, label, class: className }: Props = $props();
</script>

<span class={cn(queueBadge({ state }), className)}>
	{#if state === 'executing' || state === 'active'}
		<span class="size-1.5 animate-pulse rounded-full bg-current motion-reduce:animate-none"></span>
	{:else if state === 'attention'}
		<span class="size-1.5 rounded-full bg-current"></span>
	{/if}
	{label ?? state}
</span>
