<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	/** Badge classes per `ProcessStatus`. */
	export const statusBadge = tv({
		base: 'inline-flex items-center gap-1 rounded px-1.5 py-0.5 font-mono text-[11px] leading-4 ring-1 ring-inset',
		variants: {
			status: {
				queued: 'bg-slate-500/10 text-slate-700 ring-slate-500/30 dark:text-slate-300',
				running: 'bg-sky-500/10 text-sky-700 ring-sky-500/40 dark:text-sky-300',
				succeeded: 'bg-emerald-500/10 text-emerald-700 ring-emerald-500/40 dark:text-emerald-300',
				failed: 'bg-red-500/10 text-red-700 ring-red-500/40 dark:text-red-300',
				cancelled: 'bg-amber-500/10 text-amber-700 ring-amber-500/40 dark:text-amber-300',
				lost: 'bg-fuchsia-500/10 text-fuchsia-700 ring-fuchsia-500/40 dark:text-fuchsia-300'
			}
		}
	});
</script>

<script lang="ts">
	import type { ProcessStatus } from '$lib/api';
	import { cn } from '$lib/utils';

	interface Props {
		status: ProcessStatus;
		class?: string;
	}

	let { status, class: className }: Props = $props();
</script>

<span class={cn(statusBadge({ status }), className)}>
	{#if status === 'running'}
		<span class="size-1.5 animate-pulse rounded-full bg-current"></span>
	{/if}
	{status}
</span>
