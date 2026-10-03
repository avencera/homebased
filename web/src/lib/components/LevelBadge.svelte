<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	/** Level colors, warmer as the level rises. */
	const levelTone = tv({
		variants: {
			priority: {
				high: 'text-rose-700 dark:text-rose-300',
				medium: 'text-amber-700 dark:text-amber-300',
				low: 'text-slate-600 dark:text-slate-400'
			}
		}
	});

	/** Rail color per level. */
	export const levelRail = tv({
		variants: {
			priority: {
				high: 'bg-rose-500',
				medium: 'bg-amber-500',
				low: 'bg-slate-400 dark:bg-slate-500'
			}
		}
	});
</script>

<script lang="ts">
	import type { Priority } from '$lib/api';
	import { cn } from '$lib/utils';

	interface Props {
		priority: Priority;
		class?: string;
	}

	let { priority, class: className }: Props = $props();
</script>

<span class={cn('font-mono text-[11px] leading-4 font-medium', levelTone({ priority }), className)}>
	{priority}
</span>
