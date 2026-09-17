<script lang="ts" module>
	import { tv } from 'tailwind-variants';

	/** Badge classes per `CallbackStatus`. */
	export const callbackBadge = tv({
		base: 'inline-flex items-center rounded px-1.5 py-0.5 font-mono text-[11px] leading-4 ring-1 ring-inset',
		variants: {
			callback: {
				pending: 'bg-transparent text-muted-foreground ring-border',
				sending: 'bg-sky-500/10 text-sky-700 ring-sky-500/30 dark:text-sky-300',
				sent: 'bg-transparent text-emerald-700 ring-emerald-500/30 dark:text-emerald-300',
				failed: 'bg-red-500/10 text-red-700 ring-red-500/40 dark:text-red-300'
			}
		}
	});
</script>

<script lang="ts">
	import type { CallbackStatus } from '$lib/api';
	import { cn } from '$lib/utils';

	interface Props {
		callback: CallbackStatus;
		class?: string;
	}

	let { callback, class: className }: Props = $props();
</script>

<span class={cn(callbackBadge({ callback }), className)} title="callback delivery">
	{callback}
</span>
