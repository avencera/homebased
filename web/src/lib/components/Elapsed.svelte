<script lang="ts">
	import { useInterval } from 'runed';
	import { EM_DASH, epochMs, formatDuration, formatTimestamp, type Instant } from '$lib/format';
	import { cn } from '$lib/utils';

	interface Props {
		/** Start of the interval. */
		from: Instant | null | undefined;
		/** End of the interval. Leave unset to measure against the current time. */
		to?: Instant | null;
		/** Word placed after the duration, such as `ago`. */
		suffix?: string;
		class?: string;
	}

	let { from, to = null, suffix, class: className }: Props = $props();

	// One timer per instance. The clock is read only while the interval is open
	// ended, so a finished task pays nothing for the tick.
	let now = $state(Date.now());
	useInterval(() => 1000, { callback: () => (now = Date.now()) });

	const start = $derived(epochMs(from));
	const end = $derived(epochMs(to) ?? now);
	const text = $derived(start === null ? EM_DASH : formatDuration(end - start));
</script>

<span class={cn('tabular-nums', className)} title={formatTimestamp(from)}>
	{text}{suffix && start !== null ? ` ${suffix}` : ''}
</span>
