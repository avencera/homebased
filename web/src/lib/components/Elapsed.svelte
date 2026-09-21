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

	// one timer per instance. pause it once `to` is a real instant so a finished
	// task does not keep ticking
	let now = $state(Date.now());
	const clock = useInterval(() => 1000, {
		immediate: false,
		callback: () => (now = Date.now())
	});
	const closedEnd = $derived(epochMs(to));

	$effect(() => {
		if (closedEnd === null) clock.resume();
		else clock.pause();
	});

	const start = $derived(epochMs(from));
	const end = $derived(closedEnd ?? now);
	const text = $derived(start === null ? EM_DASH : formatDuration(end - start));
</script>

<span class={cn('tabular-nums', className)} title={formatTimestamp(from)}>
	{text}{suffix && start !== null ? ` ${suffix}` : ''}
</span>
