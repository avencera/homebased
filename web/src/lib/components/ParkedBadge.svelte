<script lang="ts">
	import { isParked, type TaskSummary } from '$lib/api';

	interface Props {
		task: TaskSummary;
	}

	let { task }: Props = $props();

	const waiting = $derived(task.chain?.state === 'waiting' ? task.chain : null);
</script>

{#if waiting && isParked(task)}
	<span
		class="inline-flex items-center rounded bg-violet-500/10 px-1.5 py-0.5 font-mono text-[11px] leading-4 text-violet-700 ring-1 ring-violet-500/40 ring-inset dark:text-violet-300"
		title={`Waiting on ${waiting.on.join(', ')}; continues as ${waiting.continuation}`}
	>
		parked
	</span>
{/if}
