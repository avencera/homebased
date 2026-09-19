<script lang="ts">
	import type { WorkloadView } from '$lib/api';
	import { formatWorkload } from '$lib/format';
	import { cn } from '$lib/utils';

	interface Props {
		workload: WorkloadView;
		class?: string;
	}

	let { workload, class: className }: Props = $props();

	// the badge names the selected model because it is the useful agent identity
	const label = $derived(workload.type === 'agent' ? (workload.model ?? workload.agent) : 'cmd');
	const detail = $derived.by(() => {
		if (workload.type === 'agent') return null;
		return workload.command.length > 0 ? formatWorkload(workload) : null;
	});
	const title = $derived.by(() => {
		if (workload.type === 'agent') {
			return workload.model
				? `agent ${workload.agent}, model ${workload.model}`
				: `agent ${workload.agent}, default model`;
		}
		return `command: ${workload.command.join(' ')}`;
	});
</script>

<span class={cn('flex min-w-0 items-center gap-1.5', className)} {title}>
	<span
		class="inline-flex shrink-0 items-center rounded px-1.5 py-0.5 font-mono text-[11px] leading-4 text-foreground ring-1 ring-border ring-inset"
	>
		{label}
	</span>
	{#if detail}
		<span class="truncate font-mono text-[11px] text-muted-foreground">{detail}</span>
	{/if}
</span>
