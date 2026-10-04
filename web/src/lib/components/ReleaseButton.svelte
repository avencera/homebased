<script lang="ts">
	import { releaseAttention } from '$lib/api';
	import type { QueueControl } from '$lib/daemon.svelte';
	import ConfirmDialog from './ConfirmDialog.svelte';

	interface Props {
		/** Attention the release names, so a stale click cannot clear a later one. */
		attention: string;
		/** Machine whose resource holds it. */
		machine: string;
		resourceName: string;
		control: QueueControl;
	}

	let { attention, machine, resourceName, control }: Props = $props();

	let confirming = $state(false);

	function release() {
		void control.run(attention, (operationId) => releaseAttention(attention, machine, operationId));
	}
</script>

<button
	type="button"
	disabled={control.pending !== null}
	onclick={() => (confirming = true)}
	class="rounded border border-red-500/50 px-2 py-0.5 text-[11px] leading-5 font-medium text-red-700 hover:bg-red-500/10 focus-visible:outline-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:opacity-40 dark:text-red-300"
>
	Release
</button>

<ConfirmDialog
	bind:open={confirming}
	title={`Release ${resourceName}?`}
	confirmLabel={`Release ${resourceName}`}
	onConfirm={release}
>
	Cleanup could not confirm that the run's processes are gone. Check the machine and stop anything
	the run left behind first. The next job starts on {resourceName} as soon as it is released.
</ConfirmDialog>
