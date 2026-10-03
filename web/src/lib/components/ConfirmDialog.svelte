<script lang="ts">
	import type { Snippet } from 'svelte';
	import { AlertDialog } from 'bits-ui';
	import { cn } from '$lib/utils';

	interface Props {
		/** Open state; the owner opens it, and the dialog closes itself. */
		open: boolean;
		title: string;
		/** Label of the confirming button, naming the action. */
		confirmLabel: string;
		/** Label of the button that closes without acting. */
		cancelLabel?: string;
		onConfirm: () => void;
		children: Snippet;
	}

	let {
		open = $bindable(),
		title,
		confirmLabel,
		cancelLabel = 'Keep',
		onConfirm,
		children
	}: Props = $props();

	function confirm() {
		open = false;
		onConfirm();
	}

	const buttonClass = 'rounded border px-2.5 py-1 text-[13px] leading-5';
</script>

<AlertDialog.Root bind:open>
	<AlertDialog.Portal>
		<AlertDialog.Overlay class="fixed inset-0 z-50 bg-black/30 dark:bg-black/50" />
		<AlertDialog.Content
			class="fixed top-1/2 left-1/2 z-50 flex w-[min(26rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col gap-2 rounded-lg border border-border bg-card p-4 text-[13px] text-foreground shadow-lg"
		>
			<AlertDialog.Title class="text-sm font-semibold">{title}</AlertDialog.Title>
			<AlertDialog.Description class="text-muted-foreground">
				{@render children()}
			</AlertDialog.Description>
			<div class="mt-2 flex justify-end gap-2">
				<AlertDialog.Cancel class={cn(buttonClass, 'border-border hover:bg-accent')}>
					{cancelLabel}
				</AlertDialog.Cancel>
				<AlertDialog.Action
					onclick={confirm}
					class={cn(
						buttonClass,
						'border-red-500/50 bg-red-500/10 font-medium text-red-700 hover:bg-red-500/20 dark:text-red-300'
					)}
				>
					{confirmLabel}
				</AlertDialog.Action>
			</div>
		</AlertDialog.Content>
	</AlertDialog.Portal>
</AlertDialog.Root>
