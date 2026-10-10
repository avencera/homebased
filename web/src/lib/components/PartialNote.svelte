<script lang="ts">
	import { Popover } from 'bits-ui';

	interface Props {
		/** Why the number before the asterisk is an undercount. */
		text: string;
	}

	let { text }: Props = $props();

	let open = $state(false);
	// a mouse opens the note on hover, so a click while hovering must not toggle it shut;
	// touch has no hover, so a tap toggles it and a tap elsewhere closes it
	let hovering = $state(false);

	function setOpen(next: boolean) {
		if (!next && hovering) return;
		open = next;
	}

	function enter(event: PointerEvent) {
		if (event.pointerType !== 'mouse') return;
		hovering = true;
		open = true;
	}

	function leave(event: PointerEvent) {
		if (event.pointerType !== 'mouse') return;
		hovering = false;
		open = false;
	}
</script>

<Popover.Root bind:open={() => open, setOpen}>
	<Popover.Trigger
		onpointerenter={enter}
		onpointerleave={leave}
		class="ml-0.5 cursor-help font-sans text-amber-700 hover:text-amber-600 focus-visible:outline-2 focus-visible:outline-primary dark:text-amber-300"
		aria-label="Undercount: {text}"
	>
		*
	</Popover.Trigger>
	<Popover.Portal>
		<Popover.Content
			side="top"
			sideOffset={4}
			collisionPadding={8}
			trapFocus={false}
			onOpenAutoFocus={(event) => event.preventDefault()}
			onCloseAutoFocus={(event) => event.preventDefault()}
			class="z-50 max-w-[min(18rem,calc(100vw-1rem))] rounded-lg border border-border bg-card px-3 py-2 text-left font-sans text-[12px] whitespace-normal text-foreground shadow-lg"
		>
			{text}
		</Popover.Content>
	</Popover.Portal>
</Popover.Root>
