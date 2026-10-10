<script lang="ts">
	import { resolve } from '$app/paths';
	import Menu from '@lucide/svelte/icons/menu';
	import { DropdownMenu } from 'bits-ui';

	const links = [
		{ href: resolve('/queue'), label: 'queue' },
		{ href: resolve('/files'), label: 'files' },
		{ href: resolve('/threads'), label: 'threads' },
		{ href: resolve('/usage'), label: 'usage' }
	];
</script>

<!-- the links wrap the header onto extra lines on a phone, so narrow screens get a menu -->
<nav class="hidden items-baseline gap-x-3 sm:flex sm:gap-x-4" aria-label="Pages">
	{#each links as link (link.href)}
		<a href={link.href} class="text-primary hover:underline">{link.label}</a>
	{/each}
</nav>

<DropdownMenu.Root>
	<DropdownMenu.Trigger
		class="order-last -my-1 self-center rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground sm:hidden"
		aria-label="Pages"
	>
		<Menu class="size-4" />
	</DropdownMenu.Trigger>
	<DropdownMenu.Portal>
		<DropdownMenu.Content
			align="end"
			sideOffset={4}
			class="z-50 min-w-32 rounded-lg border border-border bg-card p-1 text-[13px] text-foreground shadow-lg"
		>
			{#each links as link (link.href)}
				<DropdownMenu.Item>
					{#snippet child({ props })}
						<a
							{...props}
							href={link.href}
							class="block rounded px-2.5 py-1.5 text-primary outline-none data-highlighted:bg-accent"
						>
							{link.label}
						</a>
					{/snippet}
				</DropdownMenu.Item>
			{/each}
		</DropdownMenu.Content>
	</DropdownMenu.Portal>
</DropdownMenu.Root>
