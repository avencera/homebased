<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import X from '@lucide/svelte/icons/x';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import MachineQueue from '$lib/components/MachineQueue.svelte';
	import { QueueControl, QueueStore } from '$lib/daemon.svelte';

	const store = new QueueStore();
	const control = new QueueControl(() => store.refresh());

	const queueByMachine = $derived(new Map(store.queues.map((queue) => [queue.machine, queue])));
</script>

<div class="mx-auto max-w-page px-4 py-4 lg:px-6">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			all workers
		</a>
		<h1 class="text-base font-semibold tracking-tight">GPU queues</h1>
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				updated <Elapsed from={store.lastFetched} suffix="ago" />
			{:else}
				loading
			{/if}
		</span>
	</header>

	{#each [store.error, control.error] as error, index (index)}
		{#if error}
			<p
				class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
				role="alert"
			>
				<CircleAlert class="mt-0.5 size-4 shrink-0" />
				<span class="min-w-0 flex-1 wrap-anywhere">{error.message}</span>
				{#if index === 1}
					<button
						type="button"
						class="shrink-0 rounded p-0.5 hover:bg-red-500/10"
						aria-label="Dismiss"
						onclick={() => control.dismiss()}
					>
						<X class="size-3.5" aria-hidden="true" />
					</button>
				{/if}
			</p>
		{/if}
	{/each}

	<div class="mt-3 flex flex-col gap-3">
		{#each store.machines as machine (machine.machine)}
			<MachineQueue
				{machine}
				queue={queueByMachine.get(machine.machine)}
				jobName={(id) => store.jobName(id)}
				{control}
			/>
		{/each}
	</div>
</div>
