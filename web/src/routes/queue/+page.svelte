<script lang="ts">
	import { resolve } from '$app/paths';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import X from '@lucide/svelte/icons/x';
	import Capsule from '$lib/components/Capsule.svelte';
	import Elapsed from '$lib/components/Elapsed.svelte';
	import MachineQueue from '$lib/components/MachineQueue.svelte';
	import QueueBadge from '$lib/components/QueueBadge.svelte';
	import { peerQueueHref } from '$lib/api';
	import { machineHue } from '$lib/colors';
	import { QueueControl, QueueStore } from '$lib/daemon.svelte';
	import { queueHasWork } from '$lib/queue-view';

	const store = new QueueStore();
	const control = new QueueControl(() => store.refresh());

	const queueByMachine = $derived(new Map(store.queues.map((queue) => [queue.machine, queue])));
	// machines with a held resource or a waiting job get a full card; the rest share one quiet list
	const busy = $derived(
		store.machines.flatMap((machine) => {
			const queue = queueByMachine.get(machine.machine);
			return queue && queueHasWork(queue.resources, queue.jobs) ? [{ machine, queue }] : [];
		})
	);
	const quiet = $derived(
		store.machines.filter((machine) => !busy.some((entry) => entry.machine === machine))
	);
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

	<div class="mt-4 flex flex-col gap-6">
		{#each busy as { machine, queue } (machine.machine)}
			<MachineQueue {machine} {queue} jobName={(id) => store.jobName(id)} {control} />
		{/each}

		{#if quiet.length > 0}
			<section aria-label="Idle machines">
				<h2 class="mb-2 text-[11px] tracking-wide text-muted-foreground uppercase">
					{busy.length > 0 ? 'Idle' : 'All idle'}
				</h2>
				<ul class="divide-y divide-border/70 rounded-lg border border-border bg-card">
					{#each quiet as machine (machine.machine)}
						{@const queue = queueByMachine.get(machine.machine)}
						{@const queueHref = peerQueueHref(machine)}
						<li class="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2">
							<Capsule hue={machineHue(machine.name)} dot class="w-20 justify-start">
								{machine.name}
							</Capsule>
							{#if queue}
								{#each queue.resources as record (record.resource.id)}
									<span class="inline-flex items-center gap-1.5">
										<span class="font-mono text-muted-foreground">{record.resource.name}</span>
										<QueueBadge state="idle" />
									</span>
								{:else}
									<span class="text-muted-foreground">no resources</span>
								{/each}
							{:else if machine.read.state === 'unavailable'}
								<span class="text-amber-700 dark:text-amber-300">unavailable</span>
								<span class="min-w-0 truncate text-muted-foreground" title={machine.read.message}>
									{machine.read.message}
								</span>
							{/if}
							{#if queueHref}
								<a
									href={queueHref}
									rel="external"
									class="ml-auto text-[11px] text-muted-foreground hover:text-primary"
								>
									open on {machine.name}
								</a>
							{/if}
						</li>
					{/each}
				</ul>
			</section>
		{/if}
	</div>
</div>
