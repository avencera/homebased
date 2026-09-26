<script lang="ts">
	import { resolve } from '$app/paths';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import { ResourceOverviewStore } from '$lib/daemon.svelte';
	import { formatCentralTimestamp, shortId } from '$lib/format';
	import {
		overviewStatus,
		resourceTaskThread,
		stringField,
		type ResourceStatus
	} from '$lib/resource-state';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import ThreadLabel from '$lib/components/ThreadLabel.svelte';
	import { ThreadTitleStore } from '$lib/thread-titles.svelte';

	const store = new ResourceOverviewStore();
	const threadTitles = new ThreadTitleStore(() =>
		(store.overview?.resources ?? []).flatMap((item) => {
			const current =
				item.current_task && resourceTaskThread(item.current_task, item.resource.authority_machine);
			return current ? [item.resource.supervisor, current] : [item.resource.supervisor];
		})
	);
	const readFailureStatus = $derived.by((): ResourceStatus | null => {
		if (!store.error) return null;
		const unavailable = store.error.httpStatus === null || store.error.httpStatus >= 500;
		return {
			label: unavailable ? 'Authority unavailable' : 'Unknown',
			message: `Latest resource read failed: ${store.error.message}`,
			tone: unavailable ? 'red' : 'amber'
		};
	});

	function toneClass(tone: 'green' | 'blue' | 'amber' | 'red' | 'neutral'): string {
		switch (tone) {
			case 'green':
				return 'bg-emerald-500/10 text-emerald-700 ring-emerald-500/40 dark:text-emerald-300';
			case 'blue':
				return 'bg-sky-500/10 text-sky-700 ring-sky-500/40 dark:text-sky-300';
			case 'amber':
				return 'bg-amber-500/10 text-amber-700 ring-amber-500/40 dark:text-amber-300';
			case 'red':
				return 'bg-red-500/10 text-red-700 ring-red-500/40 dark:text-red-300';
			case 'neutral':
				return 'bg-slate-500/10 text-slate-700 ring-slate-500/30 dark:text-slate-300';
		}
	}

	function authorityIssueText(issue: Record<string, unknown>): string {
		return (
			stringField(issue, 'message') ??
			stringField(issue, 'reason') ??
			stringField(issue, 'error') ??
			stringField(issue, 'authority_machine') ??
			'Authority unavailable'
		);
	}

	function authorityIssueKey(issue: Record<string, unknown>, index: number): string {
		return (
			stringField(issue, 'resource_id') ??
			stringField(issue, 'authority_machine') ??
			stringField(issue, 'machine') ??
			String(index)
		);
	}
</script>

<div class="mx-auto max-w-7xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="text-primary hover:underline">tasks</a>
		<h1 class="text-base font-semibold tracking-tight">resources</h1>
		{#if readFailureStatus}
			<span
				class="inline-flex items-center rounded px-1.5 py-0.5 font-mono text-[11px] ring-1 ring-inset {toneClass(
					readFailureStatus.tone
				)}"
			>
				{readFailureStatus.label}
			</span>
		{/if}
		<span class="text-muted-foreground">GPU queue and return state</span>
		<span class="ml-auto text-muted-foreground">
			{#if store.lastFetched}
				{store.error ? 'read failed' : 'updated'} {formatCentralTimestamp(store.lastFetched)} CT
			{:else}
				loading
			{/if}
		</span>
	</header>

	{#if store.error}
		<p
			role="alert"
			class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
		>
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span>
				<span class="font-mono">{store.error.code}</span>
				&mdash; {store.error.message}. Resource state is not available.
			</span>
		</p>
	{/if}

	{#if store.overview && store.overview.unavailable_authorities.length > 0}
		<aside
			class="mt-3 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
		>
			<h2 class="font-semibold">Authority unavailable</h2>
			<p class="mt-0.5 text-muted-foreground">
				The daemon cannot confirm every resource authority. Affected resources do not show as
				available.
			</p>
			<ul class="mt-2 space-y-1">
				{#each store.overview.unavailable_authorities as authority, index (authorityIssueKey(authority, index))}
					<li class="font-mono text-[12px] break-words">{authorityIssueText(authority)}</li>
				{/each}
			</ul>
		</aside>
	{/if}

	{#if store.overview}
		{#if store.overview.resources.length === 0}
			<p
				class="mt-3 rounded border border-border bg-card px-3 py-6 text-center text-muted-foreground"
			>
				No resources are registered
			</p>
		{:else}
			<section class="mt-3 grid gap-3 lg:grid-cols-2">
				{#each store.overview.resources as item (item.resource.id)}
					{@const status =
						readFailureStatus ?? overviewStatus(item, store.overview.unavailable_authorities)}
					<article class="rounded border border-border bg-card p-3">
						<div class="flex flex-wrap items-start justify-between gap-2">
							<div class="min-w-0">
								<h2 class="text-sm font-semibold break-words">{item.resource.display_name}</h2>
								<p class="mt-0.5 flex flex-wrap items-center gap-x-2 text-muted-foreground">
									<span>resource</span>
									<CopyPath value={item.resource.id} label={shortId(item.resource.id)} />
									<span>&middot; authority {shortId(item.resource.authority_machine)}</span>
								</p>
							</div>
							<span
								class="inline-flex shrink-0 items-center rounded px-1.5 py-0.5 font-mono text-[11px] ring-1 ring-inset {toneClass(
									status.tone
								)}"
							>
								{status.label}
							</span>
						</div>

						<p class="mt-2 text-muted-foreground">{status.message}</p>

						<div class="mt-3 grid gap-x-3 gap-y-1 sm:grid-cols-[7rem_minmax(0,1fr)]">
							<span class="text-muted-foreground">current task</span>
							{#if item.current_task}
								{@const currentThread = resourceTaskThread(
									item.current_task,
									item.resource.authority_machine
								)}
								<span class="flex min-w-0 flex-col">
									<a
										href={resolve('/tasks/[id]', { id: item.current_task.id })}
										class="truncate text-primary hover:underline"
										title={item.current_task.display_name}
									>
										{item.current_task.display_name}
									</a>
									{#if currentThread}
										<ThreadLabel
											thread={currentThread.thread}
											title={threadTitles.title(currentThread)}
											filterLink
											class="text-muted-foreground"
										/>
									{/if}
								</span>
							{:else if item.resource.registered_background_task}
								<a
									href={resolve('/tasks/[id]', {
										id: item.resource.registered_background_task
									})}
									class="truncate text-primary hover:underline"
								>
									registered training task {shortId(item.resource.registered_background_task)}
								</a>
							{:else}
								<span class="text-muted-foreground">none reported</span>
							{/if}

							<span class="text-muted-foreground">ready queue</span>
							<span>
								{item.queued_count} waiting
								{#if item.queued_count > 0}
									<span class="text-muted-foreground">&middot; serving order</span>
								{/if}
							</span>

							<span class="text-muted-foreground">supervisor</span>
							<span class="flex min-w-0 items-center gap-1">
								<span class="truncate">machine {shortId(item.resource.supervisor.machine)}</span>
								{#if threadTitles.title(item.resource.supervisor)}
									<span class="truncate" title={threadTitles.title(item.resource.supervisor)}>
										&middot; {threadTitles.title(item.resource.supervisor)}
									</span>
								{/if}
								<CopyPath
									value={item.resource.supervisor.thread}
									label={`thread ${shortId(item.resource.supervisor.thread)}`}
								/>
							</span>
						</div>

						{#if item.attention}
							<p class="mt-2 break-words text-amber-700 dark:text-amber-300">
								{item.attention.message}
							</p>
						{/if}

						<a
							href={resolve('/resources/[id]', { id: item.resource.id })}
							class="mt-3 inline-flex rounded border border-border px-2 py-1 text-primary hover:bg-accent"
						>
							View resource
						</a>
					</article>
				{/each}
			</section>
		{/if}
	{:else if !store.error}
		<p
			class="mt-3 rounded border border-border bg-card px-3 py-6 text-center text-muted-foreground"
		>
			Loading resource state&hellip;
		</p>
	{/if}
</div>
