<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import ArrowUp from '@lucide/svelte/icons/arrow-up';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import FileIcon from '@lucide/svelte/icons/file';
	import Folder from '@lucide/svelte/icons/folder';
	import Link2 from '@lucide/svelte/icons/link-2';
	import {
		ApiError,
		asApiError,
		contentUrlForPath,
		contentUrlForToken,
		fetchContentOrigin,
		fetchDirectory,
		resolvePath,
		type DirectoryListing,
		type FileEntry,
		type FileEntryKind
	} from '$lib/api';
	import { EM_DASH, formatTimestamp, shortenHome } from '$lib/format';
	import { cn } from '$lib/utils';

	let listing = $state<DirectoryListing | null>(null);
	let error = $state<ApiError | null>(null);
	let pathInput = $state('/');
	let contentPort = $state<number | null>(null);
	let loading = $state(false);
	// a slow listing must not replace a newer navigation
	let loadGeneration = 0;

	const token = $derived(page.url.searchParams.get('token'));
	const entryPath = $derived(page.url.searchParams.get('path'));

	$effect(() => {
		const currentToken = token;
		const currentPath = entryPath;
		const generation = ++loadGeneration;
		void load(generation, currentToken, currentPath);
	});

	async function ensureOrigin(): Promise<number> {
		if (contentPort !== null) return contentPort;
		const origin = await fetchContentOrigin();
		contentPort = origin.port;
		return origin.port;
	}

	async function load(generation: number, currentToken: string | null, currentPath: string | null) {
		loading = true;
		error = null;
		try {
			if (currentToken) {
				const dir = await fetchDirectory(currentToken);
				if (generation !== loadGeneration) return;
				listing = dir;
				pathInput = dir.path;
				return;
			}
			if (currentPath) {
				const resolved = await resolvePath(currentPath);
				if (generation !== loadGeneration) return;
				pathInput = resolved.resolved ?? resolved.requested;
				if (resolved.kind === 'directory') {
					await goto(resolve(`/files?token=${encodeURIComponent(resolved.token)}`), {
						replaceState: true,
						keepFocus: true,
						noScroll: true
					});
					return;
				}
				const port = await ensureOrigin();
				if (generation !== loadGeneration) return;
				const url = resolved.content_path
					? contentUrlForPath(port, resolved.content_path)
					: contentUrlForToken(port, resolved.token);
				window.location.replace(url);
				return;
			}
			const resolved = await resolvePath('/');
			if (generation !== loadGeneration) return;
			await goto(resolve(`/files?token=${encodeURIComponent(resolved.token)}`), {
				replaceState: true,
				keepFocus: true,
				noScroll: true
			});
		} catch (cause) {
			if (generation !== loadGeneration) return;
			listing = null;
			error = asApiError(cause);
		} finally {
			if (generation === loadGeneration) loading = false;
		}
	}

	async function goToPath(raw: string) {
		const path = raw || '/';
		await goto(resolve(`/files?path=${encodeURIComponent(path)}`));
	}

	async function openEntry(entry: FileEntry) {
		const effectiveKind = entry.kind === 'symlink' ? entry.target_kind : entry.kind;
		if (effectiveKind === 'directory') {
			await goto(resolve(`/files?token=${encodeURIComponent(entry.token)}`));
			return;
		}
		if (effectiveKind !== 'file') {
			error = new ApiError(
				{
					code: 'unsupported_file',
					message: `${entry.name} is not a regular file or directory`,
					retryable: false,
					input: {}
				},
				400
			);
			return;
		}
		const opened = window.open('about:blank', '_blank');
		if (opened) opened.opener = null;
		try {
			const port = await ensureOrigin();
			const url = entry.content_path
				? contentUrlForPath(port, entry.content_path)
				: contentUrlForToken(port, entry.token);
			if (opened) opened.location.replace(url);
			else window.location.assign(url);
		} catch (cause) {
			opened?.close();
			error = asApiError(cause);
		}
	}

	async function goParent() {
		if (!listing?.parent) return;
		await goto(resolve(`/files?token=${encodeURIComponent(listing.parent)}`));
	}

	function kindIcon(kind: FileEntryKind) {
		switch (kind) {
			case 'directory':
				return Folder;
			case 'symlink':
				return Link2;
			default:
				return FileIcon;
		}
	}

	function formatSize(size: number | null | undefined): string {
		if (size === null || size === undefined) return EM_DASH;
		if (size < 1024) return `${size} B`;
		if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KiB`;
		return `${(size / (1024 * 1024)).toFixed(1)} MiB`;
	}
</script>

<div class="mx-auto max-w-5xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a href={resolve('/')} class="inline-flex items-center gap-1 text-primary hover:underline">
			<ArrowLeft class="size-3.5" />
			tasks
		</a>
		<h1 class="text-base font-semibold tracking-tight">files</h1>
		<span class="text-muted-foreground">device-wide read-only</span>
	</header>

	<form
		class="mt-3 flex flex-wrap gap-2"
		onsubmit={(event) => {
			event.preventDefault();
			void goToPath(pathInput);
		}}
	>
		<input
			bind:value={pathInput}
			type="text"
			spellcheck="false"
			aria-label="absolute path"
			placeholder="/absolute/path"
			class="min-w-[16rem] flex-1 rounded border border-border bg-card px-2 py-1 font-mono"
		/>
		<button
			type="submit"
			class="rounded border border-border px-2 py-1 text-muted-foreground hover:bg-accent"
		>
			go
		</button>
		<button
			type="button"
			onclick={() => void goParent()}
			disabled={!listing?.parent}
			class="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-muted-foreground hover:bg-accent disabled:opacity-40"
		>
			<ArrowUp class="size-3.5" />
			parent
		</button>
	</form>

	{#if error}
		<p
			class="mt-3 flex items-start gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
		>
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span>
				<span class="font-mono">{error.code}</span>
				&mdash; {error.message}
			</span>
		</p>
	{/if}

	{#if listing}
		<p class="mt-3 truncate font-mono text-muted-foreground" title={listing.path}>
			{shortenHome(listing.path)}
		</p>
		<div class="mt-2 overflow-x-auto rounded border border-border bg-card">
			<div
				class="grid grid-cols-[minmax(12rem,1fr)_5rem_5rem_9rem] gap-2 border-b border-border bg-muted px-3 py-1.5 text-[11px] tracking-wide text-muted-foreground uppercase"
			>
				<span>name</span>
				<span>kind</span>
				<span>size</span>
				<span>modified</span>
			</div>
			{#each listing.entries as entry (entry.token)}
				{@const Icon = kindIcon(entry.kind)}
				<button
					type="button"
					onclick={() => void openEntry(entry)}
					class={cn(
						'grid w-full grid-cols-[minmax(12rem,1fr)_5rem_5rem_9rem] items-center gap-2 border-b border-border/60 px-3 py-1.5 text-left last:border-b-0 hover:bg-accent/60',
						entry.kind === 'other' && 'text-muted-foreground'
					)}
				>
					<span class="flex min-w-0 items-center gap-1.5 font-mono">
						<Icon class="size-3.5 shrink-0 opacity-70" />
						<span class="truncate" title={entry.name}>{entry.name}</span>
					</span>
					<span class="font-mono text-[11px] text-muted-foreground">{entry.kind}</span>
					<span class="font-mono text-[11px] text-muted-foreground tabular-nums">
						{formatSize(entry.size)}
					</span>
					<span class="font-mono text-[11px] text-muted-foreground">
						{entry.modified ? formatTimestamp(entry.modified) : EM_DASH}
					</span>
				</button>
			{/each}
			{#if listing.entries.length === 0 && !loading}
				<p class="px-3 py-6 text-center text-muted-foreground">Empty directory</p>
			{/if}
		</div>
	{:else if loading}
		<p class="mt-6 text-center text-muted-foreground">loading</p>
	{/if}
</div>
