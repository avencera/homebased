<script lang="ts">
	import { resolve } from '$app/paths';
	import { resolvePath } from '$lib/api';
	import { candidatePaths, splitCommandPaths } from '$lib/command-paths';
	import { cn } from '$lib/utils';

	interface Props {
		/** Arguments the daemon spawned, one per line. */
		argv: readonly string[];
		/** Directory the command ran in, for relative paths. */
		cwd: string;
		class?: string;
	}

	let { argv, cwd, class: className }: Props = $props();

	const lines = $derived(argv.map((argument) => splitCommandPaths(argument, cwd)));
	// a joined key keeps the task poll from re-checking paths when argv is unchanged
	const candidateKey = $derived(candidatePaths(lines.flat()).join('\0'));
	let existing = $state<ReadonlySet<string>>(new Set());

	$effect(() => {
		const paths = candidateKey === '' ? [] : candidateKey.split('\0');
		let stale = false;
		void Promise.all(
			paths.map((path) =>
				resolvePath(path).then(
					() => path,
					() => null
				)
			)
		).then((found) => {
			if (stale) return;
			existing = new Set(found.filter((path) => path !== null));
		});
		return () => {
			stale = true;
		};
	});
</script>

<div class={cn('font-mono wrap-anywhere', className)}>
	{#each lines as pieces, index (index)}
		<div class="whitespace-pre-wrap">
			{#each pieces as piece, pieceIndex (pieceIndex)}
				{#if piece.type === 'path' && existing.has(piece.path)}
					<a
						href={resolve(`/files?path=${encodeURIComponent(piece.path)}`)}
						class="text-primary hover:underline"
						title={piece.path}>{piece.text}</a
					>
				{:else}
					{piece.text}
				{/if}
			{/each}
		</div>
	{/each}
</div>
