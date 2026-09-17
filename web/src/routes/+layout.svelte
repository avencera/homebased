<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';

	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>homebased</title>
</svelte:head>

<div class="min-h-screen bg-background text-[13px] text-foreground">
	{@render children?.()}
</div>
