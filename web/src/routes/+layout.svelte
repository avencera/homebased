<script lang="ts">
	import './layout.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>homebased</title>
</svelte:head>

<div class="min-h-screen bg-background text-[13px] text-foreground">
	{@render children?.()}
</div>
