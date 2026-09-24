<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { page } from '$app/state';
	import { Schema } from 'effect';
	import ArrowLeft from '@lucide/svelte/icons/arrow-left';
	import CircleAlert from '@lucide/svelte/icons/circle-alert';
	import RefreshCw from '@lucide/svelte/icons/refresh-cw';
	import { ApiError, asApiError } from '$lib/api';
	import CopyPath from '$lib/components/CopyPath.svelte';
	import { ResourceDetailStore } from '$lib/daemon.svelte';
	import { formatCentralTimestamp, shortId } from '$lib/format';
	import {
		asRecord,
		backgroundLaunchText,
		currentRequestId,
		detailStatus,
		loanActionId,
		loanPhase,
		loanReleaseProvenance,
		loanReturnContext,
		numberField,
		stringField,
		tagOf,
		type ResourceStatus
	} from '$lib/resource-state';
	import {
		submitResourceAction,
		type BrowserResourceAction,
		type PendingAction,
		type ResourceDetail,
		type ResourceTaskSummary
	} from '$lib/resources';

	interface PendingOperation {
		resourceId: string;
		operationId: string;
		expectedRevision: number;
		action: BrowserResourceAction;
	}

	type OperationFailure =
		{ type: 'unknown' } | { type: 'stale_revision' } | { type: 'definitive_refusal' };

	const StoredOperationSchema = Schema.Struct({
		resourceId: Schema.String,
		operationId: Schema.String,
		expectedRevision: Schema.Finite,
		action: Schema.Union(
			Schema.Struct({ type: Schema.Literal('cancel_queued'), request_id: Schema.String }),
			Schema.Struct({ type: Schema.Literal('stop_active'), task_id: Schema.String }),
			Schema.Struct({ type: Schema.Literal('renotify'), notice_id: Schema.String })
		)
	});

	const store = new ResourceDetailStore(() => page.params.id ?? '');
	const detail = $derived(store.detail);
	const status = $derived.by((): ResourceStatus | null => {
		if (!detail) return null;
		if (!store.error) return detailStatus(detail);
		const unavailable = store.error.httpStatus === null || store.error.httpStatus >= 500;
		return {
			label: unavailable ? 'Authority unavailable' : 'Unknown',
			message: `Latest resource read failed: ${store.error.message}`,
			tone: unavailable ? 'red' : 'amber'
		};
	});
	const phase = $derived(loanPhase(detail?.loan ?? null));
	const phaseType = $derived(tagOf(phase));
	const loanStateType = $derived(tagOf(detail?.loan?.state));
	const returnContext = $derived(loanReturnContext(detail?.loan ?? null));
	const releaseProvenance = $derived(loanReleaseProvenance(detail?.loan ?? null));
	const operatorRelease = $derived(operatorReleaseDetails(releaseProvenance));
	const currentRequestIdValue = $derived(currentRequestId(detail?.loan ?? null));
	const queuedRequests = $derived(
		detail?.requests.filter((request) => tagOf(request.state) === 'queued') ?? []
	);
	const otherRequests = $derived(
		detail?.requests.filter((request) => tagOf(request.state) !== 'queued') ?? []
	);
	const currentRequest = $derived(
		detail?.requests.find((request) => request.request_id === currentRequestIdValue) ?? null
	);
	const stoppableTask = $derived(
		phaseType === 'serving' && currentRequest && detail?.current_task?.id === currentRequest.task_id
			? detail.current_task
			: null
	);
	const stopPermitted = $derived(
		stoppableTask !== null &&
			['queued', 'running'].includes(stoppableTask.status) &&
			stoppableTask.cancel_requested_at == null
	);

	interface OperatorReleaseDetails {
		operationId: string | null;
		taskId: string | null;
		actionId: string | null;
		idleBoundary: boolean;
	}

	let operation = $state<PendingOperation | null>(null);
	let operationBusy = $state(false);
	let operationMessage = $state<string | null>(null);
	let operationError = $state<string | null>(null);
	let operationResourceId = '';

	$effect(() => loadSavedOperation(page.params.id ?? ''));

	function loadSavedOperation(resourceId: string) {
		if (!resourceId || resourceId === operationResourceId) return;
		operationResourceId = resourceId;
		operation = readOperation(resourceId);
		operationBusy = false;
		operationError = null;
		operationMessage = operation
			? 'An earlier action has no confirmed result. Retry uses the same operation ID.'
			: null;
	}

	function readOperation(resourceId: string): PendingOperation | null {
		try {
			const raw = sessionStorage.getItem(operationStorageKey(resourceId));
			if (!raw) return null;
			const value: unknown = JSON.parse(raw);
			const saved = Schema.decodeUnknownSync(StoredOperationSchema)(value);
			if (saved.resourceId !== resourceId) {
				sessionStorage.removeItem(operationStorageKey(resourceId));
				return null;
			}
			return saved;
		} catch {
			try {
				sessionStorage.removeItem(operationStorageKey(resourceId));
			} catch {
				return null;
			}
			return null;
		}
	}

	function operationStorageKey(resourceId: string): string {
		return `homebased:resource-operation:${resourceId}`;
	}

	async function openTaskLogs(event: MouseEvent, taskId: string) {
		if (
			event.defaultPrevented ||
			event.button !== 0 ||
			event.metaKey ||
			event.ctrlKey ||
			event.shiftKey ||
			event.altKey
		) {
			return;
		}
		event.preventDefault();
		await goto(resolve('/tasks/[id]', { id: taskId }));
		window.location.hash = 'output-log';
	}

	function saveOperation(next: PendingOperation) {
		operation = next;
		try {
			sessionStorage.setItem(operationStorageKey(next.resourceId), JSON.stringify(next));
		} catch {
			operationMessage = 'The operation ID is held in this page but could not be saved for reload.';
		}
	}

	function clearOperation(resourceId: string) {
		operation = null;
		try {
			sessionStorage.removeItem(operationStorageKey(resourceId));
		} catch {
			// the server outcome is known, so unavailable browser storage does not block the UI
		}
	}

	function makeUuid(): string {
		if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
			return crypto.randomUUID();
		}
		const bytes = new Uint8Array(16);
		if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
			crypto.getRandomValues(bytes);
		} else {
			for (let index = 0; index < bytes.length; index += 1) {
				bytes[index] = Math.floor(Math.random() * 256);
			}
		}
		bytes[6] = (bytes[6] & 0x0f) | 0x40;
		bytes[8] = (bytes[8] & 0x3f) | 0x80;
		const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0'));
		return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex
			.slice(6, 8)
			.join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10).join('')}`;
	}

	function actionLabel(action: BrowserResourceAction): string {
		switch (action.type) {
			case 'cancel_queued':
				return 'Cancel queued request';
			case 'stop_active':
				return 'Stop active test';
			case 'renotify':
				return 'Retry failed notification';
		}
	}

	async function beginAction(action: BrowserResourceAction) {
		if (!detail || store.error || operation || operationBusy) return;
		const next = {
			resourceId: detail.resource.id,
			operationId: makeUuid(),
			expectedRevision: detail.resource.state_revision,
			action
		};
		operationError = null;
		operationMessage = null;
		saveOperation(next);
		await sendOperation(next);
	}

	async function retryOperation() {
		if (!detail || !operation || operationBusy) return;
		if (operation.resourceId !== detail.resource.id) return;
		operationError = null;
		operationMessage = null;
		await sendOperation(operation);
	}

	async function sendOperation(pending: PendingOperation) {
		const currentDetail = detail;
		if (!currentDetail || pending.resourceId !== currentDetail.resource.id) return;
		operationBusy = true;
		try {
			const updated = await submitResourceAction(
				pending.resourceId,
				pending.expectedRevision,
				pending.operationId,
				pending.action
			);
			store.showAuthoritative(updated);
			clearOperation(pending.resourceId);
			operationError = null;
			operationMessage = `${actionLabel(pending.action)} returned authoritative resource detail.`;
			await store.refreshPending();
		} catch (cause) {
			const error = asApiError(cause);
			const failure = classifyOperationFailure(error);
			if (failure.type === 'unknown') {
				operationMessage =
					'The outcome is unknown. Details are refreshing; retry will send the same request with the same operation ID.';
				operationError = null;
				await store.refresh();
				if (store.error !== null) {
					operationMessage = `${operationMessage} The latest resource read failed.`;
				}
				return;
			}

			clearOperation(pending.resourceId);
			operationMessage = null;
			await store.refresh();
			const recovery =
				failure.type === 'stale_revision'
					? 'The resource revision changed. Choose the action again to use the refreshed revision.'
					: 'The action was refused. Review the refreshed resource details before choosing an action again.';
			const refreshFailure = store.error
				? ` The detail refresh failed: ${store.error.message}`
				: '';
			operationError = `${error.code}: ${error.message}. ${recovery}${refreshFailure}`;
		} finally {
			operationBusy = false;
		}
	}

	function classifyOperationFailure(error: ApiError): OperationFailure {
		if (error.code === 'resource_stale_revision') return { type: 'stale_revision' };
		if (isUnknownOutcome(error)) return { type: 'unknown' };
		return { type: 'definitive_refusal' };
	}

	function isUnknownOutcome(error: ApiError): boolean {
		return (
			error.httpStatus === null ||
			error.httpStatus >= 500 ||
			error.code === 'invalid_response' ||
			error.code === 'invalid_json' ||
			[
				'resource_outcome_unknown',
				'resource_operation_unavailable',
				'resource_authority_unavailable'
			].includes(error.code)
		);
	}

	function statusToneClass(tone: 'green' | 'blue' | 'amber' | 'red' | 'neutral'): string {
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

	function taskUnavailable(task: ResourceTaskSummary): boolean {
		const availability = task.availability;
		return (
			task.available === false ||
			availability === 'unavailable' ||
			tagOf(availability) === 'unavailable'
		);
	}

	function authorityIssueText(issue: Record<string, unknown>): string {
		return (
			stringField(issue, 'message') ??
			stringField(issue, 'name') ??
			stringField(issue, 'machine') ??
			'Authority unavailable'
		);
	}

	function phaseLabel(value: unknown): string {
		const name = tagOf(value);
		switch (name) {
			case 'awaiting_release':
				return 'Awaiting training release';
			case 'serving':
				return 'Serving an optimization';
			case 'awaiting_return':
				return 'Awaiting return decision';
			case 'restoring':
				return 'Restoring background work';
			default:
				return `Unknown phase${name ? `: ${name}` : ''}`;
		}
	}

	function requestStateLabel(requestState: unknown): string {
		const name = tagOf(requestState);
		switch (name) {
			case 'queued':
				return 'queued';
			case 'assigned':
				return 'reserved';
			case 'finished':
				return `finished${stringField(asRecord(requestState)?.outcome, 'kind') ? ` · ${stringField(asRecord(requestState)?.outcome, 'kind')}` : ''}`;
			case 'cancelled_before_launch':
				return 'cancelled before launch';
			case 'rejected':
				return 'rejected';
			default:
				return `unknown request state${name ? `: ${name}` : ''}`;
		}
	}

	function requestResult(requestState: unknown): string | null {
		const state = asRecord(requestState);
		const name = tagOf(requestState);
		if (name === 'finished') {
			const outcome = asRecord(state?.outcome);
			if (tagOf(outcome) === 'exit') {
				const code = numberField(outcome, 'code');
				return code === null ? 'exit code unavailable' : `exit ${code}`;
			}
			if (tagOf(outcome) === 'signal') {
				return `signal ${stringField(outcome, 'signal') ?? 'unknown'}`;
			}
			if (tagOf(outcome) === 'spawn_failed') {
				return `start failed: ${stringField(outcome, 'message') ?? 'reason unavailable'}`;
			}
			if (tagOf(outcome) === 'cancelled') return 'cancelled';
			return `unknown result${tagOf(outcome) ? `: ${tagOf(outcome)}` : ''}`;
		}
		if (name === 'rejected') return stringField(state, 'reason') ?? 'rejection reason unavailable';
		return null;
	}

	function waitReason(index: number): string {
		let reason: string;
		switch (phaseType) {
			case 'awaiting_release':
				reason =
					detail?.background_task?.status === 'lost'
						? 'The trainer was lost; no checkpoint or result is reusable. The resource stays reserved until operator release.'
						: 'Waiting for the next saved checkpoint and confirmed training release.';
				break;
			case 'serving':
				reason = 'Waiting for the current request to release the GPU.';
				break;
			case 'awaiting_return':
				reason = 'Return reserved; new requests wait for the next checkpoint.';
				break;
			case 'restoring':
				reason = 'Waiting for the return task to confirm its start.';
				break;
			default:
				reason =
					loanStateType === 'needs_attention'
						? 'Blocked until the resource attention is resolved.'
						: detail?.background_task?.status === 'running'
							? 'Waiting for supervised training to release the GPU.'
							: 'Waiting for the resource authority to open this FIFO request.';
		}
		return index === 0 ? reason : `After earlier FIFO request ${index}; ${reason}`;
	}

	function trainerStateText(current: ResourceDetail): string {
		if (loanStateType === 'needs_attention') return 'Supervisor attention is required';
		if (phaseType === 'awaiting_release') {
			if (current.background_task?.status === 'lost') {
				return 'Lost trainer · operator release not confirmed';
			}
			return current.background_task
				? `Release pending · task ${current.background_task.status}`
				: 'Release pending · training task state unavailable';
		}
		if (phaseType === 'serving') {
			const kind = tagOf(returnContext);
			if (kind === 'stopped') return 'Training stopped for optimization';
			if (kind === 'already_completed') return 'Training completed before optimization';
			if (kind === 'ended_without_result') return 'Training ended without a usable result';
			if (kind === 'lost_without_result') {
				return 'Trainer was lost · no checkpoint or result is reusable';
			}
			if (kind === 'idle') return 'No training task was registered at release';
			return 'Optimization is serving · return context unknown';
		}
		if (phaseType === 'awaiting_return')
			return 'Waiting for the supervisor to decide what runs next';
		if (phaseType === 'restoring') return 'Return task holds the reservation';
		if (current.background_task) return `Training task ${current.background_task.status}`;
		const launchText = backgroundLaunchText(current.background_launch);
		if (launchText !== null) return launchText;
		if (current.resource.registered_background_task === null)
			return 'No training task is registered';
		return 'Registered training task state is unavailable';
	}

	function holdsNowText(current: ResourceDetail): string {
		if (loanStateType === 'needs_attention') return 'Resource reserved for supervisor attention';
		switch (phaseType) {
			case 'awaiting_release':
				if (current.background_task?.status === 'lost') {
					return `${current.background_task.display_name} · lost; operator release required`;
				}
				return current.background_task
					? `${current.background_task.display_name} · release pending`
					: 'Training release is pending; task detail is unavailable';
			case 'serving':
				return currentRequest?.display_name ?? 'Optimization request is reserved';
			case 'awaiting_return':
				return 'Return reservation · waiting for supervisor decision';
			case 'restoring':
				return `Return task ${stringField(phase, 'resume_task_id') ?? 'identity unavailable'} · starting`;
			default:
				if (current.current_task) {
					return `${current.current_task.display_name} · ${current.current_task.status}`;
				}
				if (current.background_task) {
					return `${current.background_task.display_name} · ${current.background_task.status}`;
				}
				if (current.background_launch) {
					return `Launch task ${current.background_launch.task_id} · ${current.background_launch.status}`;
				}
				if (current.resource.registered_background_task === null && !current.loan) {
					return 'No task or loan is reported';
				}
				return 'Current holder is unknown';
		}
	}

	function operatorReleaseDetails(provenance: unknown): OperatorReleaseDetails | null {
		const idleBoundary = tagOf(provenance) === 'idle_boundary';
		const proof = idleBoundary ? asRecord(provenance)?.proof : provenance;
		if (tagOf(proof) !== 'operator_attested_gpu_free') return null;
		return {
			operationId: stringField(proof, 'operation_id'),
			taskId: stringField(proof, 'task_id'),
			actionId: idleBoundary ? null : stringField(provenance, 'action_id'),
			idleBoundary
		};
	}

	// the task-layer outcome is tagged by `kind`, not by `type`
	function endedOutcomeText(outcome: unknown): string {
		const kind = stringField(outcome, 'kind');
		switch (kind) {
			case 'exit':
				return `exit ${numberField(outcome, 'code') ?? 'code unavailable'}`;
			case 'signal':
				return `signal ${numberField(outcome, 'signal') ?? 'unavailable'}`;
			case 'cancelled':
				return 'cancelled without a saved checkpoint stop';
			case 'spawn_failed':
				return `start failed: ${stringField(outcome, 'message') ?? 'reason unavailable'}`;
			default:
				return `unknown${kind ? `: ${kind}` : ''}`;
		}
	}

	function returnContextLabel(context: unknown): string {
		switch (tagOf(context)) {
			case 'stopped':
				return 'Stopped at a saved checkpoint';
			case 'already_completed':
				return 'Training completed before release';
			case 'ended_without_result':
				return 'Training ended without a result; it cannot resume';
			case 'lost_without_result':
				return 'Trainer lost; no reusable checkpoint or result';
			case 'idle':
				return 'No registered training task';
			default:
				return `Unknown return context${tagOf(context) ? `: ${tagOf(context)}` : ''}`;
		}
	}

	function pendingActionPhase(action: PendingAction): string {
		switch (tagOf(action.phase)) {
			case 'release_required':
				if (
					action.resource_id === detail?.resource.id &&
					detail.background_task?.status === 'lost'
				) {
					return 'Operator release required';
				}
				return 'Release required';
			case 'return_required':
				return 'Return decision required';
			case 'restoring':
				return 'Return task starting';
			case 'attention_required':
				return 'Attention required';
			default:
				return phaseLabel(action.phase);
		}
	}

	function noticeCanRetry(
		current: ResourceDetail,
		notice: ResourceDetail['notices'][number]
	): boolean {
		const delivery = tagOf(notice.delivery);
		const currentActionId = loanActionId(current.loan);
		const stateType = tagOf(current.loan?.state);
		const currentPhaseType = tagOf(loanPhase(current.loan));
		return (
			delivery === 'failed' &&
			(stateType === 'needs_attention' ||
				(stateType === 'active' &&
					['awaiting_release', 'awaiting_return', 'restoring'].includes(currentPhaseType ?? ''))) &&
			currentActionId !== null &&
			notice.action_id === currentActionId &&
			current.loan?.id === notice.loan_id
		);
	}

	function noticeFailure(notice: ResourceDetail['notices'][number]): string | null {
		return stringField(notice.delivery, 'last_error');
	}
</script>

<div class="mx-auto max-w-7xl px-4 py-4">
	<header class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<a
			href={resolve('/resources')}
			class="inline-flex items-center gap-1 text-primary hover:underline"
		>
			<ArrowLeft class="size-3.5" />
			resources
		</a>
		{#if detail}
			<h1 class="text-base font-semibold tracking-tight">{detail.resource.display_name}</h1>
			<CopyPath value={detail.resource.id} label={shortId(detail.resource.id)} />
			{#if status}
				<span
					class="inline-flex items-center rounded px-1.5 py-0.5 font-mono text-[11px] ring-1 ring-inset {statusToneClass(
						status.tone
					)}"
				>
					{status.label}
				</span>
			{/if}
		{:else}
			<h1 class="text-base font-semibold tracking-tight">Resource detail</h1>
			<CopyPath value={page.params.id ?? ''} label={shortId(page.params.id ?? '')} />
			{#if status}
				<span
					class="inline-flex items-center rounded px-1.5 py-0.5 font-mono text-[11px] ring-1 ring-inset {statusToneClass(
						status.tone
					)}"
				>
					{status.label}
				</span>
			{/if}
		{/if}
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
				&mdash; {store.error.message}. The resource is not shown as available.
			</span>
		</p>
	{/if}

	{#if store.pendingError}
		<p
			class="mt-3 rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-amber-800 dark:text-amber-300"
		>
			Pending supervisor actions could not be loaded:
			<span class="font-mono">{store.pendingError.code}</span>
			&mdash; {store.pendingError.message}
		</p>
	{/if}

	{#if operation}
		<aside
			class="mt-3 rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-amber-800 dark:text-amber-300"
			aria-live="polite"
		>
			<div class="flex flex-wrap items-start justify-between gap-2">
				<div class="min-w-0">
					<p class="font-semibold">{operationMessage ?? 'Operation result is not confirmed'}</p>
					<p class="mt-1 font-mono text-[11px] break-all">
						expected revision {operation.expectedRevision} · operation {operation.operationId}
					</p>
				</div>
				{#if detail?.resource.id === operation.resourceId}
					<button
						type="button"
						onclick={retryOperation}
						disabled={operationBusy || !detail}
						class="inline-flex items-center gap-1 rounded border border-amber-700/30 px-2 py-1 font-medium hover:bg-amber-500/10 disabled:cursor-not-allowed disabled:opacity-50"
					>
						<RefreshCw class="size-3.5" />
						{operationBusy ? 'Sending…' : 'Retry same operation'}
					</button>
				{:else}
					<a
						href={resolve('/resources/[id]', { id: operation.resourceId })}
						class="rounded border border-amber-700/30 px-2 py-1 hover:bg-amber-500/10"
					>
						Open affected resource
					</a>
				{/if}
			</div>
		</aside>
	{/if}

	{#if operationError}
		<aside
			role="alert"
			class="mt-3 flex flex-wrap items-start justify-between gap-2 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-red-700 dark:text-red-300"
		>
			<p class="min-w-0 flex-1">{operationError}</p>
			<button
				type="button"
				onclick={() => (operationError = null)}
				class="shrink-0 rounded border border-red-700/30 px-2 py-0.5 hover:bg-red-500/10"
			>
				Dismiss
			</button>
		</aside>
	{/if}

	{#if operationMessage && !operation}
		<p
			class="mt-3 rounded border border-emerald-500/40 bg-emerald-500/10 px-3 py-2 text-emerald-800 dark:text-emerald-300"
			aria-live="polite"
		>
			{operationMessage}
		</p>
	{/if}

	{#if detail}
		{@const currentTask = detail.current_task}
		{@const backgroundTask = detail.background_task}
		<section class="mt-3 rounded border border-border bg-card p-3">
			<div class="flex flex-wrap items-start justify-between gap-3">
				<div class="min-w-0">
					<p class="text-[11px] tracking-wide text-muted-foreground uppercase">
						what holds the GPU now
					</p>
					<h2 class="mt-1 text-sm font-semibold break-words">{holdsNowText(detail)}</h2>
					<p class="mt-1 text-muted-foreground">{status?.message ?? 'Resource state is unknown'}</p>
				</div>
				<div class="flex flex-wrap items-center gap-2">
					{#if currentTask}
						{#if taskUnavailable(currentTask)}
							<span class="text-muted-foreground">Task details unavailable</span>
						{:else}
							<a
								href={resolve('/tasks/[id]', { id: currentTask.id })}
								class="text-primary hover:underline">Task details</a
							>
							<a
								href={resolve('/tasks/[id]', { id: currentTask.id })}
								onclick={(event) => void openTaskLogs(event, currentTask.id)}
								class="text-primary hover:underline">Logs</a
							>
						{/if}
					{:else if detail.current_task_id}
						<span class="text-muted-foreground">Task summary unavailable</span>
						<a
							href={resolve('/tasks/[id]', { id: detail.current_task_id })}
							class="text-primary hover:underline">Task details</a
						>
						<a
							href={resolve('/tasks/[id]', { id: detail.current_task_id })}
							onclick={(event) => void openTaskLogs(event, detail.current_task_id ?? '')}
							class="text-primary hover:underline">Logs</a
						>
					{/if}
					{#if stoppableTask}
						<button
							type="button"
							onclick={() => void beginAction({ type: 'stop_active', task_id: stoppableTask.id })}
							disabled={!stopPermitted ||
								store.error !== null ||
								operation !== null ||
								operationBusy}
							class="rounded border border-red-500/40 px-2 py-1 text-red-700 hover:bg-red-500/10 disabled:cursor-not-allowed disabled:opacity-50 dark:text-red-300"
							title={stoppableTask.cancel_requested_at
								? 'Stop already requested'
								: stopPermitted
									? 'Request stop; the resource remains reserved until process release is confirmed'
									: 'Stop is available only for a queued or running active test'}
						>
							{stoppableTask.cancel_requested_at ? 'Stopping…' : 'Stop active test'}
						</button>
					{/if}
				</div>
			</div>

			<div
				class="mt-3 grid gap-x-4 gap-y-2 border-t border-border pt-3 sm:grid-cols-2 xl:grid-cols-4"
			>
				<div>
					<p class="text-muted-foreground">resource authority</p>
					<p class="mt-0.5 font-mono break-all">{detail.resource.authority_machine}</p>
				</div>
				<div>
					<p class="text-muted-foreground">resource revision</p>
					<p class="mt-0.5 font-mono">{detail.resource.state_revision}</p>
				</div>
				<div>
					<p class="text-muted-foreground">trainer state</p>
					<p class="mt-0.5">{trainerStateText(detail)}</p>
				</div>
				<div>
					<p class="text-muted-foreground">loan phase</p>
					<p class="mt-0.5">
						{detail.loan
							? phaseType === 'awaiting_release' && backgroundTask?.status === 'lost'
								? 'Awaiting operator release decision'
								: phaseLabel(phase)
							: 'No active loan reported'}
					</p>
				</div>
			</div>
		</section>

		{#if detail.attention || loanStateType === 'needs_attention'}
			<aside
				class="mt-3 rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-amber-800 dark:text-amber-300"
			>
				<h2 class="font-semibold">Attention required</h2>
				<p class="mt-1 break-words">
					{detail.attention?.message ??
						stringField(detail.loan?.state, 'reason') ??
						'The resource loan needs supervisor attention.'}
				</p>
				{#if detail.attention}
					<p class="mt-1 font-mono text-[11px]">{detail.attention.code}</p>
				{/if}
			</aside>
		{/if}

		<div class="mt-3 grid gap-3 lg:grid-cols-2">
			<section class="rounded border border-border bg-card p-3">
				<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">
					training and return context
				</h2>
				{#if backgroundTask}
					<div class="mt-2 flex flex-wrap items-center gap-2">
						<strong class="min-w-0 break-words">{backgroundTask.display_name}</strong>
						<span class="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px]"
							>{backgroundTask.status}</span
						>
						{#if taskUnavailable(backgroundTask)}
							<span class="text-muted-foreground">task details unavailable</span>
						{:else}
							<a
								href={resolve('/tasks/[id]', { id: backgroundTask.id })}
								class="text-primary hover:underline">Task details</a
							>
							<a
								href={resolve('/tasks/[id]', { id: backgroundTask.id })}
								onclick={(event) => void openTaskLogs(event, backgroundTask.id)}
								class="text-primary hover:underline">Logs</a
							>
						{/if}
					</div>
					{#if backgroundTask.status === 'lost'}
						<p class="mt-2 text-amber-700 dark:text-amber-300">
							No reusable checkpoint or result is recorded for this lost trainer.
						</p>
					{/if}
				{:else if detail.background_task_id || detail.resource.registered_background_task}
					<p class="mt-2 text-amber-700 dark:text-amber-300">
						Registered trainer task state is unavailable.
					</p>
					<p class="mt-1 font-mono text-[11px]">
						task {detail.background_task_id ?? detail.resource.registered_background_task}
					</p>
					<a
						href={resolve('/tasks/[id]', {
							id: detail.background_task_id ?? detail.resource.registered_background_task ?? ''
						})}
						class="mt-1 inline-flex text-primary hover:underline">Task details</a
					>
					<a
						href={resolve('/tasks/[id]', {
							id: detail.background_task_id ?? detail.resource.registered_background_task ?? ''
						})}
						onclick={(event) =>
							void openTaskLogs(
								event,
								detail.background_task_id ?? detail.resource.registered_background_task ?? ''
							)}
						class="mt-1 ml-3 inline-flex text-primary hover:underline">Logs</a
					>
				{:else}
					<p class="mt-2 text-muted-foreground">No background task is registered.</p>
				{/if}

				{#if returnContext}
					<div class="mt-3 rounded border border-border/70 bg-muted/40 p-2">
						<p class="font-medium">{returnContextLabel(returnContext)}</p>
						{#if tagOf(returnContext) === 'stopped'}
							<p class="mt-1 font-mono text-[11px] break-all">
								checkpoint {stringField(returnContext, 'checkpoint_ref') ?? 'reference unavailable'}
							</p>
							<p class="mt-1 font-mono text-[11px] break-all">
								recovery {stringField(returnContext, 'recovery_ref') ?? 'reference unavailable'}
							</p>
						{:else if tagOf(returnContext) === 'already_completed'}
							<p class="mt-1 font-mono text-[11px] break-all">
								result {stringField(returnContext, 'result_ref') ?? 'reference unavailable'}
							</p>
						{:else if tagOf(returnContext) === 'ended_without_result'}
							<p class="mt-1">
								Outcome: {endedOutcomeText(asRecord(returnContext)?.outcome)}
							</p>
						{:else if tagOf(returnContext) === 'lost_without_result'}
							<p class="mt-1 text-amber-700 dark:text-amber-300">
								No checkpoint or result can be reused. This lost run cannot resume.
							</p>
						{:else if tagOf(returnContext) !== 'idle'}
							<p class="mt-1 text-amber-700 dark:text-amber-300">
								This return context is not recognized. The resource is not shown as idle.
							</p>
						{/if}
						{#if stringField(returnContext, 'task_id')}
							<a
								href={resolve('/tasks/[id]', { id: stringField(returnContext, 'task_id') ?? '' })}
								class="mt-2 inline-flex text-primary hover:underline"
							>
								Return task details
							</a>
						{/if}
					</div>
				{:else if phaseType === 'awaiting_release'}
					{#if detail?.background_task?.status === 'lost'}
						<p
							class="mt-3 rounded border border-amber-500/40 bg-amber-500/10 p-2 text-amber-800 dark:text-amber-300"
						>
							The trainer was lost. GPU release is not proven. An operator must confirm that no GPU
							work remains.
						</p>
					{:else}
						<p class="mt-3 rounded border border-border/70 bg-muted/40 p-2 text-muted-foreground">
							Waiting for the next complete saved checkpoint before training stops.
						</p>
					{/if}
				{:else if phaseType === 'awaiting_return'}
					<p
						class="mt-3 rounded border border-border/70 bg-muted/40 p-2 text-amber-800 dark:text-amber-300"
					>
						Return reserved; new requests wait for the supervisor decision.
					</p>
				{:else if phaseType === 'restoring'}
					<p class="mt-3 rounded border border-border/70 bg-muted/40 p-2 text-muted-foreground">
						Return task holds the reservation. A trainer releases it on a confirmed start. A native
						foreground command releases it after a successful, confirmed exit. A container releases
						it after it exits with code 0 and Homebased confirms its removal.
					</p>
					<p class="mt-2 font-mono text-[11px] break-all">
						return_execution_mode: {detail.return_execution_mode ?? 'unknown — do not guess'}
					</p>
				{/if}
				{#if operatorRelease}
					<aside
						class="mt-3 rounded border border-amber-500/40 bg-amber-500/10 p-2 text-amber-800 dark:text-amber-300"
					>
						<h3 class="font-medium">
							{operatorRelease.idleBoundary
								? 'Operator-attested idle boundary'
								: 'Operator-attested trainer release'}
						</h3>
						<p class="mt-1">
							An operator attested that GPU work had ended. This is a human decision, not proof of
							process-group exit or trainer-lock release.
						</p>
						{#if operatorRelease.operationId}
							<div class="mt-1 flex flex-wrap items-center gap-1 font-mono text-[11px] break-all">
								<span>attestation {operatorRelease.operationId}</span>
								<CopyPath
									value={operatorRelease.operationId}
									label={`copy attestation ${shortId(operatorRelease.operationId)}`}
								/>
							</div>
						{/if}
						{#if operatorRelease.actionId}
							<div class="mt-1 flex flex-wrap items-center gap-1 font-mono text-[11px] break-all">
								<span>release action {operatorRelease.actionId}</span>
								<CopyPath
									value={operatorRelease.actionId}
									label={`copy action ${shortId(operatorRelease.actionId)}`}
								/>
							</div>
						{/if}
						{#if operatorRelease.taskId}
							<p class="mt-1 font-mono text-[11px] break-all">
								trainer task {operatorRelease.taskId}
							</p>
							<div class="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-sm">
								<a
									href={resolve('/tasks/[id]', { id: operatorRelease.taskId })}
									class="text-primary hover:underline">Task details</a
								>
								<a
									href={resolve('/tasks/[id]', { id: operatorRelease.taskId })}
									onclick={(event) => void openTaskLogs(event, operatorRelease.taskId ?? '')}
									class="text-primary hover:underline">Logs</a
								>
							</div>
						{/if}
					</aside>
				{/if}
			</section>

			<section class="rounded border border-border bg-card p-3">
				<div class="flex flex-wrap items-start justify-between gap-2">
					<div>
						<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">
							supervisor and pending action
						</h2>
						<p class="mt-2 break-all">machine {detail.resource.supervisor.machine}</p>
						<div class="mt-1 flex flex-wrap items-center gap-1">
							<span>thread</span>
							<CopyPath
								value={detail.resource.supervisor.thread}
								label={detail.resource.supervisor.thread}
							/>
							<a
								href={resolve(`/?thread=${detail.resource.supervisor.thread}`)}
								class="text-primary hover:underline"
							>
								view tasks
							</a>
						</div>
						<p class="mt-1 text-muted-foreground">
							assignment revision {detail.resource.assignment_revision}
						</p>
					</div>
					{#if loanActionId(detail.loan)}
						<CopyPath
							value={loanActionId(detail.loan) ?? ''}
							label={`action ${shortId(loanActionId(detail.loan) ?? '')}`}
						/>
					{/if}
				</div>

				{#if store.pendingActions?.length}
					<ul class="mt-3 divide-y divide-border rounded border border-border/70">
						{#each store.pendingActions as action (action.action_id)}
							<li class="px-2 py-2">
								<div class="flex flex-wrap items-center gap-2">
									<strong>{pendingActionPhase(action)}</strong>
									<span class="font-mono text-[11px] text-muted-foreground"
										>revision {action.state_revision}</span
									>
								</div>
								<p class="mt-1 font-mono text-[11px] break-all">action {action.action_id}</p>
								{#if action.return_context}
									<p class="mt-1 text-muted-foreground">
										{returnContextLabel(action.return_context)}
									</p>
								{/if}
							</li>
						{/each}
					</ul>
				{:else if store.pendingUnavailableAuthorities?.length}
					<p class="mt-3 text-amber-700 dark:text-amber-300">
						No action is reported by reachable authorities. Some supervisor actions may be
						unavailable.
					</p>
					<ul class="mt-2 space-y-1">
						{#each store.pendingUnavailableAuthorities as authority, index (`${stringField(authority, 'machine') ?? index}`)}
							<li class="font-mono text-[11px] break-words">{authorityIssueText(authority)}</li>
						{/each}
					</ul>
				{:else if store.pendingActions !== null}
					<p class="mt-3 text-muted-foreground">No pending supervisor actions for this resource.</p>
				{:else if !store.pendingError}
					<p class="mt-3 text-muted-foreground">Loading pending actions&hellip;</p>
				{/if}
			</section>
		</div>

		<section class="mt-3 rounded border border-border bg-card p-3">
			<div class="flex flex-wrap items-baseline justify-between gap-2">
				<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">
					ready queue · authority FIFO
				</h2>
				<span class="text-muted-foreground">{queuedRequests.length} waiting</span>
			</div>
			{#if queuedRequests.length === 0}
				<p class="mt-2 text-muted-foreground">
					{phaseType === 'awaiting_return'
						? 'Queue drained. The return is reserved; new requests wait for the next checkpoint.'
						: 'No queued requests'}
				</p>
			{:else}
				<ol class="mt-2 divide-y divide-border rounded border border-border/70">
					{#each queuedRequests as request, index (request.request_id)}
						<li class="flex flex-wrap items-start justify-between gap-3 px-2 py-2">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-baseline gap-x-2 gap-y-1">
									<span class="font-mono text-muted-foreground">{index + 1}.</span>
									<strong class="break-words">{request.display_name}</strong>
									<span class="font-mono text-[11px] text-muted-foreground">
										sequence {request.acceptance_sequence}
									</span>
								</div>
								<p class="mt-1 text-muted-foreground">{waitReason(index)}</p>
								<p
									class="mt-1 flex flex-wrap items-center gap-x-2 text-[11px] text-muted-foreground"
								>
									<span>requester machine {shortId(request.origin_machine)}</span>
									<span>&middot; task {shortId(request.task_id)}</span>
								</p>
								<div class="mt-1 flex flex-wrap gap-x-3 gap-y-1">
									<a
										href={resolve('/tasks/[id]', { id: request.task_id })}
										class="text-primary hover:underline">Task details</a
									>
									<a
										href={resolve('/tasks/[id]', { id: request.task_id })}
										onclick={(event) => void openTaskLogs(event, request.task_id)}
										class="text-primary hover:underline">Logs</a
									>
								</div>
							</div>
							<button
								type="button"
								onclick={() =>
									void beginAction({ type: 'cancel_queued', request_id: request.request_id })}
								disabled={store.error !== null || operation !== null || operationBusy}
								class="shrink-0 rounded border border-border px-2 py-1 hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
								title="Cancel only this queued request; keep any active return obligation"
							>
								Cancel queued request
							</button>
						</li>
					{/each}
				</ol>
			{/if}
		</section>

		<section class="mt-3 rounded border border-border bg-card p-3">
			<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">supervisor notices</h2>
			{#if detail.notices.length === 0}
				<p class="mt-2 text-muted-foreground">No supervisor notices reported.</p>
			{:else}
				<ul class="mt-2 divide-y divide-border rounded border border-border/70">
					{#each detail.notices as notice (notice.id)}
						{@const payloadType = tagOf(notice.payload) ?? 'unknown'}
						{@const deliveryType = tagOf(notice.delivery) ?? 'unknown'}
						{@const canRetry = noticeCanRetry(detail, notice)}
						<li class="flex flex-wrap items-start justify-between gap-3 px-2 py-2">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-center gap-2">
									<strong>{payloadType.replaceAll('_', ' ')}</strong>
									<span class="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px]"
										>{deliveryType}</span
									>
									<span class="text-[11px] text-muted-foreground">
										attempts {numberField(notice.delivery, 'attempts') ??
											numberField(notice.delivery, 'attempt') ??
											0}
									</span>
								</div>
								<p class="mt-1 font-mono text-[11px] break-all text-muted-foreground">
									notice {notice.id} · action {notice.action_id}
								</p>
								{#if noticeFailure(notice)}
									<p class="mt-1 break-words text-red-700 dark:text-red-300">
										{noticeFailure(notice)}
									</p>
								{/if}
							</div>
							{#if canRetry}
								<button
									type="button"
									onclick={() => void beginAction({ type: 'renotify', notice_id: notice.id })}
									disabled={store.error !== null || operation !== null || operationBusy}
									class="shrink-0 rounded border border-border px-2 py-1 hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
									title="Retry this failed notice for its still-pending action"
								>
									Retry failed notification
								</button>
							{/if}
						</li>
					{/each}
				</ul>
			{/if}
		</section>

		{#if otherRequests.length > 0}
			<section class="mt-3 rounded border border-border bg-card p-3">
				<h2 class="text-[11px] tracking-wide text-muted-foreground uppercase">
					active and completed requests
				</h2>
				<ol class="mt-2 divide-y divide-border rounded border border-border/70">
					{#each otherRequests as request (request.request_id)}
						<li class="flex flex-wrap items-start justify-between gap-3 px-2 py-2">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-baseline gap-x-2 gap-y-1">
									<strong class="break-words">{request.display_name}</strong>
									<span class="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px]">
										{requestStateLabel(request.state)}
									</span>
								</div>
								{#if requestResult(request.state)}
									<p class="mt-1 break-words">Result: {requestResult(request.state)}</p>
								{/if}
								<p class="mt-1 font-mono text-[11px] text-muted-foreground">
									request {request.request_id} · task {request.task_id}
								</p>
								<div class="mt-1 flex flex-wrap gap-x-3 gap-y-1">
									<a
										href={resolve('/tasks/[id]', { id: request.task_id })}
										class="text-primary hover:underline">Task details</a
									>
									<a
										href={resolve('/tasks/[id]', { id: request.task_id })}
										onclick={(event) => void openTaskLogs(event, request.task_id)}
										class="text-primary hover:underline">Logs</a
									>
								</div>
							</div>
						</li>
					{/each}
				</ol>
			</section>
		{/if}
	{:else if !store.error}
		<p
			class="mt-3 rounded border border-border bg-card px-3 py-6 text-center text-muted-foreground"
		>
			Loading authoritative resource detail&hellip;
		</p>
	{/if}
</div>
