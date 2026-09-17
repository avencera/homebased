// Display helpers. Everything here is pure so the components stay declarative.

import { isInFlight, type ExitReason, type TaskSummary } from './api';

/** Home directories the daemon can run under, matched so paths can show `~`. */
const HOME_PREFIX = /^(\/home\/[^/]+|\/Users\/[^/]+|\/root)(?=\/|$)/;

/** Placeholder for a value the daemon reports as null. */
export const EM_DASH = '—';

/** First bytes of a UUID, enough to recognise a task or thread by eye. */
export function shortId(value: string, length = 8): string {
	return value.slice(0, length);
}

/**
 * Replace a leading home directory with `~`. The API sends absolute paths and
 * no `HOME`, so the prefix is recognised by shape rather than compared.
 */
export function shortenHome(path: string): string {
	return path.replace(HOME_PREFIX, '~');
}

/** Instant accepted by the display helpers. */
export type Instant = string | number | Date;

/** Parse an RFC 3339 timestamp, a `Date`, or epoch milliseconds. */
export function epochMs(value: Instant | null | undefined): number | null {
	if (value === null || value === undefined) return null;
	if (typeof value === 'number') return Number.isFinite(value) ? value : null;
	const ms = value instanceof Date ? value.getTime() : Date.parse(value);
	return Number.isNaN(ms) ? null : ms;
}

/** Absolute local time, for hover titles and the detail page. */
export function formatTimestamp(value: Instant | null | undefined): string {
	const ms = epochMs(value);
	if (ms === null) return EM_DASH;
	return new Date(ms).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' });
}

/** Compact duration: `2h 05m`, `3m 12s`, `9s`. */
export function formatDuration(ms: number): string {
	if (!Number.isFinite(ms)) return EM_DASH;
	const seconds = Math.max(0, Math.floor(ms / 1000));
	const hours = Math.floor(seconds / 3600);
	const minutes = Math.floor(seconds / 60) % 60;
	const rest = seconds % 60;
	if (hours > 0) return `${hours}h ${pad(minutes)}m`;
	if (minutes > 0) return `${minutes}m ${pad(rest)}s`;
	return `${rest}s`;
}

/** How long the task has run, or ran before it finished. */
export function taskDurationMs(task: TaskSummary, now: number): number {
	const start = epochMs(task.created_at) ?? now;
	const end = isInFlight(task.status) ? now : (epochMs(task.updated_at) ?? now);
	return end - start;
}

/**
 * Time left on the wall-clock budget. Measured from `created_at` because the
 * API reports no separate start time, so queue time counts against it.
 */
export function timeoutRemainingMs(task: TaskSummary, now: number): number | null {
	if (!isInFlight(task.status)) return null;
	const start = epochMs(task.created_at);
	if (start === null) return null;
	return start + task.timeout_secs * 1000 - now;
}

/** Agent plus model, as one dense label. */
export function agentLabel(task: Pick<TaskSummary, 'agent' | 'model'>): string {
	return task.model ? `${task.agent}:${task.model}` : task.agent;
}

/** One line per `ExitReason` variant. */
export function exitReasonText(reason: ExitReason | null): string {
	if (reason === null) return EM_DASH;
	switch (reason.kind) {
		case 'exit':
			return reason.code === 0 ? 'exit 0' : `exit ${reason.code}`;
		case 'signal':
			return `signal ${reason.signal}`;
		case 'timeout':
			return `timeout after ${formatDuration(reason.secs * 1000)}`;
		case 'cancelled':
			return 'cancelled';
		case 'spawn_failed':
			return `spawn failed: ${reason.message}`;
	}
}

/** First line of a report summary, for the dense table. */
export function firstLine(text: string): string {
	const line = text.split('\n', 1)[0]?.trim() ?? '';
	return line.length > 0 ? line : EM_DASH;
}

function pad(value: number): string {
	return value.toString().padStart(2, '0');
}
