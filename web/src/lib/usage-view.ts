// Display helpers for the usage page. Pure, so the page stays declarative.

// No import from format.ts: its extensionless imports do not load under `node --test`
const EM_DASH = '—';

/** Time windows the usage page offers, with their length. */
export const USAGE_WINDOWS = [
	{ key: '24h', label: '24h', ms: 24 * 60 * 60 * 1000 },
	{ key: '7d', label: '7d', ms: 7 * 24 * 60 * 60 * 1000 },
	{ key: '30d', label: '30d', ms: 30 * 24 * 60 * 60 * 1000 }
] as const;

export type UsageWindowKey = (typeof USAGE_WINDOWS)[number]['key'];

export const DEFAULT_USAGE_WINDOW: UsageWindowKey = '7d';

/** Start of a window that ends at `now`. */
export function windowStart(key: UsageWindowKey, now: number): Date {
	const window = USAGE_WINDOWS.find((candidate) => candidate.key === key);
	return new Date(now - (window?.ms ?? 0));
}

const SUFFIXES = [
	{ at: 1e9, suffix: 'B' },
	{ at: 1e6, suffix: 'M' },
	{ at: 1e3, suffix: 'K' }
] as const;

/** Compact token count: `950`, `32.3K`, `4.97M`, `1.2B`. Three significant digits at most. */
export function formatTokens(count: number): string {
	if (!Number.isFinite(count)) return EM_DASH;
	const magnitude = Math.abs(count);
	for (const [index, { at, suffix }] of SUFFIXES.entries()) {
		if (magnitude < at) continue;
		const scaled = count / at;
		const digits = Math.abs(scaled) < 10 ? 2 : Math.abs(scaled) < 100 ? 1 : 0;
		const text = scaled.toFixed(digits);
		// 999.9K rounds up to 1000K, which reads better as 1.00M
		const larger = SUFFIXES[index - 1];
		if (larger && Math.abs(Number(text)) >= 1000) return formatTokens(larger.at * Math.sign(count));
		return `${text}${suffix}`;
	}
	return Math.round(count).toString();
}

const COST = new Intl.NumberFormat('en-US', {
	style: 'currency',
	currency: 'USD',
	minimumFractionDigits: 2,
	maximumFractionDigits: 2
});

/** Dollar cost. */
export function formatCost(usd: number): string {
	if (!Number.isFinite(usd)) return EM_DASH;
	return COST.format(usd);
}

/** Why a task's cost is an undercount: its worker stopped before Claude Code's final tally. */
export const TASK_NOTE =
	'This worker stopped before Claude Code wrote its final tally, so its cost is missing and its tokens are undercounted, output most of all.';

/** Why a group's cost is an undercount, naming how many of its tasks stopped early. */
export function groupNote(stopped: number, tasks: number): string {
	const which = stopped === 1 ? '1 task' : `${stopped} tasks`;
	return `${which} of ${tasks} stopped before Claude Code wrote the final tally, so this cost and its tokens are undercounts.`;
}

/** Model id without the vendor prefix, for dense tables. */
export function modelLabel(model: string): string {
	return model.replace(/^claude-/, '');
}
