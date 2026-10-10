import assert from 'node:assert/strict';
import { test } from 'node:test';

import { formatCost, formatTokens, modelLabel, windowStart } from './usage-view.ts';

test('formatTokens scales to K, M, and B with at most three significant digits', () => {
	assert.equal(formatTokens(0), '0');
	assert.equal(formatTokens(950), '950');
	assert.equal(formatTokens(1_000), '1.00K');
	assert.equal(formatTokens(32_339), '32.3K');
	assert.equal(formatTokens(4_972_919), '4.97M');
	assert.equal(formatTokens(120_000_000), '120M');
	assert.equal(formatTokens(1_200_000_000), '1.20B');
});

test('formatTokens does not print a thousand of the smaller unit', () => {
	assert.equal(formatTokens(999_999), '1.00M');
	assert.equal(formatTokens(999_999_999), '1.00B');
});

test('formatCost groups thousands', () => {
	assert.equal(formatCost(2.489), '$2.49');
	assert.equal(formatCost(1234.5), '$1,234.50');
});

test('windowStart subtracts the window from now', () => {
	const now = Date.parse('2026-10-09T12:00:00Z');
	assert.equal(windowStart('24h', now).toISOString(), '2026-10-08T12:00:00.000Z');
	assert.equal(windowStart('7d', now).toISOString(), '2026-10-02T12:00:00.000Z');
});

test('modelLabel drops the vendor prefix only', () => {
	assert.equal(modelLabel('claude-opus-5-5'), 'opus-5-5');
	assert.equal(modelLabel('other-model'), 'other-model');
});
