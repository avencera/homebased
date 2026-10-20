import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

import { contentUrlForPath, contentUrlForToken, type ContentOrigin } from './api.ts';

const original = Object.getOwnPropertyDescriptor(globalThis, 'window');

function browseFrom(protocol: string, hostname: string) {
	Object.defineProperty(globalThis, 'window', {
		value: { location: { protocol, hostname } },
		configurable: true
	});
}

afterEach(() => {
	if (original) Object.defineProperty(globalThis, 'window', original);
	else Reflect.deleteProperty(globalThis, 'window');
});

const proxied: ContentOrigin = { api_version: 1, port: 7679, https_port: 7680 };
const plain: ContentOrigin = { api_version: 1, port: 51434, https_port: null };

test('an https page uses the proxy port, a plain page the daemon port', () => {
	browseFrom('https:', 'main.camel-scylla.ts.net');
	assert.equal(
		contentUrlForPath(proxied, '/tmp/a b.txt'),
		'https://main.camel-scylla.ts.net:7680/raw/tmp/a%20b.txt'
	);
	assert.equal(
		contentUrlForToken(proxied, 'tok/en'),
		'https://main.camel-scylla.ts.net:7680/by-token/tok%2Fen'
	);

	browseFrom('http:', 'main');
	assert.equal(contentUrlForPath(proxied, '/tmp/a.txt'), 'http://main:7679/raw/tmp/a.txt');
});

test('without a proxy port every page keeps the daemon port', () => {
	browseFrom('https:', 'main.camel-scylla.ts.net');
	assert.equal(
		contentUrlForPath(plain, '/tmp/a.txt'),
		'https://main.camel-scylla.ts.net:51434/raw/tmp/a.txt'
	);
	// a response from an older daemon has no https_port key at all
	const legacy = { api_version: 1, port: 51434 } as ContentOrigin;
	assert.equal(
		contentUrlForToken(legacy, 't'),
		'https://main.camel-scylla.ts.net:51434/by-token/t'
	);
});

test('ipv6 hosts are bracketed', () => {
	browseFrom('http:', '::1');
	assert.equal(contentUrlForPath(plain, '/x'), 'http://[::1]:51434/raw/x');
});
