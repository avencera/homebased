import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

import { peerQueueHref, peerTaskHref, type FleetMachine } from './api.ts';

const original = Object.getOwnPropertyDescriptor(globalThis, 'window');

function browseFrom(protocol: string) {
	Object.defineProperty(globalThis, 'window', {
		value: { location: { protocol, hostname: 'main' } },
		configurable: true
	});
}

afterEach(() => {
	if (original) Object.defineProperty(globalThis, 'window', original);
	else Reflect.deleteProperty(globalThis, 'window');
});

function peer(dashboard_url: string | null): FleetMachine {
	return {
		machine: '01a15b13-ec3e-7286-8a56-f5dd83d51a3a',
		name: 'mba',
		version: '0.23.5',
		location: { type: 'peer', address: 'http://mba:7677', dashboard_url },
		read: { state: 'online' }
	};
}

test('an https page links a peer at its https dashboard', () => {
	browseFrom('https:');
	const mba = peer('https://mba.camel-scylla.ts.net:7678');
	assert.equal(peerTaskHref(mba, 't1'), 'https://mba.camel-scylla.ts.net:7678/tasks/t1');
	assert.equal(peerQueueHref(mba), 'https://mba.camel-scylla.ts.net:7678/queue');
});

test('a plain page, or a peer without an https dashboard, uses the daemon address', () => {
	browseFrom('http:');
	assert.equal(
		peerTaskHref(peer('https://mba.camel-scylla.ts.net:7678'), 't1'),
		'http://mba:7677/tasks/t1'
	);

	browseFrom('https:');
	assert.equal(peerQueueHref(peer(null)), 'http://mba:7677/queue');
});

test('the local machine has no peer link', () => {
	browseFrom('https:');
	const local: FleetMachine = { ...peer(null), location: { type: 'local' } };
	assert.equal(peerTaskHref(local, 't1'), null);
});
