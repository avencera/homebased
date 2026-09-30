import assert from 'node:assert/strict';
import { test } from 'node:test';

import { candidatePaths, splitCommandPaths } from './command-paths.ts';

const cwd = '/Users/me/code/app';

test('an absolute script path is one candidate', () => {
	assert.deepEqual(splitCommandPaths('/Users/me/code/app/_scratch/watch.sh', cwd), [
		{
			type: 'path',
			text: '/Users/me/code/app/_scratch/watch.sh',
			path: '/Users/me/code/app/_scratch/watch.sh'
		}
	]);
});

test('relative and bare file names resolve against the cwd', () => {
	const pieces = splitCommandPaths('python script.py --out=./build/out.json', cwd);
	assert.deepEqual(candidatePaths(pieces), [
		'/Users/me/code/app/script.py',
		'/Users/me/code/app/./build/out.json'
	]);
	assert.equal(
		pieces.map((piece) => piece.text).join(''),
		'python script.py --out=./build/out.json'
	);
});

test('paths inside a shell script keep their surrounding quotes as text', () => {
	const script = `cd "/tmp/work" && ./run.sh; echo done`;
	const pieces = splitCommandPaths(script, cwd);
	assert.deepEqual(candidatePaths(pieces), ['/tmp/work', '/Users/me/code/app/./run.sh']);
	assert.equal(pieces.map((piece) => piece.text).join(''), script);
});

test('flags, URLs, home paths, and plain words are not candidates', () => {
	const argument = '-v --json https://example.com/a ~/notes.md echo 1.5 //net';
	assert.deepEqual(candidatePaths(splitCommandPaths(argument, cwd)), []);
});
