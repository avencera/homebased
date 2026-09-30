/** Piece of a command argument: plain text, or a word that may name a file. */
export type CommandPiece =
	{ type: 'text'; text: string } | { type: 'path'; text: string; path: string };

// quotes, brackets, and punctuation that wrap a path inside a shell script argument
const LEADING_NOISE = /^[\s"'`([{<]+/;
const TRAILING_NOISE = /[\s"'`)\]}>;,:]+$/;
// `--flag=value` and `VAR=value` carry the path after the `=`
const ASSIGNMENT = /^[^/=]*=/;
// a bare `name.ext` word, such as `python script.py`, is worth checking in the cwd
const BARE_FILE = /^[\w.-]*\w\.[A-Za-z][A-Za-z0-9]{0,7}$/;
const URL_SCHEME = /^[a-z][a-z0-9+.-]*:\/\//i;

/**
 * Split one command argument into text and path candidates.
 *
 * A candidate is only a guess: the caller checks that it exists before linking it. Relative
 * candidates are joined to `cwd`, because the daemon spawns the command there. A `~` path is
 * left as text: the argv the daemon ran was already expanded, so a literal `~` names nothing.
 */
export function splitCommandPaths(argument: string, cwd: string): CommandPiece[] {
	const pieces: CommandPiece[] = [];
	for (const word of argument.split(/(\s+)/)) {
		for (const piece of splitWord(word, cwd)) pushPiece(pieces, piece);
	}
	return pieces;
}

/** Unique absolute paths named by the candidates in `pieces`. */
export function candidatePaths(pieces: readonly CommandPiece[]): string[] {
	const paths = pieces.flatMap((piece) => (piece.type === 'path' ? [piece.path] : []));
	return [...new Set(paths)];
}

function splitWord(word: string, cwd: string): CommandPiece[] {
	const leading = word.match(LEADING_NOISE)?.[0] ?? '';
	const afterLeading = word.slice(leading.length);
	const assignment = afterLeading.match(ASSIGNMENT)?.[0] ?? '';
	const rest = afterLeading.slice(assignment.length);
	const trailing = rest.match(TRAILING_NOISE)?.[0] ?? '';
	const core = rest.slice(0, rest.length - trailing.length);

	const path = absoluteCandidate(core, cwd);
	if (path === null) return [{ type: 'text', text: word }];
	return [
		{ type: 'text', text: leading + assignment },
		{ type: 'path', text: core, path },
		{ type: 'text', text: trailing }
	];
}

function absoluteCandidate(core: string, cwd: string): string | null {
	if (core.length < 2 || URL_SCHEME.test(core)) return null;
	if (core.startsWith('/')) return core.startsWith('//') ? null : core;
	if (core.startsWith('~') || core.startsWith('-')) return null;
	if (!core.includes('/') && !BARE_FILE.test(core)) return null;
	return `${cwd.replace(/\/+$/, '')}/${core}`;
}

function pushPiece(pieces: CommandPiece[], piece: CommandPiece) {
	if (piece.type === 'text' && piece.text === '') return;
	const last = pieces.at(-1);
	if (piece.type === 'text' && last?.type === 'text') {
		last.text += piece.text;
		return;
	}
	pieces.push({ ...piece });
}
