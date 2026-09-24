// Stable pill colors. A hue is the only input, so one name keeps one color on
// every page and in both color schemes

/** Hues of the familiar machines, chosen away from the status badge hues. */
const MACHINE_HUES: Readonly<Record<string, number>> = { main: 292, code: 185 };

/** Hues for names without a fixed color, spaced for contrast. */
const PALETTE = [25, 65, 110, 150, 205, 240, 275, 320, 345] as const;

/** Hue from a stable hash of the name, so it does not change between loads. */
export function hueFor(name: string): number {
	// FNV-1a: cheap, deterministic, and well spread for short names
	let hash = 0x811c9dc5;
	for (let index = 0; index < name.length; index += 1) {
		hash ^= name.charCodeAt(index);
		hash = Math.imul(hash, 0x01000193);
	}
	return PALETTE[(hash >>> 0) % PALETTE.length];
}

/** Hue of a machine: fixed for main and code, hashed for any other name. */
export function machineHue(name: string): number {
	return MACHINE_HUES[name] ?? hueFor(name);
}
