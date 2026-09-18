import tailwindcss from '@tailwindcss/vite';
import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// Dashboard address used when the daemon is started with
// `--web-listen 127.0.0.1:7677`. `npm run dev` proxies the read API there so
// the dev server talks to a real daemon.
const DAEMON_ORIGIN = 'http://127.0.0.1:7677';

export default defineConfig({
	plugins: [
		tailwindcss(),
		sveltekit({
			compilerOptions: {
				// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true
			},
			// `fallback` makes deep links such as /tasks/<uuid> work from the embedded bundle
			adapter: adapter({ pages: 'build', assets: 'build', fallback: 'index.html' })
		})
	],
	server: {
		proxy: {
			'/v1': DAEMON_ORIGIN
		}
	}
});
