// The dashboard is a static SPA embedded in the daemon binary: no server, no
// prerendered routes, every page renders in the browser against /v1.
export const ssr = false;
export const prerender = false;
