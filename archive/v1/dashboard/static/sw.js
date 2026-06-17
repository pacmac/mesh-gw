// Service worker — cache app shell so the page loads even when the bridge
// restarts or is temporarily unreachable.
// API calls go cross-origin (MESH_API on port 8001) so the SW never
// intercepts them — no exclusion list needed.
const CACHE = 'mesh-gw-dash-v1';
const SHELL = ['/', '/app.js', '/style.css', '/config.js'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  // Navigation: try network, fall back to cached shell
  if (e.request.mode === 'navigate') {
    e.respondWith(fetch(e.request).catch(() => caches.match('/')));
    return;
  }
  // Static shell assets: cache-first, update in background
  if (SHELL.includes(url.pathname)) {
    e.respondWith(
      caches.match(e.request).then(cached => {
        const network = fetch(e.request).then(res => {
          if (res.ok) caches.open(CACHE).then(c => c.put(e.request, res.clone()));
          return res;
        });
        return cached || network;
      })
    );
  }
});
