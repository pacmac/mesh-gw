// Service worker — cache app shell so the page loads even when the bridge
// restarts or is temporarily unreachable (instead of Chrome's error page).
const CACHE = 'mesh-bridge-v1';
const SHELL = ['/', '/app.js', '/style.css'];

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
  // Don't intercept API calls — let them fail naturally so the JS can handle it
  if (url.pathname.startsWith('/ws') || url.pathname === '/status' ||
      url.pathname === '/nodes' || url.pathname === '/info' ||
      url.pathname === '/config' || url.pathname === '/bridge_config' ||
      url.pathname === '/channels' || url.pathname === '/messages' ||
      url.pathname === '/ble' || url.pathname.startsWith('/ble/') ||
      url.pathname === '/rpc') {
    return;
  }
  // Navigation request (user types the URL / refreshes) — try network, fall back to cache
  if (e.request.mode === 'navigate') {
    e.respondWith(
      fetch(e.request).catch(() => caches.match('/'))
    );
    return;
  }
  // Static assets — cache-first, update in background
  if (SHELL.includes(url.pathname)) {
    e.respondWith(
      caches.match(e.request).then(cached => {
        const network = fetch(e.request).then(res => {
          if (res.ok) {
            caches.open(CACHE).then(c => c.put(e.request, res.clone()));
          }
          return res;
        });
        return cached || network;
      })
    );
  }
});
