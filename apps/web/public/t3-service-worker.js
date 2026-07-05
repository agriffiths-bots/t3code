const CACHE_VERSION = "t3-code-v1";
const APP_SHELL_URLS = ["/", "/manifest.webmanifest", "/pwa-icon-192.png", "/pwa-icon-512.png"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_VERSION)
      .then((cache) => cache.addAll(APP_SHELL_URLS))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(keys.filter((key) => key !== CACHE_VERSION).map((key) => caches.delete(key))),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;

  const acceptsHtml = request.headers.get("accept")?.includes("text/html") ?? false;
  if (!acceptsHtml || new URL(request.url).origin !== self.location.origin) return;

  event.respondWith(
    fetch(request)
      .then((response) => {
        const contentType = response.headers.get("content-type") ?? "";
        if (response.ok && contentType.includes("text/html")) {
          const copy = response.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put("/", copy));
        }
        return response;
      })
      .catch(async () => {
        const cached = await caches.match("/");
        return (
          cached ??
          new Response("T3 Code is offline. Reconnect to continue.", {
            status: 503,
            headers: { "content-type": "text/plain; charset=utf-8" },
          })
        );
      }),
  );
});
