# PWA Installability, Keep-Alive, and Push

T3 Code's web app is installable from Android Chrome through `apps/web/public/manifest.webmanifest`
and the scoped service worker at `/sw.js`.

The manifest declares a stable `id`, `start_url`, `/` scope, standalone display mode, theme and
background colors, and 192 px, 512 px, and maskable icons. The document head links the manifest and
mobile app metadata from `apps/web/index.html`.

The service worker is intentionally small: it claims the root scope, caches the app shell for a
navigation fallback, handles Push API payloads, opens thread deep links, and acknowledges
notification open/close/dismiss actions back to `/api/notifications/ack`.

Keep-alive strategy is best effort because browsers deliberately suspend background pages. While the
app is visible, `PwaRuntime` requests the Screen Wake Lock API when available and refreshes device
registration periodically. When Android Chrome backgrounds or kills the installed app, native Push
API delivery becomes the durable wake-up path. The foreground WebSocket should reconnect through the
existing connection supervisor when the page resumes.

Native notification delivery paths:

- Android Chrome/PWA: browser Push API subscription registered with the server and delivered using
  VAPID/web-push.
- Desktop/Electron on Windows: renderer Notification API fed by the live server notification stream.

Cross-device dismissal is driven by the server. Any authenticated client ACK or service-worker ACK
removes the active notification and fans out a dismiss event to live desktop clients plus a dismiss
push to web-push devices.
