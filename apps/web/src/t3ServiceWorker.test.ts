import { describe, expect, it } from "vite-plus/test";

import serviceWorkerSource from "../public/sw.js?raw";

const ORIGIN = "https://app.test";
const CACHE_VERSION = "t3-code-v2";

type RequestStub = {
  headers: Headers;
  method: "GET";
  mode: "cors" | "navigate";
  url: string;
};

type CacheInput = Request | RequestStub | URL | string;
type FetchResponder = (input: CacheInput) => Promise<Response> | Response;
type FetchListener = (event: FetchEventStub) => void;

type FetchEventStub = {
  request: RequestStub;
  respondWith: (response: Promise<Response> | Response) => void;
  waitUntil: (promise: Promise<unknown>) => void;
};

type WorkerExports = {
  currentBuildAssetCacheName: () => Promise<string>;
  deleteSupersededCaches: () => Promise<void>;
};

type WorkerLoader = (
  selfScope: WorkerSelfStub,
  cacheStorage: MemoryCacheStorage,
  responseConstructor: typeof Response,
  requestConstructor: typeof Request,
  urlConstructor: typeof URL,
  promiseConstructor: typeof Promise,
  setConstructor: typeof Set,
  errorConstructor: typeof Error,
  fetchImplementation: FetchResponder,
) => WorkerExports;

type WorkerSelfStub = {
  addEventListener: (type: string, listener: unknown) => void;
  clients: {
    claim: () => Promise<void>;
    matchAll: () => Promise<unknown[]>;
    openWindow: () => Promise<void>;
  };
  location: URL;
  registration: {
    getNotifications: () => Promise<unknown[]>;
    showNotification: () => Promise<void>;
  };
  skipWaiting: () => void;
};

function absoluteUrl(input: CacheInput): string {
  if (typeof input === "string") {
    return new URL(input, ORIGIN).href;
  }
  if (input instanceof URL) {
    return input.href;
  }
  return new URL(input.url, ORIGIN).href;
}

function pathnameFromInput(input: CacheInput): string {
  return new URL(absoluteUrl(input)).pathname;
}

class MemoryCache {
  private readonly entries = new Map<string, Response>();

  async addAll(urls: string[]): Promise<void> {
    for (const url of urls) {
      await this.put(url, new Response("", { status: 200 }));
    }
  }

  async delete(input: CacheInput): Promise<boolean> {
    return this.entries.delete(absoluteUrl(input));
  }

  async keys(): Promise<Request[]> {
    return [...this.entries.keys()].map((url) => new Request(url));
  }

  async match(input: CacheInput): Promise<Response | undefined> {
    return this.entries.get(absoluteUrl(input))?.clone();
  }

  async put(input: CacheInput, response: Response): Promise<void> {
    this.entries.set(absoluteUrl(input), response.clone());
  }
}

class MemoryCacheStorage {
  private readonly caches = new Map<string, MemoryCache>();

  async delete(name: string): Promise<boolean> {
    return this.caches.delete(name);
  }

  async keys(): Promise<string[]> {
    return [...this.caches.keys()];
  }

  async match(input: CacheInput): Promise<Response | undefined> {
    for (const cache of this.caches.values()) {
      const response = await cache.match(input);
      if (response) return response;
    }
    return undefined;
  }

  async open(name: string): Promise<MemoryCache> {
    const existing = this.caches.get(name);
    if (existing) return existing;

    const cache = new MemoryCache();
    this.caches.set(name, cache);
    return cache;
  }
}

function createHarness(fetchResponder: FetchResponder) {
  const listeners = new Map<string, FetchListener>();
  const cacheStorage = new MemoryCacheStorage();
  const fetchCalls: string[] = [];
  let windowClientCount = 0;
  const selfScope: WorkerSelfStub = {
    location: new URL(`${ORIGIN}/`),
    addEventListener(type, listener) {
      if (type === "fetch") {
        listeners.set(type, listener as FetchListener);
      }
    },
    skipWaiting() {},
    clients: {
      async claim() {},
      async matchAll() {
        return Array.from({ length: windowClientCount }, () => ({}));
      },
      async openWindow() {},
    },
    registration: {
      async getNotifications() {
        return [];
      },
      async showNotification() {},
    },
  };
  const loadWorker = new Function(
    "self",
    "caches",
    "Response",
    "Request",
    "URL",
    "Promise",
    "Set",
    "Error",
    "fetch",
    `${serviceWorkerSource}\nreturn { currentBuildAssetCacheName, deleteSupersededCaches };`,
  ) as WorkerLoader;
  const workerExports = loadWorker(
    selfScope,
    cacheStorage,
    Response,
    Request,
    URL,
    Promise,
    Set,
    Error,
    async (input: CacheInput) => {
      fetchCalls.push(pathnameFromInput(input));
      return fetchResponder(input);
    },
  );

  return {
    cache: () => cacheStorage.open(CACHE_VERSION),
    cacheStorage,
    dispatchFetch: async (request: RequestStub) => {
      const listener = listeners.get("fetch");
      if (!listener) {
        throw new Error("Service worker did not register a fetch listener.");
      }

      const waitUntilPromises: Promise<unknown>[] = [];
      let responsePromise: Promise<Response> | undefined;
      listener({
        request,
        respondWith(response) {
          responsePromise = Promise.resolve(response);
        },
        waitUntil(promise) {
          waitUntilPromises.push(promise);
        },
      });

      const response = await responsePromise;
      await Promise.all(waitUntilPromises);
      return { handled: responsePromise !== undefined, response };
    },
    fetchCalls,
    setWindowClientCount(count: number) {
      windowClientCount = count;
    },
    workerExports,
  };
}

function assetResponse(body: string): Response {
  return new Response(body, {
    headers: { "content-type": "application/javascript; charset=utf-8" },
  });
}

async function cachedText(cache: MemoryCache, input: CacheInput): Promise<string | undefined> {
  return (await cache.match(input))?.text();
}

async function cachedStorageText(
  cacheStorage: MemoryCacheStorage,
  input: CacheInput,
): Promise<string | undefined> {
  return (await cacheStorage.match(input))?.text();
}

function htmlResponse(body: string): Response {
  return new Response(body, {
    headers: { "content-type": "text/html; charset=utf-8" },
  });
}

function navigationRequest(pathname: string): RequestStub {
  return {
    headers: new Headers({ accept: "text/html" }),
    method: "GET",
    mode: "navigate",
    url: new URL(pathname, ORIGIN).href,
  };
}

interface MemoryIdbRequest {
  error?: Error | null;
  result?: unknown;
  onerror?: () => void;
  onsuccess?: () => void;
  onupgradeneeded?: () => void;
}

interface MemoryIdbTransaction {
  error?: Error | null;
  onabort?: () => void;
  oncomplete?: () => void;
  onerror?: () => void;
  objectStore: () => {
    get: (key: string) => MemoryIdbRequest;
    put: (value: unknown, key: string) => MemoryIdbRequest;
  };
}

class MemoryIndexedDbFactory {
  private created = false;
  private readonly values = new Map<string, unknown>();

  open(): MemoryIdbRequest {
    const request: MemoryIdbRequest = {};
    queueMicrotask(() => {
      request.result = {
        close() {},
        createObjectStore: () => {
          this.created = true;
        },
        objectStoreNames: {
          contains: () => this.created,
        },
        transaction: (): MemoryIdbTransaction => {
          let writeQueued = false;
          const transaction: MemoryIdbTransaction = {
            objectStore: () => ({
              get: (key) => {
                const getRequest: MemoryIdbRequest = {};
                queueMicrotask(() => {
                  getRequest.result = this.values.get(key);
                  getRequest.onsuccess?.();
                  if (!writeQueued) transaction.oncomplete?.();
                });
                return getRequest;
              },
              put: (value, key) => {
                writeQueued = true;
                const putRequest: MemoryIdbRequest = {};
                queueMicrotask(() => {
                  this.values.set(key, value);
                  putRequest.result = key;
                  putRequest.onsuccess?.();
                  transaction.oncomplete?.();
                });
                return putRequest;
              },
            }),
          };
          return transaction;
        },
      };
      if (!this.created) {
        request.onupgradeneeded?.();
      }
      request.onsuccess?.();
    });
    return request;
  }
}

type PushSubscriptionStub = {
  endpoint: string;
  toJSON: () => {
    endpoint: string;
    expirationTime: number | null;
    keys: { auth: string; p256dh: string };
  };
};

function makePushSubscription(endpoint: string): PushSubscriptionStub {
  return {
    endpoint,
    toJSON: () => ({
      endpoint,
      expirationTime: null,
      keys: { auth: "new-auth", p256dh: "new-p256dh" },
    }),
  };
}

function createPushRecoveryHarness(
  recoveryResponder: (init: RequestInit) => Promise<Response> = async () =>
    new Response(JSON.stringify({ recoveryToken: "rotated-recovery-token" }), {
      headers: { "content-type": "application/json" },
    }),
) {
  const listeners = new Map<string, (event: unknown) => void>();
  const indexedDb = new MemoryIndexedDbFactory();
  const subscription = makePushSubscription("https://push.example/new");
  const subscribeCalls: Array<{
    readonly applicationServerKey: Uint8Array;
    readonly userVisibleOnly: boolean;
  }> = [];
  const recoveryPosts: Array<{ readonly body: string; readonly init: RequestInit }> = [];
  const selfScope = {
    location: new URL(`${ORIGIN}/`),
    addEventListener(type: string, listener: (event: unknown) => void) {
      listeners.set(type, listener);
    },
    skipWaiting() {},
    clients: {
      async claim() {},
      async matchAll() {
        return [];
      },
      async openWindow() {},
    },
    registration: {
      async getNotifications() {
        return [];
      },
      async showNotification() {},
      pushManager: {
        async subscribe(options: {
          readonly applicationServerKey: Uint8Array;
          readonly userVisibleOnly: boolean;
        }) {
          subscribeCalls.push(options);
          return subscription;
        },
      },
    },
  };
  const loadWorker = new Function(
    "self",
    "caches",
    "Response",
    "Request",
    "URL",
    "Promise",
    "Set",
    "Error",
    "fetch",
    "indexedDB",
    `${serviceWorkerSource}\nreturn { readPushRecoveryConfig };`,
  ) as (
    selfScope: unknown,
    cacheStorage: MemoryCacheStorage,
    responseConstructor: typeof Response,
    requestConstructor: typeof Request,
    urlConstructor: typeof URL,
    promiseConstructor: typeof Promise,
    setConstructor: typeof Set,
    errorConstructor: typeof Error,
    fetchImplementation: (input: string, init: RequestInit) => Promise<Response>,
    indexedDbFactory: MemoryIndexedDbFactory,
  ) => { readPushRecoveryConfig: () => Promise<unknown> };
  const workerExports = loadWorker(
    selfScope,
    new MemoryCacheStorage(),
    Response,
    Request,
    URL,
    Promise,
    Set,
    Error,
    async (_input, init) => {
      recoveryPosts.push({ body: String(init.body), init });
      return await recoveryResponder(init);
    },
    indexedDb,
  );

  const dispatchExtendableEvent = async (type: string, event: Record<string, unknown>) => {
    const listener = listeners.get(type);
    if (!listener) throw new Error(`Service worker did not register a ${type} listener.`);
    const waitUntilPromises: Promise<unknown>[] = [];
    listener({
      ...event,
      waitUntil(promise: Promise<unknown>) {
        waitUntilPromises.push(promise);
      },
    });
    await Promise.all(waitUntilPromises);
  };

  return { dispatchExtendableEvent, recoveryPosts, subscribeCalls, workerExports };
}

describe("t3 service worker", () => {
  it("recovers a changed push subscription in the background and stores the rotated token", async () => {
    const harness = createPushRecoveryHarness();
    await harness.dispatchExtendableEvent("message", {
      data: {
        type: "t3-push-recovery-config",
        oldEndpoint: "https://push.example/old",
        recoveryToken: "initial-recovery-token",
        recoveryUrl: `${ORIGIN}/api/notifications/recover`,
        vapidPublicKey: "AQIDBA",
      },
    });

    await harness.dispatchExtendableEvent("pushsubscriptionchange", {
      oldSubscription: { endpoint: "https://push.example/intermediate" },
      newSubscription: null,
    });

    expect(harness.subscribeCalls).toHaveLength(1);
    expect(Array.from(harness.subscribeCalls[0]?.applicationServerKey ?? [])).toEqual([1, 2, 3, 4]);
    expect(harness.recoveryPosts).toHaveLength(1);
    expect(harness.recoveryPosts[0]?.init).toMatchObject({
      method: "POST",
      mode: "cors",
      credentials: "omit",
    });
    expect(JSON.parse(harness.recoveryPosts[0]?.body ?? "{}")).toEqual({
      oldEndpoint: "https://push.example/old",
      recoveryToken: "initial-recovery-token",
      newSubscription: {
        endpoint: "https://push.example/new",
        expirationTime: null,
        keys: { auth: "new-auth", p256dh: "new-p256dh" },
      },
    });
    expect(await harness.workerExports.readPushRecoveryConfig()).toEqual({
      oldEndpoint: "https://push.example/new",
      recoveryToken: "rotated-recovery-token",
      recoveryUrl: `${ORIGIN}/api/notifications/recover`,
      vapidPublicKey: "AQIDBA",
    });
  });

  it("does not let a delayed recovery response replace a newer page registration token", async () => {
    let finishRecovery!: () => void;
    let signalRecoveryStarted!: () => void;
    const recoveryStarted = new Promise<void>((resolve) => {
      signalRecoveryStarted = resolve;
    });
    const harness = createPushRecoveryHarness(async () => {
      signalRecoveryStarted();
      await new Promise<void>((resolve) => {
        finishRecovery = resolve;
      });
      return new Response(JSON.stringify({ recoveryToken: "stale-recovery-token" }), {
        headers: { "content-type": "application/json" },
      });
    });
    const initialConfig = {
      type: "t3-push-recovery-config",
      oldEndpoint: "https://push.example/old",
      recoveryToken: "initial-recovery-token",
      recoveryUrl: `${ORIGIN}/api/notifications/recover`,
      vapidPublicKey: "AQIDBA",
    };
    const freshConfig = {
      type: "t3-push-recovery-config",
      oldEndpoint: "https://push.example/page-registration",
      recoveryToken: "fresh-page-token",
      recoveryUrl: `${ORIGIN}/api/notifications/recover`,
      vapidPublicKey: "AQIDBA",
    };
    await harness.dispatchExtendableEvent("message", { data: initialConfig });

    const delayedRecovery = harness.dispatchExtendableEvent("pushsubscriptionchange", {
      oldSubscription: { endpoint: "https://push.example/intermediate" },
      newSubscription: null,
    });
    await recoveryStarted;
    await harness.dispatchExtendableEvent("message", { data: freshConfig });
    finishRecovery();
    await delayedRecovery;

    expect(await harness.workerExports.readPushRecoveryConfig()).toEqual({
      oldEndpoint: freshConfig.oldEndpoint,
      recoveryToken: freshConfig.recoveryToken,
      recoveryUrl: freshConfig.recoveryUrl,
      vapidPublicKey: freshConfig.vapidPublicKey,
    });
  });

  it("retries the identical recovery request when the first response is lost", async () => {
    let attempts = 0;
    const harness = createPushRecoveryHarness(async () => {
      attempts += 1;
      if (attempts === 1) throw new TypeError("recovery response lost");
      return new Response(JSON.stringify({ recoveryToken: "replayed-recovery-token" }), {
        headers: { "content-type": "application/json" },
      });
    });
    await harness.dispatchExtendableEvent("message", {
      data: {
        type: "t3-push-recovery-config",
        oldEndpoint: "https://push.example/old",
        recoveryToken: "initial-recovery-token",
        recoveryUrl: `${ORIGIN}/api/notifications/recover`,
        vapidPublicKey: "AQIDBA",
      },
    });

    await harness.dispatchExtendableEvent("pushsubscriptionchange", {
      oldSubscription: { endpoint: "https://push.example/intermediate" },
      newSubscription: null,
    });

    expect(harness.recoveryPosts).toHaveLength(2);
    expect(harness.recoveryPosts[1]?.body).toBe(harness.recoveryPosts[0]?.body);
    expect(await harness.workerExports.readPushRecoveryConfig()).toEqual({
      oldEndpoint: "https://push.example/new",
      recoveryToken: "replayed-recovery-token",
      recoveryUrl: `${ORIGIN}/api/notifications/recover`,
      vapidPublicKey: "AQIDBA",
    });
  });

  it("does not refresh the offline shell from excluded HTML endpoints", async () => {
    const originalShell = '<html><script type="module" src="/assets/current.js"></script></html>';
    const harness = createHarness(() => htmlResponse("<html>api html</html>"));
    const cache = await harness.cache();
    await cache.put("/", htmlResponse(originalShell));

    const result = await harness.dispatchFetch(navigationRequest("/api/debug"));

    expect(result.handled).toBe(false);
    expect(harness.fetchCalls).toEqual([]);
    expect(await cachedText(cache, "/")).toBe(originalShell);
  });

  it("refreshes the offline shell for SPA navigations", async () => {
    const nextShell = '<html><script type="module" src="/assets/app.js"></script></html>';
    const harness = createHarness((input) => {
      const pathname = pathnameFromInput(input);
      if (pathname === "/settings") {
        return htmlResponse(nextShell);
      }
      return assetResponse(`asset:${pathname}`);
    });
    const cache = await harness.cache();
    await cache.put("/", htmlResponse("<html>old shell</html>"));

    const result = await harness.dispatchFetch(navigationRequest("/settings"));

    expect(result.handled).toBe(true);
    expect(await result.response?.text()).toBe(nextShell);
    expect(await cachedText(cache, "/")).toBe(nextShell);
    expect(await cachedText(cache, "/assets/app.js")).toBeUndefined();
    expect(await cachedStorageText(harness.cacheStorage, "/assets/app.js")).toBe(
      "asset:/assets/app.js",
    );
  });

  it("removes superseded build-asset caches only when no windows are open", async () => {
    const harness = createHarness((input) => assetResponse(`asset:${pathnameFromInput(input)}`));
    const cache = await harness.cache();
    await cache.put("/", htmlResponse('<html><script src="/assets/current.js"></script></html>'));
    const currentAssetCacheName = await harness.workerExports.currentBuildAssetCacheName();
    const currentAssetCache = await harness.cacheStorage.open(currentAssetCacheName);
    await currentAssetCache.put("/assets/current.js", assetResponse("current"));
    const oldAssetCacheName = "t3-code-assets-old-build";
    const oldAssetCache = await harness.cacheStorage.open(oldAssetCacheName);
    await oldAssetCache.put("/assets/old.js", assetResponse("old"));

    harness.setWindowClientCount(1);
    await harness.workerExports.deleteSupersededCaches();
    expect(await harness.cacheStorage.keys()).toContain(oldAssetCacheName);

    harness.setWindowClientCount(0);
    await harness.workerExports.deleteSupersededCaches();

    const cacheNames = await harness.cacheStorage.keys();
    expect(harness.fetchCalls).toEqual([]);
    expect(cacheNames).toContain(CACHE_VERSION);
    expect(cacheNames).toContain(currentAssetCacheName);
    expect(cacheNames).not.toContain(oldAssetCacheName);
    expect(await cachedStorageText(harness.cacheStorage, "/assets/current.js")).toBe("current");
    expect(await harness.cacheStorage.match("/assets/old.js")).toBeUndefined();
  });
});
