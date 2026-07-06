import { describe, expect, it } from "vite-plus/test";
import { Children, isValidElement, type ReactElement, type ReactNode } from "react";

import { PwaRuntimeView, PwaServiceWorkerRegistration, resolveDeepLinkTarget } from "./pwaRuntime";

describe("resolveDeepLinkTarget", () => {
  it("keeps notification deep links as pathnames for browser history", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "browser")).toBe("/env-1/thread-1");
  });

  it("routes notification deep links through hash history for Electron", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "hash")).toBe("#/env-1/thread-1");
  });

  it("rejects missing, non-path, and protocol-relative deep links", () => {
    expect(resolveDeepLinkTarget(undefined, "hash")).toBeNull();
    expect(resolveDeepLinkTarget("https://example.com/env-1/thread-1", "hash")).toBeNull();
    expect(resolveDeepLinkTarget("//example.com/env-1/thread-1", "hash")).toBeNull();
  });
});

describe("PwaRuntimeView", () => {
  it("registers the app shell service worker before an environment exists", () => {
    const runtime = PwaRuntimeView({ enabled: true, environmentId: null });

    expect(isValidElement(runtime)).toBe(true);
    const children = Children.toArray(
      (runtime as ReactElement<{ readonly children: ReactNode }>).props.children,
    );
    expect(children).toHaveLength(1);
    expect(isValidElement(children[0]) && children[0].type).toBe(PwaServiceWorkerRegistration);
  });

  it("does not mount browser PWA side effects for unsupported runtimes", () => {
    expect(PwaRuntimeView({ enabled: false, environmentId: null })).toBeNull();
  });
});
