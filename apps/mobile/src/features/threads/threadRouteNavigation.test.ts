import { describe, expect, it, vi } from "vite-plus/test";

import { navigateBackOrHome } from "./threadRouteNavigation";

describe("navigateBackOrHome", () => {
  it("goes back when the thread route has navigation history", () => {
    const goBack = vi.fn();
    const goHome = vi.fn();

    navigateBackOrHome({ canGoBack: () => true, goBack, goHome });

    expect(goBack).toHaveBeenCalledOnce();
    expect(goHome).not.toHaveBeenCalled();
  });

  it("routes home when the thread route is the navigation root", () => {
    const goBack = vi.fn();
    const goHome = vi.fn();

    navigateBackOrHome({ canGoBack: () => false, goBack, goHome });

    expect(goBack).not.toHaveBeenCalled();
    expect(goHome).toHaveBeenCalledOnce();
  });
});
