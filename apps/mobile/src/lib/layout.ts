function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/**
 * Use available space, not device or orientation labels, to choose the shell.
 *
 * The height floor deliberately keeps every current iPhone in the compact shell
 * when it rotates to landscape, while still allowing iPad and foldable-sized
 * windows to adopt the persistent sidebar as they resize.
 */
export const SPLIT_LAYOUT_MIN_WIDTH = 720;
export const SPLIT_LAYOUT_MIN_HEIGHT = 600;

const SPLIT_SIDEBAR_MIN_WIDTH = 280;
const SPLIT_SIDEBAR_MAX_WIDTH = 380;

export const AUXILIARY_PANE_MIN_CONTENT_WIDTH = 960;
export const CHAT_CONTENT_MAX_WIDTH = 960;

const AUXILIARY_PANE_MIN_WIDTH = 260;
const AUXILIARY_PANE_MAX_WIDTH = 320;
const FILE_INSPECTOR_MIN_VIEWPORT_WIDTH = 820;
const FILE_INSPECTOR_MIN_MAIN_WIDTH = 560;
const STABLE_FORM_SHEET_MAX_HEIGHT = 720;
const STABLE_FORM_SHEET_VERTICAL_MARGIN = 64;
const STABLE_FORM_SHEET_MIN_DETENT = 0.62;
const STABLE_FORM_SHEET_MAX_DETENT = 0.92;

export type LayoutVariant = "compact" | "split";

export interface Layout {
  readonly variant: LayoutVariant;
  readonly usesSplitView: boolean;
  readonly listPaneWidth: number | null;
  readonly shellPadding: number;
}

export interface WorkspacePaneLayout {
  readonly primarySidebarVisible: boolean;
  readonly primarySidebarSuppressedByAuxiliary: boolean;
  readonly contentPaneWidth: number;
  readonly supportsAuxiliaryPane: boolean;
  readonly auxiliaryPaneVisible: boolean;
  readonly auxiliaryPaneWidth: number | null;
}

export interface FileInspectorPaneLayout {
  readonly supported: boolean;
  readonly width: number | null;
}

export type WorkspaceAuxiliaryPaneRole = "supplementary" | "inspector";

export function deriveLayout(input: { readonly width: number; readonly height: number }): Layout {
  const { width, height } = input;
  const wideEnoughForSplit = width >= SPLIT_LAYOUT_MIN_WIDTH && height >= SPLIT_LAYOUT_MIN_HEIGHT;

  if (!wideEnoughForSplit) {
    return {
      variant: "compact",
      usesSplitView: false,
      listPaneWidth: null,
      shellPadding: 0,
    };
  }

  return {
    variant: "split",
    usesSplitView: true,
    listPaneWidth: clamp(
      Math.round(width * 0.32),
      SPLIT_SIDEBAR_MIN_WIDTH,
      SPLIT_SIDEBAR_MAX_WIDTH,
    ),
    shellPadding: 0,
  };
}

export function deriveWorkspacePaneLayout(input: {
  readonly layout: Layout;
  readonly viewportWidth: number;
  readonly primarySidebarPreferredVisible: boolean;
  readonly auxiliaryPanePreferredVisible: boolean;
  readonly auxiliaryPaneRole?: WorkspaceAuxiliaryPaneRole;
}): WorkspacePaneLayout {
  const viewportWidth = Math.max(0, input.viewportWidth);
  const auxiliaryPaneRole = input.auxiliaryPaneRole ?? "supplementary";
  const preferredPrimarySidebarVisible =
    input.layout.usesSplitView && input.primarySidebarPreferredVisible;
  const preferredPrimarySidebarWidth = preferredPrimarySidebarVisible
    ? (input.layout.listPaneWidth ?? 0)
    : 0;

  if (auxiliaryPaneRole === "inspector") {
    const fileInspector = deriveFileInspectorPaneLayout({
      layout: input.layout,
      viewportWidth,
    });
    const auxiliaryPaneVisible = fileInspector.supported && input.auxiliaryPanePreferredVisible;
    const primarySidebarSuppressedByAuxiliary =
      auxiliaryPaneVisible &&
      fileInspector.width !== null &&
      input.layout.listPaneWidth !== null &&
      viewportWidth - input.layout.listPaneWidth - fileInspector.width <
        FILE_INSPECTOR_MIN_MAIN_WIDTH;
    const primarySidebarVisible =
      preferredPrimarySidebarVisible && !primarySidebarSuppressedByAuxiliary;
    const primarySidebarWidth = primarySidebarVisible ? (input.layout.listPaneWidth ?? 0) : 0;

    return {
      primarySidebarVisible,
      primarySidebarSuppressedByAuxiliary,
      contentPaneWidth: Math.max(0, viewportWidth - primarySidebarWidth),
      supportsAuxiliaryPane: fileInspector.supported,
      auxiliaryPaneVisible,
      auxiliaryPaneWidth: fileInspector.width,
    };
  }

  const contentPaneWidth = Math.max(0, viewportWidth - preferredPrimarySidebarWidth);
  const supportsAuxiliaryPane =
    input.layout.usesSplitView && contentPaneWidth >= AUXILIARY_PANE_MIN_CONTENT_WIDTH;
  const auxiliaryPaneVisible = supportsAuxiliaryPane && input.auxiliaryPanePreferredVisible;

  return {
    primarySidebarVisible: preferredPrimarySidebarVisible,
    primarySidebarSuppressedByAuxiliary: false,
    contentPaneWidth,
    supportsAuxiliaryPane,
    auxiliaryPaneVisible,
    auxiliaryPaneWidth: supportsAuxiliaryPane
      ? clamp(
          Math.round(contentPaneWidth * 0.28),
          AUXILIARY_PANE_MIN_WIDTH,
          AUXILIARY_PANE_MAX_WIDTH,
        )
      : null,
  };
}

export function deriveFileInspectorPaneLayout(input: {
  readonly layout: Layout;
  readonly viewportWidth: number;
}): FileInspectorPaneLayout {
  const viewportWidth = Math.max(0, input.viewportWidth);
  const supported =
    input.layout.usesSplitView && viewportWidth >= FILE_INSPECTOR_MIN_VIEWPORT_WIDTH;

  return {
    supported,
    width: supported
      ? clamp(Math.round(viewportWidth * 0.28), AUXILIARY_PANE_MIN_WIDTH, AUXILIARY_PANE_MAX_WIDTH)
      : null,
  };
}

export function deriveCenteredContentHorizontalPadding(input: {
  readonly viewportWidth: number;
  readonly maxContentWidth: number | null;
  readonly minimumPadding: number;
}): number {
  const viewportWidth = Number.isFinite(input.viewportWidth) ? Math.max(0, input.viewportWidth) : 0;
  const minimumPadding = Number.isFinite(input.minimumPadding)
    ? Math.max(0, input.minimumPadding)
    : 0;

  if (
    input.maxContentWidth === null ||
    !Number.isFinite(input.maxContentWidth) ||
    input.maxContentWidth <= 0
  ) {
    return minimumPadding;
  }

  return minimumPadding + Math.max(0, (viewportWidth - input.maxContentWidth) / 2);
}

export function deriveStableFormSheetDetent(containerHeight: number): number {
  if (!Number.isFinite(containerHeight) || containerHeight <= 0) {
    return STABLE_FORM_SHEET_MAX_DETENT;
  }

  const targetHeight = Math.min(
    STABLE_FORM_SHEET_MAX_HEIGHT,
    Math.max(0, containerHeight - STABLE_FORM_SHEET_VERTICAL_MARGIN),
  );
  const detent = clamp(
    targetHeight / containerHeight,
    STABLE_FORM_SHEET_MIN_DETENT,
    STABLE_FORM_SHEET_MAX_DETENT,
  );
  return Math.round(detent * 1_000) / 1_000;
}
