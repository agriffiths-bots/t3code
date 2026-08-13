export interface HostedDisplayUrlAllowlistEntry {
  readonly url: string;
  readonly sourceFile: string;
  readonly rationale: string;
}

export const HOSTED_DISPLAY_URL_MARKER_PREFIX = "__T3_DISPLAY_ONLY_URL_SOURCE__";
export const HOSTED_DISPLAY_URL_MARKER_SEPARATOR = "__T3_DISPLAY_ONLY_URL_VALUE__";
export const HOSTED_DISPLAY_URL_MARKER_SUFFIX = "__T3_DISPLAY_ONLY_URL_END__";

/**
 * Bind an emitted display literal to its reviewed source declaration without
 * relying on source maps, which are intentionally absent from some builds.
 */
export function hostedDisplayUrlMarker(sourceFile: string, url: string): string {
  return (
    HOSTED_DISPLAY_URL_MARKER_PREFIX +
    sourceFile +
    HOSTED_DISPLAY_URL_MARKER_SEPARATOR +
    url +
    HOSTED_DISPLAY_URL_MARKER_SUFFIX
  );
}

/**
 * Audited display-only URL literals allowed in hosted web assets.
 *
 * The build verifies every entry against its declared source file before the
 * asset scanner can use it. Keep this list limited to exact UI copy: runtime
 * endpoints and broad host or port exemptions do not belong here.
 */
export const HOSTED_DISPLAY_URL_ALLOWLIST = [
  {
    url: "http://127.0.0.1:5173/",
    sourceFile: "apps/web/src/components/settings/SettingsFontPreviews.tsx",
    rationale:
      "The terminal-font settings preview renders a static, non-interactive Vite startup transcript so users can judge glyphs, colours, and decoration; it is never read as connection or backend configuration.",
  },
  {
    url: "http://localhost:5173",
    sourceFile: "apps/web/src/components/projectScriptEditor.tsx",
    rationale:
      "The project-script editor uses this exact value only as placeholder copy illustrating the optional preview URL field; it is not assigned to state, fetched, opened, or used as a backend endpoint.",
  },
] as const satisfies ReadonlyArray<HostedDisplayUrlAllowlistEntry>;
