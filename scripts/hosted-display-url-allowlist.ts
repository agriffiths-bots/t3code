export interface HostedDisplayUrlAllowlistEntry {
  readonly url: string;
  readonly sourceFile: string;
  readonly rationale: string;
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
