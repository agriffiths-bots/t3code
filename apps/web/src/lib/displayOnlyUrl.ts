const MARKER_PREFIX = "__T3_DISPLAY_ONLY_URL_SOURCE__";
const MARKER_SEPARATOR = "__T3_DISPLAY_ONLY_URL_VALUE__";
const MARKER_SUFFIX = "__T3_DISPLAY_ONLY_URL_END__";

/**
 * Strip scanner provenance from audited display copy before it reaches the UI.
 * Marker grammar is duplicated in the build scanner deliberately: drift makes
 * the build fail closed instead of allowing an unproven loopback literal.
 */
export function displayOnlyUrl(markedLiteral: string): string {
  const separatorIndex = markedLiteral.indexOf(MARKER_SEPARATOR);
  if (
    !markedLiteral.startsWith(MARKER_PREFIX) ||
    separatorIndex < MARKER_PREFIX.length ||
    !markedLiteral.endsWith(MARKER_SUFFIX)
  ) {
    throw new Error("Invalid audited display-only URL marker");
  }
  return markedLiteral.slice(separatorIndex + MARKER_SEPARATOR.length, -MARKER_SUFFIX.length);
}
