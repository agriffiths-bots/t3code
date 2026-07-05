/**
 * Fenced code-block language that marks a self-contained generative-UI
 * artifact (```genui). Kept in its own tiny, dependency-free module so
 * `ChatMarkdown` can check for the fence on every render without statically
 * pulling in the artifact renderer and its HTML sanitizer — those load lazily
 * only when a `genui` block is actually rendered.
 */
export const GENUI_FENCE_LANGUAGE = "genui";
