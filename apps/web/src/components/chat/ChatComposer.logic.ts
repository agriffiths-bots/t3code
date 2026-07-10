export function shouldSubmitComposerOnEnter(input: {
  hasCoarsePointer: boolean;
  shiftKey: boolean;
}): boolean {
  if (input.shiftKey) return false;
  return !input.hasCoarsePointer;
}
