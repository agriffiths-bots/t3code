export function shouldSubmitComposerOnEnter(input: {
  isMobileViewport: boolean;
  shiftKey: boolean;
}): boolean {
  if (input.shiftKey) return false;
  return !input.isMobileViewport;
}
