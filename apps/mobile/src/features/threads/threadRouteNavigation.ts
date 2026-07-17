export function navigateBackOrHome(input: {
  readonly canGoBack: () => boolean;
  readonly goBack: () => void;
  readonly goHome: () => void;
}): void {
  if (input.canGoBack()) {
    input.goBack();
    return;
  }

  input.goHome();
}
