import { Easing, LinearTransition } from "react-native-reanimated";

/**
 * Animates between final Yoga layouts on the UI thread. Keeping pane widths
 * out of animated styles avoids recalculating the entire workspace on every
 * display frame while a sidebar or inspector moves.
 */
export const WORKSPACE_PANE_LAYOUT_TRANSITION = LinearTransition.duration(220).easing(
  Easing.out(Easing.cubic),
);
