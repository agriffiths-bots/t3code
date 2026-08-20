import { createFileRoute, redirect, useNavigate } from "@tanstack/react-router";

import {
  AuthenticatedPairingApplySurface,
  DesktopLocalPairingSurface,
  HostedPairingRouteSurface,
  PairingPendingSurface,
  PairingRouteSurface,
} from "../components/auth/PairingRouteSurface";
import { peekPairingTokenFromUrl } from "../environments/primary";
import { pairRouteDisposition } from "./pair.logic";

export const Route = createFileRoute("/pair")({
  beforeLoad: async ({ context }) => {
    const { authGateState } = context;
    const disposition = pairRouteDisposition({
      authStatus: authGateState.status,
      pairingToken: peekPairingTokenFromUrl(),
      isDesktop: window.desktopBridge !== undefined,
    });

    if (disposition === "redirect-home") {
      throw redirect({ to: "/", replace: true });
    }

    return {
      authGateState,
      pairDisposition: disposition,
    };
  },
  component: PairRouteView,
  pendingComponent: PairRoutePendingView,
});

function PairRouteView() {
  const { authGateState, pairDisposition } = Route.useRouteContext();
  const navigate = useNavigate();

  if (!authGateState) {
    return null;
  }

  if (pairDisposition === "hosted-pairing" || authGateState.status === "hosted-pairing") {
    return <HostedPairingRouteSurface />;
  }

  const goHome = () => {
    void navigate({ to: "/", replace: true });
  };

  if (pairDisposition === "desktop-local-session") {
    return <DesktopLocalPairingSurface onContinue={goHome} />;
  }

  if (pairDisposition === "apply-pairing-credential" || authGateState.status === "authenticated") {
    return (
      <AuthenticatedPairingApplySurface
        onAuthenticated={() => {
          window.location.replace("/");
        }}
        onContinueWithoutApplying={goHome}
      />
    );
  }

  if (authGateState.status !== "requires-auth") {
    return null;
  }

  return (
    <PairingRouteSurface
      auth={authGateState.auth}
      onAuthenticated={goHome}
      {...(authGateState.errorMessage ? { initialErrorMessage: authGateState.errorMessage } : {})}
    />
  );
}

function PairRoutePendingView() {
  return <PairingPendingSurface />;
}
