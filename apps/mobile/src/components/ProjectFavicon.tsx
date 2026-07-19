import { SymbolView } from "./AppSymbol";
import { Image } from "expo-image";
import { useState } from "react";
import { View } from "react-native";
import type { EnvironmentId } from "@t3tools/contracts";
import { isProjectFaviconFallbackUrl } from "@t3tools/shared/projectFavicon";
import { useThemeColor } from "../lib/useThemeColor";
import { type AssetRequestSource, useAssetRequestSource } from "../state/assets";

/* ─── Favicon cache (matches web pattern) ────────────────────────────── */
const loadedFaviconUrls = new Set<string>();

/* ─── Component ──────────────────────────────────────────────────────── */
export function ProjectFavicon(props: {
  readonly environmentId: EnvironmentId;
  readonly open?: boolean;
  readonly size?: number;
  readonly projectTitle: string;
  readonly workspaceRoot?: string | null;
}) {
  const size = props.size ?? 42;
  const faviconSource = useAssetRequestSource(
    props.environmentId,
    props.workspaceRoot === null || props.workspaceRoot === undefined
      ? null
      : { _tag: "project-favicon", cwd: props.workspaceRoot },
  );
  const renderableFaviconSource = isProjectFaviconFallbackUrl(faviconSource?.uri ?? null)
    ? null
    : faviconSource;

  return (
    <ProjectFaviconImage
      key={faviconSource?.uri}
      faviconSource={renderableFaviconSource}
      open={props.open}
      projectTitle={props.projectTitle}
      size={size}
    />
  );
}

function ProjectFaviconImage(props: {
  readonly faviconSource: AssetRequestSource | null;
  readonly open?: boolean;
  readonly projectTitle: string;
  readonly size: number;
}) {
  const iconMuted = useThemeColor("--color-icon-subtle");

  const [status, setStatus] = useState<"loading" | "loaded" | "error">(() =>
    props.faviconSource && loadedFaviconUrls.has(props.faviconSource.uri) ? "loaded" : "loading",
  );

  const showImage = props.faviconSource !== null && status === "loaded";

  return (
    <View
      style={{
        width: props.size,
        height: props.size,
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      {/* Folder icon fallback (matches web's FolderIcon) */}
      {!showImage ? (
        <SymbolView
          name={{ ios: "folder.fill", android: props.open ? "folder_open" : "folder" }}
          size={props.size * 0.78}
          tintColor={iconMuted}
          type="monochrome"
        />
      ) : null}

      {/* Favicon image (hidden until loaded) */}
      {props.faviconSource ? (
        <Image
          source={{
            uri: props.faviconSource.uri,
            headers: props.faviconSource.headers,
          }}
          accessibilityLabel={`${props.projectTitle} favicon`}
          style={{
            width: props.size,
            height: props.size,
            borderRadius: props.size * 0.16,
            ...(showImage ? {} : { position: "absolute" as const, opacity: 0 }),
          }}
          contentFit="contain"
          onLoad={() => {
            if (props.faviconSource) loadedFaviconUrls.add(props.faviconSource.uri);
            setStatus("loaded");
          }}
          onError={() => setStatus("error")}
        />
      ) : null}
    </View>
  );
}
