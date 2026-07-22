import type { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import { Platform, Pressable, ScrollView } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { ProjectFavicon } from "../../components/ProjectFavicon";
import { cn } from "../../lib/cn";
import { scopedProjectKey } from "../../lib/scopedEntities";

export function ThreadListV2ProjectScope(props: {
  readonly projects: ReadonlyArray<EnvironmentProject>;
  readonly selectedKey: string | null;
  readonly onChange: (key: string | null) => void;
}) {
  if (props.projects.length === 0) return null;

  return (
    <ScrollView
      horizontal
      contentContainerStyle={{
        gap: 8,
        paddingHorizontal: 16,
        paddingBottom: 8,
        paddingTop: Platform.OS === "ios" ? 12 : 4,
      }}
      keyboardShouldPersistTaps="handled"
      showsHorizontalScrollIndicator={false}
    >
      {props.projects.length > 1 ? (
        <Pressable
          accessibilityLabel="Show all threads"
          accessibilityRole="button"
          accessibilityState={{ selected: props.selectedKey === null }}
          hitSlop={4}
          onPress={() => props.onChange(null)}
          className={cn(
            "min-h-8 items-center justify-center rounded-lg border px-3",
            props.selectedKey === null
              ? "border-border bg-subtle-strong"
              : "border-black/15 dark:border-white/15",
          )}
        >
          <Text className="text-sm font-t3-medium text-foreground">All</Text>
        </Pressable>
      ) : null}
      {props.projects.map((project) => {
        const key = scopedProjectKey(project.environmentId, project.id);
        const selected = props.selectedKey === key;
        return (
          <Pressable
            key={key}
            accessibilityLabel={`Show ${project.title} threads`}
            accessibilityRole="button"
            accessibilityState={{ selected }}
            hitSlop={4}
            onPress={() => props.onChange(selected ? null : key)}
            className={cn(
              "min-h-8 flex-row items-center gap-1.5 rounded-lg border py-1 pl-2 pr-3",
              selected ? "border-border bg-subtle-strong" : "border-black/15 dark:border-white/15",
            )}
          >
            <ProjectFavicon
              environmentId={project.environmentId}
              projectTitle={project.title}
              size={15}
              workspaceRoot={project.workspaceRoot}
            />
            <Text
              className={cn(
                "max-w-36 text-sm font-t3-medium",
                selected ? "text-foreground" : "text-foreground-muted",
              )}
              numberOfLines={1}
            >
              {project.title}
            </Text>
          </Pressable>
        );
      })}
    </ScrollView>
  );
}
