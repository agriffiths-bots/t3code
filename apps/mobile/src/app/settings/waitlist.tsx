import Constants from "expo-constants";
import { Stack } from "expo-router";
import { ScrollView, Text, View } from "react-native";

import { CloudWaitlistEnrollment } from "../../features/cloud/CloudWaitlistEnrollment";
import { useNativeClerkAuthModal } from "../../features/cloud/useNativeClerkAuthModal";
import { useThemeColor } from "../../lib/useThemeColor";

function hasClerkConfig(): boolean {
  const clerkConfig = Constants.expoConfig?.extra?.clerk as
    | { readonly publishableKey?: string | null }
    | undefined;
  return Boolean(clerkConfig?.publishableKey);
}

export default function SettingsWaitlistRouteScreen() {
  const { presentAuth } = useNativeClerkAuthModal();
  const foreground = String(useThemeColor("--color-foreground"));
  const secondaryForeground = String(useThemeColor("--color-foreground-secondary"));

  return (
    <>
      <Stack.Screen options={{ title: "Join the waitlist" }} />
      <ScrollView
        automaticallyAdjustKeyboardInsets
        contentContainerStyle={{
          paddingBottom: 32,
          paddingHorizontal: 20,
          paddingTop: 12,
        }}
        contentInsetAdjustmentBehavior="automatic"
        keyboardDismissMode="interactive"
        keyboardShouldPersistTaps="handled"
        showsVerticalScrollIndicator={false}
      >
        {hasClerkConfig() ? (
          <CloudWaitlistEnrollment onSignIn={() => void presentAuth()} />
        ) : (
          <View className="gap-3 px-4">
            <Text
              style={{
                color: foreground,
                fontFamily: "DMSans_700Bold",
                fontSize: 22,
                textAlign: "center",
              }}
            >
              T3 Cloud is not configured
            </Text>
            <Text
              selectable
              style={{
                color: secondaryForeground,
                fontFamily: "DMSans_400Regular",
                fontSize: 16,
                lineHeight: 22,
                textAlign: "center",
              }}
            >
              Add T3CODE_CLERK_PUBLISHABLE_KEY to this build to enable waitlist enrollment.
            </Text>
          </View>
        )}
      </ScrollView>
    </>
  );
}
