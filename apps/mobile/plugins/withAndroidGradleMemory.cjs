const { withGradleProperties } = require("expo/config-plugins");

const DEFAULT_GRADLE_JVM_ARGS =
  "-Xmx4096m -XX:MaxMetaspaceSize=1024m -XX:+HeapDumpOnOutOfMemoryError";

module.exports = function withAndroidGradleMemory(config, options = {}) {
  const jvmArgs =
    typeof options.jvmArgs === "string" && options.jvmArgs.trim().length > 0
      ? options.jvmArgs.trim()
      : DEFAULT_GRADLE_JVM_ARGS;

  return withGradleProperties(config, (nextConfig) => {
    upsertGradleProperty(nextConfig.modResults, "org.gradle.jvmargs", jvmArgs);
    return nextConfig;
  });
};

function upsertGradleProperty(properties, key, value) {
  const existing = properties.find(
    (property) => property.type === "property" && property.key === key,
  );

  if (existing != null) {
    existing.value = value;
    return;
  }

  properties.push({
    type: "property",
    key,
    value,
  });
}
