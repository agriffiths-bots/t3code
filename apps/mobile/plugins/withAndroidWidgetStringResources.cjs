const { AndroidConfig, withStringsXml } = require("expo/config-plugins");

module.exports = function withAndroidWidgetStringResources(config, options = {}) {
  const widgets = Array.isArray(options.widgets) ? options.widgets : [];

  return withStringsXml(config, (nextConfig) => {
    const stringItems = widgets.flatMap((widget) => getWidgetStringItems(widget));

    if (stringItems.length > 0) {
      nextConfig.modResults = AndroidConfig.Strings.setStringItem(
        stringItems,
        nextConfig.modResults,
      );
    }

    return nextConfig;
  });
};

function getWidgetStringItems(widget) {
  const name = readNonEmptyString(widget, "name");
  const displayName = readNonEmptyString(widget, "displayName");
  const description = readNonEmptyString(widget, "description");
  const resourceName = toAndroidResourceName(name);

  return [
    AndroidConfig.Resources.buildResourceItem({
      name: `${resourceName}_display_name`,
      value: displayName,
    }),
    AndroidConfig.Resources.buildResourceItem({
      name: `${resourceName}_description`,
      value: description,
    }),
  ];
}

function readNonEmptyString(widget, key) {
  const value = widget?.[key];

  if (typeof value === "string" && value.trim().length > 0) {
    return value.trim();
  }

  throw new Error(`Android widget ${key} must be a non-empty string.`);
}

function toAndroidResourceName(name) {
  const resourceName = name
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^A-Za-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();

  return /^[a-z]/.test(resourceName) ? resourceName : `widget_${resourceName}`;
}
