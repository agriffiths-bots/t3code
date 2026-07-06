const COMPOSER_IMAGE_MIME_BY_EXTENSION: Readonly<Record<string, string>> = {
  gif: "image/gif",
  jpeg: "image/jpeg",
  jpg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
};

const COMPOSER_SUPPORTED_IMAGE_MIME_TYPES = new Set([
  "image/gif",
  "image/jpeg",
  "image/png",
  "image/webp",
]);

export const COMPOSER_ATTACHMENT_INPUT_ACCEPT = [
  ...COMPOSER_SUPPORTED_IMAGE_MIME_TYPES,
  ...Object.keys(COMPOSER_IMAGE_MIME_BY_EXTENSION).map((extension) => `.${extension}`),
].join(",");
export const COMPOSER_ATTACHMENT_FORMAT_LABEL = "PNG, JPEG, GIF, or WebP images";

function extensionFromFileName(name: string): string {
  const dotIndex = name.lastIndexOf(".");
  return dotIndex >= 0 ? name.slice(dotIndex + 1).toLowerCase() : "";
}

export function inferComposerImageMimeType(file: File): string | null {
  const explicitType = file.type.trim().toLowerCase();
  if (explicitType.length > 0) {
    const normalizedType = explicitType === "image/jpg" ? "image/jpeg" : explicitType;
    return COMPOSER_SUPPORTED_IMAGE_MIME_TYPES.has(normalizedType) ? normalizedType : null;
  }

  return COMPOSER_IMAGE_MIME_BY_EXTENSION[extensionFromFileName(file.name)] ?? null;
}

export function normalizeComposerImageFile(file: File, mimeType: string): File {
  if (file.type.trim().toLowerCase() === mimeType) {
    return file;
  }

  return new File([file], file.name || "image", {
    type: mimeType,
    lastModified: file.lastModified,
  });
}
