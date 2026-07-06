import { describe, expect, it } from "vite-plus/test";

import {
  COMPOSER_ATTACHMENT_FORMAT_LABEL,
  COMPOSER_ATTACHMENT_INPUT_ACCEPT,
  inferComposerImageMimeType,
  normalizeComposerImageFile,
} from "./composerAttachments";

describe("composerAttachments", () => {
  it("advertises image files to the platform picker", () => {
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).toContain("image/png");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).toContain("image/jpeg");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).toContain(".png");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).toContain(".jpg");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).not.toContain("image/*");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).not.toContain(".heic");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).not.toContain(".svg");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).not.toContain(".pdf");
    expect(COMPOSER_ATTACHMENT_INPUT_ACCEPT).not.toContain(".md");
  });

  it("describes the attachment formats accepted by providers", () => {
    expect(COMPOSER_ATTACHMENT_FORMAT_LABEL).toBe("PNG, JPEG, GIF, or WebP images");
  });

  it("uses the browser-reported image MIME type when present", () => {
    const file = new File(["x"], "screenshot.bin", { type: "image/png" });

    expect(inferComposerImageMimeType(file)).toBe("image/png");
  });

  it("normalizes browser-reported image/jpg MIME types", () => {
    const file = new File(["x"], "screenshot.bin", { type: "image/jpg" });

    expect(inferComposerImageMimeType(file)).toBe("image/jpeg");
  });

  it("falls back to image file extensions for mobile pickers that omit MIME types", () => {
    const file = new File(["x"], "screenshot.JPG", { type: "" });

    expect(inferComposerImageMimeType(file)).toBe("image/jpeg");
  });

  it("returns null for non-image files", () => {
    const file = new File(["x"], "notes.pdf", { type: "application/pdf" });

    expect(inferComposerImageMimeType(file)).toBeNull();
  });

  it("does not override explicit non-image MIME types with image extensions", () => {
    const file = new File(["x"], "notes.jpg", { type: "application/pdf" });

    expect(inferComposerImageMimeType(file)).toBeNull();
  });

  it("returns null for provider-unsupported image MIME types", () => {
    const file = new File(["x"], "photo.heic", { type: "image/heic" });

    expect(inferComposerImageMimeType(file)).toBeNull();
  });

  it("rewrites empty browser MIME types so FileReader emits an image data URL", async () => {
    const file = new File(["image-bytes"], "screenshot.jpg", { type: "" });

    const normalized = normalizeComposerImageFile(file, "image/jpeg");

    expect(normalized).not.toBe(file);
    expect(normalized.name).toBe("screenshot.jpg");
    expect(normalized.type).toBe("image/jpeg");
    await expect(normalized.text()).resolves.toBe("image-bytes");
  });
});
