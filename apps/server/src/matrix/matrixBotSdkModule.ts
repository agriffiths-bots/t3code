/**
 * The Matrix SDK, bundled as one file for the packaged server.
 *
 * The SDK's dependency tree is around four thousand files, which a desktop
 * artifact must not carry loose: Windows packages unpack their whole
 * `node_modules` so the WSL backend can read it. Packing the SDK here keeps
 * that tree inside one bundled module, and keeps this import out of the main
 * bundle, so a server with no bridge configured never loads it and never
 * touches the native crypto binding it pulls in.
 *
 * Loaded through `MatrixBotSdkClient`'s loader, never imported directly.
 */
export {
  LogService,
  MatrixClient,
  RustSdkCryptoStorageProvider,
  SimpleFsStorageProvider,
} from "matrix-bot-sdk";
