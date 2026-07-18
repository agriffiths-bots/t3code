import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  DEFAULT_AUTOMATIC_GIT_FETCH_INTERVAL,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthReviewWriteScope,
  AuthRelayWriteScope,
  AuthTerminalOperateScope,
  AuthAccessReadScope,
  AuthAccessStreamError,
  type AuthAccessStreamEvent,
  type AuthEnvironmentScope,
  type AssetResource,
  AuthSessionId,
  type DiscoveredLocalServerList,
  type OrchestrationCommand,
  type GitActionProgressEvent,
  type GitManagerServiceError,
  GitCommandError,
  VcsRepositoryDetectionError,
  NonNegativeInt,
  OrchestrationDispatchCommandError,
  type OrchestrationEvent,
  type OrchestrationShellStreamItem,
  type OrchestrationThreadStreamItem,
  OrchestrationGetFullThreadDiffError,
  OrchestrationGetSnapshotError,
  OrchestrationGetTurnDiffError,
  OrchestrationScheduledTaskMutationError,
  ORCHESTRATION_WS_METHODS,
  type ProjectEntriesFailure,
  type ProjectFileFailure,
  type ProjectFileOperation,
  ProjectListEntriesError,
  ProjectReadFileError,
  ProjectSearchEntriesError,
  ProjectWriteFileError,
  RelayClientInstallFailedError,
  type RelayClientInstallProgressEvent,
  type ScheduledTaskId,
  OrchestrationReplayEventsError,
  type FilesystemBrowseFailure,
  FilesystemBrowseError,
  AssetWorkspaceContextNotFoundError,
  AssetWorkspaceContextResolutionError,
  EnvironmentAuthorizationError,
  EnvironmentAuthenticatedPrincipal,
  ThreadId,
  type TerminalAttachStreamEvent,
  type TerminalError,
  type TerminalEvent,
  type TerminalMetadataStreamEvent,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { clamp } from "effect/Number";
import { HttpRouter, HttpServerRequest, HttpServerRespondable } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";

import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as ServerConfig from "./config.ts";
import * as Keybindings from "./keybindings.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import {
  cleanupPersistedCommandAttachments,
  normalizeDispatchCommand,
} from "./orchestration/Normalizer.ts";
import * as OrchestrationEngine from "./orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import { scopeScheduledTaskStreamForAudience } from "./orchestration/scheduledTaskAudienceStream.ts";
import { coveredThreadRevision } from "./orchestration/threadRevision.ts";
import { ScheduledTaskRepository, toScheduleEntry } from "./persistence/Services/ScheduledTasks.ts";
import { OrchestrationEventStore } from "./persistence/Services/OrchestrationEventStore.ts";
import {
  observeRpcEffect as instrumentRpcEffect,
  observeRpcStream as instrumentRpcStream,
  observeRpcStreamEffect as instrumentRpcStreamEffect,
} from "./observability/RpcInstrumentation.ts";
import * as ProviderRegistry from "./provider/Services/ProviderRegistry.ts";
import * as ProviderMaintenanceRunner from "./provider/providerMaintenanceRunner.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as PlanUsageSnapshot from "./usage/PlanUsageSnapshot.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as PreviewManager from "./preview/Manager.ts";
import { issueAssetUrl } from "./assets/AssetAccess.ts";
import { parseThreadSegmentFromAttachmentId } from "./attachmentStore.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as ProjectFilesystemAudienceGuard from "./project/ProjectFilesystemAudienceGuard.ts";
import * as BootstrapTurnStartDispatcher from "./orchestration/Services/BootstrapTurnStartDispatcher.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import {
  isAudienceScopedReadRpcMethod,
  isRpcMethodAllowedForAudienceCeiling,
} from "./auth/audienceScopePolicy.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as DeviceNotifications from "./notifications/DeviceNotifications.ts";
import * as SourceControlDiscovery from "./sourceControl/SourceControlDiscovery.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import * as AzureDevOpsCli from "./sourceControl/AzureDevOpsCli.ts";
import * as BitbucketApi from "./sourceControl/BitbucketApi.ts";
import * as GitHubCli from "./sourceControl/GitHubCli.ts";
import * as GitLabCli from "./sourceControl/GitLabCli.ts";
import * as SourceControlProviderRegistry from "./sourceControl/SourceControlProviderRegistry.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "./vcs/VcsDriverRegistry.ts";
import * as VcsProjectConfig from "./vcs/VcsProjectConfig.ts";
import * as VcsProcess from "./vcs/VcsProcess.ts";
import * as PairingGrantStore from "./auth/PairingGrantStore.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import { failEnvironmentAuthInvalid, failEnvironmentInternal } from "./auth/http.ts";
import * as RelayClient from "@t3tools/shared/relayClient";
import { makeServerConfigHeartbeatStream, shouldSendServerConfigHeartbeat } from "./wsKeepalive.ts";
const isOrchestrationDispatchCommandError = Schema.is(OrchestrationDispatchCommandError);
const isOrchestrationGetSnapshotError = Schema.is(OrchestrationGetSnapshotError);
const isOrchestrationReplayEventsError = Schema.is(OrchestrationReplayEventsError);
const isOrchestrationScheduledTaskMutationError = Schema.is(
  OrchestrationScheduledTaskMutationError,
);

const SHELL_REPLAY_SNAPSHOT_GAP_THRESHOLD = 1_000;
function unexpectedCompatibilityError(error: never): never {
  throw new Error(`Unhandled compatibility error: ${String(error)}`);
}

function projectEntriesFailureContext(error: WorkspaceEntries.WorkspaceEntriesError): {
  readonly failure: ProjectEntriesFailure;
  readonly normalizedCwd?: string;
  readonly timeout?: string;
  readonly detail?: string;
} {
  switch (error._tag) {
    case "WorkspaceRootNotExistsError":
      return {
        failure: "workspace_root_not_found",
        normalizedCwd: error.normalizedWorkspaceRoot,
      };
    case "WorkspaceRootCreateFailedError":
      return {
        failure: "workspace_root_create_failed",
        normalizedCwd: error.normalizedWorkspaceRoot,
      };
    case "WorkspaceRootStatFailedError":
      return {
        failure: "workspace_root_stat_failed",
        normalizedCwd: error.normalizedWorkspaceRoot,
        detail: error.phase,
      };
    case "WorkspaceRootNotDirectoryError":
      return {
        failure: "workspace_root_not_directory",
        normalizedCwd: error.normalizedWorkspaceRoot,
      };
    case "WorkspaceSearchIndexCreateFailed":
      return {
        failure: "search_index_create_failed",
        normalizedCwd: error.cwd,
        detail: error.reason,
      };
    case "WorkspaceSearchIndexScanTimedOut":
      return {
        failure: "search_index_scan_timed_out",
        normalizedCwd: error.cwd,
        timeout: error.timeout,
      };
    case "WorkspaceSearchIndexSearchFailed":
      return {
        failure: "search_index_search_failed",
        normalizedCwd: error.cwd,
        detail: error.reason,
      };
    default:
      return unexpectedCompatibilityError(error);
  }
}

function filesystemBrowseFailureContext(error: WorkspaceEntries.WorkspaceEntriesBrowseError): {
  readonly failure: FilesystemBrowseFailure;
  readonly parentPath?: string;
  readonly platform?: string;
} {
  switch (error._tag) {
    case "WorkspaceEntriesWindowsPathUnsupportedError":
      return { failure: "windows_path_unsupported", platform: error.platform };
    case "WorkspaceEntriesCurrentProjectRequiredError":
      return { failure: "current_project_required" };
    case "WorkspaceEntriesReadDirectoryError":
      return { failure: "read_directory_failed", parentPath: error.parentPath };
    default:
      return unexpectedCompatibilityError(error);
  }
}

function projectFileFailureContext(
  error:
    | WorkspaceFileSystem.WorkspaceFileSystemError
    | WorkspacePaths.WorkspacePathOutsideRootError,
): {
  readonly failure: ProjectFileFailure;
  readonly resolvedPath?: string;
  readonly resolvedWorkspaceRoot?: string;
  readonly operation?: ProjectFileOperation;
  readonly operationPath?: string;
} {
  switch (error._tag) {
    case "WorkspacePathOutsideRootError":
      return { failure: "workspace_path_outside_root" };
    case "WorkspaceFileSystemOperationError":
      return {
        failure: "operation_failed",
        resolvedPath: error.resolvedPath,
        operation: error.operation,
        operationPath: error.operationPath,
      };
    case "WorkspaceFilePathEscapeError":
      return {
        failure: "resolved_path_outside_root",
        resolvedPath: error.resolvedPath,
        resolvedWorkspaceRoot: error.resolvedWorkspaceRoot,
      };
    case "WorkspacePathNotFileError":
      return { failure: "path_not_file", resolvedPath: error.resolvedPath };
    case "WorkspaceBinaryFileError":
      return { failure: "binary_file", resolvedPath: error.resolvedPath };
    default:
      return unexpectedCompatibilityError(error);
  }
}

export function isThreadDetailEvent(event: OrchestrationEvent): event is Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.message-sent"
      | "thread.deleted"
      | "thread.archived"
      | "thread.unarchived"
      | "thread.proposed-plan-upserted"
      | "thread.activity-appended"
      | "thread.turn-diff-completed"
      | "thread.turn-effective-model-set"
      | "thread.reverted"
      | "thread.session-set";
  }
> {
  return (
    event.type === "thread.message-sent" ||
    event.type === "thread.deleted" ||
    event.type === "thread.archived" ||
    event.type === "thread.unarchived" ||
    event.type === "thread.proposed-plan-upserted" ||
    event.type === "thread.activity-appended" ||
    event.type === "thread.turn-diff-completed" ||
    event.type === "thread.turn-effective-model-set" ||
    event.type === "thread.reverted" ||
    event.type === "thread.session-set"
  );
}

const PROVIDER_STATUS_DEBOUNCE_MS = 200;

const RPC_REQUIRED_SCOPE = new Map<string, AuthEnvironmentScope>([
  [ORCHESTRATION_WS_METHODS.dispatchCommand, AuthOrchestrationOperateScope],
  [ORCHESTRATION_WS_METHODS.getTurnDiff, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.getFullThreadDiff, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.replayEvents, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.subscribeShell, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.getArchivedShellSnapshot, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.subscribeThread, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.subscribeScheduledTasks, AuthOrchestrationReadScope],
  [ORCHESTRATION_WS_METHODS.setScheduledTaskEnabled, AuthOrchestrationOperateScope],
  [ORCHESTRATION_WS_METHODS.deleteScheduledTask, AuthOrchestrationOperateScope],
  [WS_METHODS.serverGetConfig, AuthOrchestrationReadScope],
  [WS_METHODS.serverRefreshProviders, AuthOrchestrationOperateScope],
  [WS_METHODS.serverUpdateProvider, AuthOrchestrationOperateScope],
  [WS_METHODS.serverUpsertKeybinding, AuthOrchestrationOperateScope],
  [WS_METHODS.serverRemoveKeybinding, AuthOrchestrationOperateScope],
  [WS_METHODS.serverGetSettings, AuthOrchestrationReadScope],
  [WS_METHODS.serverUpdateSettings, AuthOrchestrationOperateScope],
  [WS_METHODS.serverDiscoverSourceControl, AuthOrchestrationReadScope],
  [WS_METHODS.serverGetTraceDiagnostics, AuthOrchestrationReadScope],
  [WS_METHODS.serverGetProcessDiagnostics, AuthOrchestrationReadScope],
  [WS_METHODS.serverGetProcessResourceHistory, AuthOrchestrationReadScope],
  [WS_METHODS.serverSignalProcess, AuthOrchestrationOperateScope],
  [WS_METHODS.serverGetNotificationConfig, AuthOrchestrationReadScope],
  [WS_METHODS.serverRegisterNotificationDevice, AuthOrchestrationOperateScope],
  [WS_METHODS.serverAckNotification, AuthOrchestrationReadScope],
  [WS_METHODS.cloudGetRelayClientStatus, AuthRelayWriteScope],
  [WS_METHODS.cloudInstallRelayClient, AuthRelayWriteScope],
  [WS_METHODS.sourceControlLookupRepository, AuthOrchestrationReadScope],
  [WS_METHODS.sourceControlCloneRepository, AuthOrchestrationOperateScope],
  [WS_METHODS.sourceControlPublishRepository, AuthOrchestrationOperateScope],
  [WS_METHODS.projectsListEntries, AuthOrchestrationReadScope],
  [WS_METHODS.projectsReadFile, AuthOrchestrationReadScope],
  [WS_METHODS.projectsSearchEntries, AuthOrchestrationReadScope],
  [WS_METHODS.projectsWriteFile, AuthOrchestrationOperateScope],
  [WS_METHODS.shellOpenInEditor, AuthOrchestrationOperateScope],
  [WS_METHODS.filesystemBrowse, AuthOrchestrationReadScope],
  [WS_METHODS.assetsCreateUrl, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeVcsStatus, AuthOrchestrationReadScope],
  [WS_METHODS.vcsRefreshStatus, AuthOrchestrationReadScope],
  [WS_METHODS.vcsPull, AuthOrchestrationOperateScope],
  [WS_METHODS.gitRunStackedAction, AuthOrchestrationOperateScope],
  [WS_METHODS.gitResolvePullRequest, AuthOrchestrationOperateScope],
  [WS_METHODS.gitPreparePullRequestThread, AuthOrchestrationOperateScope],
  [WS_METHODS.vcsListRefs, AuthOrchestrationReadScope],
  [WS_METHODS.vcsCreateWorktree, AuthOrchestrationOperateScope],
  [WS_METHODS.vcsRemoveWorktree, AuthOrchestrationOperateScope],
  [WS_METHODS.vcsCreateRef, AuthOrchestrationOperateScope],
  [WS_METHODS.vcsSwitchRef, AuthOrchestrationOperateScope],
  [WS_METHODS.vcsInit, AuthOrchestrationOperateScope],
  [WS_METHODS.reviewGetDiffPreview, AuthReviewWriteScope],
  [WS_METHODS.terminalOpen, AuthTerminalOperateScope],
  [WS_METHODS.terminalAttach, AuthTerminalOperateScope],
  [WS_METHODS.terminalWrite, AuthTerminalOperateScope],
  [WS_METHODS.terminalResize, AuthTerminalOperateScope],
  [WS_METHODS.terminalClear, AuthTerminalOperateScope],
  [WS_METHODS.terminalRestart, AuthTerminalOperateScope],
  [WS_METHODS.terminalClose, AuthTerminalOperateScope],
  [WS_METHODS.subscribeTerminalEvents, AuthTerminalOperateScope],
  [WS_METHODS.subscribeTerminalMetadata, AuthTerminalOperateScope],
  [WS_METHODS.previewOpen, AuthOrchestrationOperateScope],
  [WS_METHODS.previewNavigate, AuthOrchestrationOperateScope],
  [WS_METHODS.previewResize, AuthOrchestrationOperateScope],
  [WS_METHODS.previewRefresh, AuthOrchestrationOperateScope],
  [WS_METHODS.previewClose, AuthOrchestrationOperateScope],
  [WS_METHODS.previewList, AuthOrchestrationReadScope],
  [WS_METHODS.previewReportStatus, AuthOrchestrationOperateScope],
  [WS_METHODS.subscribePreviewEvents, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeDiscoveredLocalServers, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeServerConfig, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeServerLifecycle, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeNotificationEvents, AuthOrchestrationReadScope],
  [WS_METHODS.subscribeAuthAccess, AuthAccessReadScope],
]);

function toAuthAccessStreamEvent(
  change: PairingGrantStore.BootstrapCredentialChange | SessionStore.SessionCredentialChange,
  revision: number,
  currentSessionId: AuthSessionId,
): AuthAccessStreamEvent {
  switch (change.type) {
    case "pairingLinkUpserted":
      return {
        version: 1,
        revision,
        type: "pairingLinkUpserted",
        payload: change.pairingLink,
      };
    case "pairingLinkRemoved":
      return {
        version: 1,
        revision,
        type: "pairingLinkRemoved",
        payload: { id: change.id },
      };
    case "clientUpserted":
      return {
        version: 1,
        revision,
        type: "clientUpserted",
        payload: {
          ...change.clientSession,
          current: change.clientSession.sessionId === currentSessionId,
        },
      };
    case "clientRemoved":
      return {
        version: 1,
        revision,
        type: "clientRemoved",
        payload: { sessionId: change.sessionId },
      };
  }
}

const makeWsRpcLayer = (currentSession: EnvironmentAuth.AuthenticatedSession) =>
  WsRpcGroup.toLayer(
    Effect.gen(function* () {
      const currentSessionId = currentSession.sessionId;
      const projectionSnapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
      const scheduledTaskRepository = yield* ScheduledTaskRepository;
      const orchestrationEngine = yield* OrchestrationEngine.OrchestrationEngineService;
      const orchestrationEventStore = yield* OrchestrationEventStore;
      const checkpointDiffQuery = yield* CheckpointDiffQuery.CheckpointDiffQuery;
      const keybindings = yield* Keybindings.Keybindings;
      const externalLauncher = yield* ExternalLauncher.ExternalLauncher;
      const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
      const review = yield* ReviewService.ReviewService;
      const vcsProvisioning = yield* VcsProvisioningService.VcsProvisioningService;
      const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
      const terminalManager = yield* TerminalManager.TerminalManager;
      const previewManager = yield* PreviewManager.PreviewManager;
      const portDiscovery = yield* PortScanner.PortDiscovery;
      const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
      const providerMaintenanceRunner = yield* ProviderMaintenanceRunner.ProviderMaintenanceRunner;
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const lifecycleEvents = yield* ServerLifecycleEvents.ServerLifecycleEvents;
      const serverSettings = yield* ServerSettings.ServerSettingsService;
      const planUsageSnapshot = yield* PlanUsageSnapshot.PlanUsageSnapshotStore;
      const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
      const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;
      const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
      const repositoryIdentityResolver =
        yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
      const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sourceControlDiscovery = yield* SourceControlDiscovery.SourceControlDiscovery;
      const automaticGitFetchInterval = serverSettings.getSettings.pipe(
        Effect.map((settings) => settings.automaticGitFetchInterval),
        Effect.catch((cause) =>
          Effect.logWarning("Failed to read automatic Git fetch interval setting", {
            detail: cause.message,
          }).pipe(Effect.as(DEFAULT_AUTOMATIC_GIT_FETCH_INTERVAL)),
        ),
      );
      const sourceControlRepositories =
        yield* SourceControlRepositoryService.SourceControlRepositoryService;
      const bootstrapCredentials = yield* PairingGrantStore.PairingGrantStore;
      const sessions = yield* SessionStore.SessionStore;
      const processDiagnostics = yield* ProcessDiagnostics.ProcessDiagnostics;
      const processResourceMonitor = yield* ProcessResourceMonitor.ProcessResourceMonitor;
      const deviceNotifications = yield* DeviceNotifications.DeviceNotifications;
      const relayClient = yield* RelayClient.RelayClient;
      const pathService = yield* Path.Path;
      const hostProcessPlatform = yield* HostProcessPlatform;
      const authenticatedPrincipal = {
        ...currentSession,
        scopes: new Set(currentSession.scopes),
      };
      const authorizationError = (requiredScope: AuthEnvironmentScope, method: string) =>
        new EnvironmentAuthorizationError({
          message: currentSession.scopes.includes(requiredScope)
            ? `RPC method ${method} is unavailable for the authenticated audience ceiling.`
            : `The authenticated token is missing required scope: ${requiredScope}.`,
          requiredScope,
        });
      const authorizeEffect = <A, E, R>(
        method: string,
        requiredScope: AuthEnvironmentScope,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E | EnvironmentAuthorizationError, R> =>
        currentSession.scopes.includes(requiredScope) &&
        isRpcMethodAllowedForAudienceCeiling(method, requiredScope, currentSession.audienceCeiling)
          ? effect
          : Effect.fail(authorizationError(requiredScope, method));
      const authorizeStream = <A, E, R>(
        method: string,
        requiredScope: AuthEnvironmentScope,
        stream: Stream.Stream<A, E, R>,
      ): Stream.Stream<A, E | EnvironmentAuthorizationError, R> =>
        currentSession.scopes.includes(requiredScope) &&
        isRpcMethodAllowedForAudienceCeiling(method, requiredScope, currentSession.audienceCeiling)
          ? stream
          : Stream.fail(authorizationError(requiredScope, method));
      const requiredScopeForMethod = (method: string): AuthEnvironmentScope => {
        const requiredScope = RPC_REQUIRED_SCOPE.get(method);
        if (requiredScope === undefined) {
          throw new Error(`RPC method ${method} has no declared authorization scope.`);
        }
        return requiredScope;
      };
      const observeRpcEffect = <A, E, R>(
        method: string,
        effect: Effect.Effect<A, E, R>,
        traceAttributes?: Readonly<Record<string, unknown>>,
      ) => {
        const audienceScopedEffect = isAudienceScopedReadRpcMethod(method)
          ? effect.pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, authenticatedPrincipal),
            )
          : effect;
        return instrumentRpcEffect(
          method,
          authorizeEffect(method, requiredScopeForMethod(method), audienceScopedEffect),
          traceAttributes,
        );
      };
      const observeRpcStream = <A, E, R>(
        method: string,
        stream: Stream.Stream<A, E, R>,
        traceAttributes?: Readonly<Record<string, unknown>>,
      ) => {
        const audienceScopedStream = isAudienceScopedReadRpcMethod(method)
          ? stream.pipe(
              Stream.provideService(EnvironmentAuthenticatedPrincipal, authenticatedPrincipal),
            )
          : stream;
        return instrumentRpcStream(
          method,
          authorizeStream(method, requiredScopeForMethod(method), audienceScopedStream),
          traceAttributes,
        );
      };
      const observeRpcStreamEffect = <A, StreamError, StreamContext, EffectError, EffectContext>(
        method: string,
        effect: Effect.Effect<
          Stream.Stream<A, StreamError, StreamContext>,
          EffectError,
          EffectContext
        >,
        traceAttributes?: Readonly<Record<string, unknown>>,
      ) => {
        const audienceScopedEffect = isAudienceScopedReadRpcMethod(method)
          ? effect.pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, authenticatedPrincipal),
              Effect.map((stream) =>
                stream.pipe(
                  Stream.provideService(EnvironmentAuthenticatedPrincipal, authenticatedPrincipal),
                ),
              ),
            )
          : effect;
        return instrumentRpcStreamEffect(
          method,
          authorizeEffect(method, requiredScopeForMethod(method), audienceScopedEffect),
          traceAttributes,
        );
      };
      const withAuthenticatedPrincipal = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, authenticatedPrincipal),
        );
      const withFilesystemGuardServices = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        withAuthenticatedPrincipal(effect).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, pathService),
          Effect.provideService(HostProcessPlatform, hostProcessPlatform),
          Effect.provideService(
            ProjectionSnapshotQuery.ProjectionSnapshotQuery,
            projectionSnapshotQuery,
          ),
        );
      const visiblePath = (candidatePath: string): Effect.Effect<boolean, never> =>
        currentSession.audienceCeiling === "private"
          ? Effect.succeed(true)
          : withFilesystemGuardServices(
              ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(candidatePath),
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("filesystem audience guard failed closed", {
                  candidatePath,
                  cause,
                }).pipe(Effect.as(false)),
              ),
            );
      const visibleBrowseTarget = (input: {
        readonly cwd?: string | undefined;
        readonly partialPath: string;
      }): Effect.Effect<boolean, never> =>
        currentSession.audienceCeiling === "private"
          ? Effect.succeed(true)
          : withFilesystemGuardServices(
              ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience(input),
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("filesystem browse audience guard failed closed", {
                  input,
                  cause,
                }).pipe(Effect.as(false)),
              ),
            );
      const hasHiddenDescendant = (candidatePath: string): Effect.Effect<boolean, never> =>
        currentSession.audienceCeiling === "private"
          ? Effect.succeed(false)
          : withFilesystemGuardServices(
              ProjectFilesystemAudienceGuard.hasHiddenDescendantForCurrentAudience(candidatePath),
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("filesystem hidden descendant guard failed closed", {
                  candidatePath,
                  cause,
                }).pipe(Effect.as(true)),
              ),
            );
      const projectEntriesDeniedContext = (cwd: string) => ({
        failure: "workspace_root_not_found" as const,
        normalizedCwd: pathService.resolve(cwd),
      });
      const projectFileDeniedContext = (input: {
        readonly cwd: string;
        readonly relativePath: string;
      }) => ({
        failure: "operation_failed" as const,
        resolvedPath: pathService.resolve(input.cwd, input.relativePath),
        operation: "realpath-workspace-root" as const,
        operationPath: input.cwd,
      });
      const ensureProjectEntriesVisible = (cwd: string) =>
        visiblePath(cwd).pipe(
          Effect.flatMap((visible) =>
            visible
              ? hasHiddenDescendant(cwd).pipe(Effect.map((hidden) => !hidden))
              : Effect.succeed(false),
          ),
          Effect.flatMap((visible) =>
            visible
              ? Effect.void
              : Effect.fail(
                  new ProjectListEntriesError({ cwd, ...projectEntriesDeniedContext(cwd) }),
                ),
          ),
        );
      const ensureProjectSearchVisible = (input: {
        readonly cwd: string;
        readonly query: string;
        readonly limit: number;
      }) =>
        visiblePath(input.cwd).pipe(
          Effect.flatMap((visible) =>
            visible
              ? hasHiddenDescendant(input.cwd).pipe(Effect.map((hidden) => !hidden))
              : Effect.succeed(false),
          ),
          Effect.flatMap((visible) =>
            visible
              ? Effect.void
              : Effect.fail(
                  new ProjectSearchEntriesError({
                    cwd: input.cwd,
                    queryLength: input.query.length,
                    limit: input.limit,
                    ...projectEntriesDeniedContext(input.cwd),
                  }),
                ),
          ),
        );
      const ensureProjectReadVisible = (input: {
        readonly cwd: string;
        readonly relativePath: string;
      }) =>
        visiblePath(pathService.resolve(input.cwd, input.relativePath)).pipe(
          Effect.flatMap((visible) =>
            visible
              ? Effect.void
              : Effect.fail(
                  new ProjectReadFileError({ ...input, ...projectFileDeniedContext(input) }),
                ),
          ),
        );
      const ensureProjectWriteVisible = (input: {
        readonly cwd: string;
        readonly relativePath: string;
      }) =>
        visibleMutationTarget({ cwd: input.cwd, targetPath: input.relativePath }).pipe(
          Effect.flatMap((visible) =>
            visible
              ? Effect.void
              : Effect.fail(
                  new ProjectWriteFileError({ ...input, ...projectFileDeniedContext(input) }),
                ),
          ),
        );
      const ensureBrowseVisible = (input: {
        readonly cwd?: string | undefined;
        readonly partialPath: string;
      }) => {
        const parentPath = input.cwd ?? input.partialPath;
        return visibleBrowseTarget(input).pipe(
          Effect.flatMap((visible) =>
            visible
              ? hasHiddenDescendant(parentPath).pipe(Effect.map((hidden) => !hidden))
              : Effect.succeed(false),
          ),
          Effect.flatMap((visible) =>
            visible
              ? Effect.void
              : Effect.fail(
                  new FilesystemBrowseError({
                    ...input,
                    failure: "read_directory_failed",
                    parentPath,
                  }),
                ),
          ),
        );
      };
      const gitAudienceDenied = (input: {
        readonly operation: string;
        readonly command: string;
        readonly cwd: string;
      }) =>
        new GitCommandError({
          ...input,
          detail: "Repository was not found.",
        });
      const vcsAudienceDenied = (input: { readonly operation: string; readonly cwd: string }) =>
        new VcsRepositoryDetectionError({
          ...input,
          detail: "Repository was not found.",
        });
      const ensureGitCwdVisible = (input: {
        readonly operation: string;
        readonly command: string;
        readonly cwd: string;
      }) =>
        visiblePath(input.cwd).pipe(
          Effect.flatMap((visible) =>
            visible
              ? hasHiddenDescendant(input.cwd).pipe(Effect.map((hidden) => !hidden))
              : Effect.succeed(false),
          ),
          Effect.flatMap((visible) =>
            visible ? Effect.void : Effect.fail(gitAudienceDenied(input)),
          ),
        );
      const visibleMutationTarget = (input: {
        readonly cwd: string;
        readonly targetPath: string;
      }): Effect.Effect<boolean, never> =>
        currentSession.audienceCeiling === "private"
          ? Effect.succeed(true)
          : withFilesystemGuardServices(
              ProjectFilesystemAudienceGuard.isMutationTargetVisibleToCurrentAudience(input),
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("filesystem mutation audience guard failed closed", {
                  input,
                  cause,
                }).pipe(Effect.as(false)),
              ),
            );
      const ensureGitMutationTargetVisible = (input: {
        readonly operation: string;
        readonly command: string;
        readonly cwd: string;
        readonly targetPath: string | null;
      }) => {
        if (input.targetPath === null) return Effect.void;
        const targetPath = input.targetPath;
        return visibleMutationTarget({ cwd: input.cwd, targetPath }).pipe(
          Effect.flatMap((visible) =>
            visible ? Effect.void : Effect.fail(gitAudienceDenied({ ...input, cwd: targetPath })),
          ),
        );
      };
      const ensureVcsInitTargetVisible = (cwd: string) =>
        visibleMutationTarget({ cwd, targetPath: "." }).pipe(
          Effect.flatMap((visible) =>
            visible ? Effect.void : Effect.fail(vcsAudienceDenied({ operation: "init", cwd })),
          ),
        );
      const ensureAssetResourceVisible = Effect.fn("ws.ensureAssetResourceVisible")(function* (
        resource: AssetResource,
      ) {
        switch (resource._tag) {
          case "workspace-file": {
            const thread = yield* withAuthenticatedPrincipal(
              projectionSnapshotQuery.getThreadShellById(resource.threadId),
            );
            if (Option.isNone(thread)) {
              return yield* new AssetWorkspaceContextNotFoundError({ resource });
            }
            const project = yield* withAuthenticatedPrincipal(
              projectionSnapshotQuery.getProjectShellById(thread.value.projectId),
            );
            if (Option.isNone(project)) {
              return yield* new AssetWorkspaceContextNotFoundError({ resource });
            }
            return {
              dataAudience: thread.value.dataAudience,
              workspaceRoot: thread.value.worktreePath ?? project.value.workspaceRoot,
            };
          }
          case "attachment": {
            const threadSegment = parseThreadSegmentFromAttachmentId(resource.attachmentId);
            if (!threadSegment) {
              if (currentSession.audienceCeiling === "private") {
                return { dataAudience: "private" as const };
              }
              return yield* new AssetWorkspaceContextNotFoundError({ resource });
            }
            const thread = yield* withAuthenticatedPrincipal(
              projectionSnapshotQuery.getThreadShellByIdIncludingArchived(
                ThreadId.make(threadSegment),
              ),
            );
            if (Option.isNone(thread)) {
              if (currentSession.audienceCeiling === "private") {
                return { dataAudience: "private" as const };
              }
              return yield* new AssetWorkspaceContextNotFoundError({ resource });
            }
            return { dataAudience: thread.value.dataAudience };
          }
          case "project-favicon": {
            const project = yield* withAuthenticatedPrincipal(
              projectionSnapshotQuery.getActiveProjectByWorkspaceRoot(resource.cwd),
            );
            if (Option.isNone(project)) {
              return yield* new AssetWorkspaceContextNotFoundError({ resource });
            }
            return { dataAudience: project.value.dataAudience };
          }
        }
      });

      const toDispatchCommandError = (cause: unknown, fallbackMessage: string) =>
        isOrchestrationDispatchCommandError(cause)
          ? cause
          : new OrchestrationDispatchCommandError({
              message: cause instanceof Error ? cause.message : fallbackMessage,
              cause,
            });
      const loadAuthAccessSnapshot = () =>
        Effect.all({
          pairingLinks: serverAuth.listPairingLinks(),
          clientSessions: serverAuth.listClientSessions(currentSessionId),
        }).pipe(
          Effect.mapError(
            (error) =>
              new AuthAccessStreamError({
                message: error.message,
              }),
          ),
        );

      const requireAudienceOpaqueEventCursor = <E>(makeError: (message: string) => E) =>
        currentSession.audienceCeiling === "factory"
          ? Effect.fail(
              makeError(
                "Audience-scoped event cursors are unavailable for restricted-audience sessions",
              ),
            )
          : Effect.void;

      const enrichProjectEvent = (
        event: OrchestrationEvent,
      ): Effect.Effect<OrchestrationEvent, never, never> => {
        switch (event.type) {
          case "project.created":
            return repositoryIdentityResolver.resolve(event.payload.workspaceRoot).pipe(
              Effect.map((repositoryIdentity) => ({
                ...event,
                payload: {
                  ...event.payload,
                  repositoryIdentity,
                },
              })),
            );
          case "project.meta-updated":
            return Effect.gen(function* () {
              const workspaceRoot =
                event.payload.workspaceRoot ??
                Option.match(
                  yield* projectionSnapshotQuery.getProjectShellById(event.payload.projectId),
                  {
                    onNone: () => null,
                    onSome: (project) => project.workspaceRoot,
                  },
                ) ??
                null;
              if (workspaceRoot === null) {
                return event;
              }

              const repositoryIdentity = yield* repositoryIdentityResolver.resolve(workspaceRoot);
              return {
                ...event,
                payload: {
                  ...event.payload,
                  repositoryIdentity,
                },
              } satisfies OrchestrationEvent;
            }).pipe(Effect.orElseSucceed(() => event));
          default:
            return Effect.succeed(event);
        }
      };

      const enrichOrchestrationEvents = (events: ReadonlyArray<OrchestrationEvent>) =>
        Effect.forEach(events, enrichProjectEvent, { concurrency: 4 });

      const canReadAggregate = (input: ProjectionSnapshotQuery.ProjectionEventAggregateRef) =>
        currentSession.audienceCeiling === "private"
          ? Effect.succeed(true)
          : (projectionSnapshotQuery.canReadEventAggregate?.(input) ?? Effect.succeed(false)).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetSnapshotError({
                    message: `Failed to resolve ${input.aggregateKind} event visibility`,
                    cause,
                  }),
              ),
            );

      const canReadEventAggregate = (event: OrchestrationEvent) =>
        canReadAggregate({
          aggregateKind: event.aggregateKind,
          aggregateId: event.aggregateId,
        });

      const visibleOrchestrationEvents = <E, R>(stream: Stream.Stream<OrchestrationEvent, E, R>) =>
        stream.pipe(Stream.filterEffect(canReadEventAggregate));

      const toShellStreamEvent = (
        event: OrchestrationEvent,
      ): Effect.Effect<
        Option.Option<OrchestrationShellStreamItem>,
        OrchestrationGetSnapshotError,
        never
      > => {
        switch (event.type) {
          case "project.created":
          case "project.meta-updated":
            return projectionSnapshotQuery.getProjectShellById(event.payload.projectId).pipe(
              Effect.map((project) =>
                Option.map(project, (nextProject) => ({
                  kind: "project-upserted" as const,
                  sequence: event.sequence,
                  project: nextProject,
                })),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetSnapshotError({
                    message: "Failed to load orchestration project shell",
                    cause,
                  }),
              ),
            );
          case "project.data-audience-set":
            if (currentSession.audienceCeiling === "private") {
              return projectionSnapshotQuery.getProjectShellById(event.payload.projectId).pipe(
                Effect.map((project) =>
                  Option.map(project, (nextProject) => ({
                    kind: "project-upserted" as const,
                    sequence: event.sequence,
                    project: nextProject,
                  })),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationGetSnapshotError({
                      message: "Failed to load orchestration project shell",
                      cause,
                    }),
                ),
              );
            }
            return projectionSnapshotQuery.getShellSnapshot().pipe(
              Effect.map((snapshot) =>
                Option.some({
                  kind: "snapshot" as const,
                  snapshot,
                  force: true,
                }),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetSnapshotError({
                    message: "Failed to refresh orchestration shell after audience promotion",
                    cause,
                  }),
              ),
            );
          case "project.deleted":
            return Effect.succeed(
              Option.some({
                kind: "project-removed" as const,
                sequence: event.sequence,
                projectId: event.payload.projectId,
              }),
            );
          case "thread.deleted":
          case "thread.archived":
            return Effect.succeed(
              Option.some({
                kind: "thread-removed" as const,
                sequence: event.sequence,
                threadId: event.payload.threadId,
              }),
            );
          case "thread.unarchived":
            return projectionSnapshotQuery.getThreadShellById(event.payload.threadId).pipe(
              Effect.map((thread) =>
                Option.map(thread, (nextThread) => ({
                  kind: "thread-upserted" as const,
                  sequence: event.sequence,
                  thread: nextThread,
                })),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetSnapshotError({
                    message: "Failed to load orchestration thread shell",
                    cause,
                  }),
              ),
            );
          default:
            if (event.aggregateKind !== "thread") {
              return Effect.succeed(Option.none());
            }
            return projectionSnapshotQuery
              .getThreadShellById(ThreadId.make(event.aggregateId))
              .pipe(
                Effect.map((thread) =>
                  Option.map(thread, (nextThread) => ({
                    kind: "thread-upserted" as const,
                    sequence: event.sequence,
                    thread: nextThread,
                  })),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationGetSnapshotError({
                      message: "Failed to load orchestration thread shell",
                      cause,
                    }),
                ),
              );
        }
      };

      const dispatchBootstrapTurnStart = (
        command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
      ): Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError> =>
        BootstrapTurnStartDispatcher.dispatchActive(command);

      const dispatchNormalizedCommand = (
        normalizedCommand: OrchestrationCommand,
      ): Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError> => {
        const dispatchEffect =
          normalizedCommand.type === "thread.turn.start" && normalizedCommand.bootstrap
            ? dispatchBootstrapTurnStart(normalizedCommand)
            : orchestrationEngine
                .dispatch(normalizedCommand)
                .pipe(
                  Effect.mapError((cause) =>
                    toDispatchCommandError(cause, "Failed to dispatch orchestration command"),
                  ),
                );

        return startup.enqueueCommand(dispatchEffect).pipe(
          Effect.catch((cause) =>
            cleanupPersistedCommandAttachments(normalizedCommand).pipe(
              Effect.provideService(FileSystem.FileSystem, fileSystem),
              Effect.provideService(ServerConfig.ServerConfig, config),
              Effect.andThen(Effect.fail(cause)),
            ),
          ),
          Effect.mapError((cause) =>
            toDispatchCommandError(cause, "Failed to dispatch orchestration command"),
          ),
        );
      };

      const loadServerConfig = Effect.gen(function* () {
        const keybindingsConfig = yield* keybindings.loadConfigState;
        const providers = yield* providerRegistry.getProviders;
        const settings = ServerSettings.redactServerSettingsForClient(
          yield* serverSettings.getSettings,
        );
        const environment = yield* serverEnvironment.getDescriptor;
        const auth = yield* serverAuth.getDescriptor();

        return {
          environment,
          auth,
          cwd: config.cwd,
          keybindingsConfigPath: config.keybindingsConfigPath,
          keybindings: keybindingsConfig.keybindings,
          issues: keybindingsConfig.issues,
          providers,
          planUsage: yield* planUsageSnapshot.current,
          availableEditors: yield* externalLauncher.resolveAvailableEditors(),
          observability: {
            logsDirectoryPath: config.logsDir,
            localTracingEnabled: true,
            ...(config.otlpTracesUrl !== undefined ? { otlpTracesUrl: config.otlpTracesUrl } : {}),
            otlpTracesEnabled: config.otlpTracesUrl !== undefined,
            ...(config.otlpMetricsUrl !== undefined
              ? { otlpMetricsUrl: config.otlpMetricsUrl }
              : {}),
            otlpMetricsEnabled: config.otlpMetricsUrl !== undefined,
          },
          settings,
        };
      });

      const refreshGitStatus = (cwd: string) =>
        vcsStatusBroadcaster
          .refreshStatus(cwd)
          .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach, Effect.asVoid);

      // Enable/disable reuses the same repository update logic the MCP
      // t3_schedule_update handler calls: load the row, flip `enabled`, write
      // it back. Cadence is untouched so `next_run_at` is preserved. The
      // repository write bumps the shared liveness counter, so every surface
      // (panel, thread icon, banner) refreshes live.
      const setScheduledTaskEnabled = (input: {
        readonly taskId: ScheduledTaskId;
        readonly enabled: boolean;
      }) =>
        Effect.gen(function* () {
          const tasks = yield* scheduledTaskRepository.listAll();
          const existing = tasks.find((task) => task.taskId === input.taskId);
          if (existing === undefined) {
            return yield* new OrchestrationScheduledTaskMutationError({
              message: `Scheduled task ${input.taskId} was not found`,
            });
          }
          const updated = {
            ...existing,
            enabled: NonNegativeInt.make(input.enabled ? 1 : 0),
          };
          yield* scheduledTaskRepository.update(updated);
          return toScheduleEntry(updated);
        });

      return WsRpcGroup.of({
        [ORCHESTRATION_WS_METHODS.dispatchCommand]: (command) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.dispatchCommand,
            Effect.gen(function* () {
              const normalizedCommand = yield* normalizeDispatchCommand(command);
              return yield* dispatchNormalizedCommand(normalizedCommand);
            }).pipe(
              Effect.mapError((cause) =>
                isOrchestrationDispatchCommandError(cause)
                  ? cause
                  : new OrchestrationDispatchCommandError({
                      message: "Failed to dispatch orchestration command",
                      cause,
                    }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.getTurnDiff]: (input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.getTurnDiff,
            checkpointDiffQuery.getTurnDiff(input).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetTurnDiffError({
                    message: "Failed to load turn diff",
                    cause,
                  }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.getFullThreadDiff]: (input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.getFullThreadDiff,
            checkpointDiffQuery.getFullThreadDiff(input).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetFullThreadDiffError({
                    message: "Failed to load full thread diff",
                    cause,
                  }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.replayEvents]: (input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.replayEvents,
            Effect.gen(function* () {
              yield* requireAudienceOpaqueEventCursor(
                (message) => new OrchestrationReplayEventsError({ message }),
              );
              return yield* Stream.runCollect(
                visibleOrchestrationEvents(
                  orchestrationEngine.readEvents(
                    clamp(input.fromSequenceExclusive, {
                      maximum: Number.MAX_SAFE_INTEGER,
                      minimum: 0,
                    }),
                  ),
                ),
              );
            }).pipe(
              Effect.map((events) => Array.from(events)),
              Effect.flatMap(enrichOrchestrationEvents),
              Effect.mapError((cause) =>
                isOrchestrationReplayEventsError(cause)
                  ? cause
                  : new OrchestrationReplayEventsError({
                      message: "Failed to replay orchestration events",
                      cause,
                    }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.subscribeShell]: (input) =>
          observeRpcStreamEffect(
            ORCHESTRATION_WS_METHODS.subscribeShell,
            Effect.gen(function* () {
              yield* requireAudienceOpaqueEventCursor(
                (message) => new OrchestrationGetSnapshotError({ message }),
              );
              const liveStream: Stream.Stream<
                OrchestrationShellStreamItem,
                OrchestrationGetSnapshotError
              > = visibleOrchestrationEvents(orchestrationEngine.streamDomainEvents).pipe(
                Stream.mapEffect(toShellStreamEvent),
                Stream.flatMap((event) =>
                  Option.isSome(event) ? Stream.succeed(event.value) : Stream.empty,
                ),
              );

              // When the client already holds a shell snapshot (cached, or loaded
              // over HTTP) it passes that snapshot's sequence, and we resume by
              // replaying shell events after it instead of re-sending the whole
              // projects/threads list over the socket. As in the thread path, the
              // live subscription is attached (into a scope-bound buffer) before
              // draining the catch-up replay so no event published during the
              // replay window is lost; overlapping events are deduped by sequence
              // on the client. The full range is read (not the store's default
              // page limit) since the shell filter runs after reading.
              if (input.afterSequence !== undefined) {
                const afterSequence = input.afterSequence;
                return Stream.unwrap(
                  Effect.gen(function* () {
                    const liveBuffer = yield* Queue.unbounded<
                      | {
                          readonly _tag: "item";
                          readonly item: OrchestrationShellStreamItem;
                        }
                      | {
                          readonly _tag: "error";
                          readonly error: OrchestrationGetSnapshotError;
                        }
                    >();
                    const liveBufferStream = Stream.fromQueue(liveBuffer).pipe(
                      Stream.mapEffect((entry) =>
                        entry._tag === "item"
                          ? Effect.succeed(entry.item)
                          : Effect.fail(entry.error),
                      ),
                    );
                    yield* Effect.forkScoped(
                      liveStream.pipe(
                        Stream.runForEach((item) =>
                          Queue.offer(liveBuffer, {
                            _tag: "item" as const,
                            item,
                          }),
                        ),
                        Effect.catch((error) =>
                          Queue.offer(liveBuffer, {
                            _tag: "error" as const,
                            error,
                          }),
                        ),
                      ),
                    );
                    yield* Effect.yieldNow;
                    const currentSequence = yield* projectionSnapshotQuery
                      .getSnapshotSequence()
                      .pipe(
                        Effect.tapError((cause) =>
                          Effect.logError("orchestration shell sequence load failed", { cause }),
                        ),
                        Effect.mapError(
                          (cause) =>
                            new OrchestrationGetSnapshotError({
                              message: "Failed to load orchestration shell sequence",
                              cause,
                            }),
                        ),
                      );
                    const isClientCursorAhead = afterSequence > currentSequence.snapshotSequence;
                    if (
                      isClientCursorAhead ||
                      currentSequence.snapshotSequence - afterSequence >
                        SHELL_REPLAY_SNAPSHOT_GAP_THRESHOLD
                    ) {
                      const snapshot = yield* projectionSnapshotQuery.getShellSnapshot().pipe(
                        Effect.tapError((cause) =>
                          Effect.logError("orchestration shell snapshot load failed", { cause }),
                        ),
                        Effect.mapError(
                          (cause) =>
                            new OrchestrationGetSnapshotError({
                              message: "Failed to load orchestration shell snapshot",
                              cause,
                            }),
                        ),
                      );
                      return Stream.concat(
                        Stream.make({
                          kind: "snapshot" as const,
                          snapshot,
                          ...(isClientCursorAhead ? { force: true } : {}),
                        }),
                        liveBufferStream,
                      );
                    }
                    const catchUpStream = visibleOrchestrationEvents(
                      orchestrationEngine.readEvents(afterSequence, Number.MAX_SAFE_INTEGER),
                    ).pipe(
                      Stream.mapEffect(toShellStreamEvent),
                      Stream.flatMap((event) =>
                        Option.isSome(event) ? Stream.succeed(event.value) : Stream.empty,
                      ),
                      Stream.mapError((cause) =>
                        isOrchestrationGetSnapshotError(cause)
                          ? cause
                          : new OrchestrationGetSnapshotError({
                              message: "Failed to replay orchestration shell events",
                              cause,
                            }),
                      ),
                    );
                    return Stream.concat(
                      catchUpStream,
                      Stream.concat(
                        Stream.make({
                          kind: "caught-up" as const,
                          sequence: currentSequence.snapshotSequence,
                        }),
                        liveBufferStream,
                      ),
                    );
                  }),
                );
              }

              const snapshot = yield* projectionSnapshotQuery.getShellSnapshot().pipe(
                Effect.tapError((cause) =>
                  Effect.logError("orchestration shell snapshot load failed", { cause }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationGetSnapshotError({
                      message: "Failed to load orchestration shell snapshot",
                      cause,
                    }),
                ),
              );

              return Stream.concat(
                Stream.make({
                  kind: "snapshot" as const,
                  snapshot,
                }),
                liveStream,
              );
            }),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.getArchivedShellSnapshot]: (_input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.getArchivedShellSnapshot,
            projectionSnapshotQuery.getArchivedShellSnapshot().pipe(
              Effect.tapError((cause) =>
                Effect.logError("orchestration archived shell snapshot load failed", { cause }),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestrationGetSnapshotError({
                    message: "Failed to load archived orchestration shell snapshot",
                    cause,
                  }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.subscribeThread]: (input) =>
          observeRpcStreamEffect(
            ORCHESTRATION_WS_METHODS.subscribeThread,
            Effect.gen(function* () {
              yield* requireAudienceOpaqueEventCursor(
                (message) => new OrchestrationGetSnapshotError({ message }),
              );
              const canReadThread = yield* canReadAggregate({
                aggregateKind: "thread",
                aggregateId: input.threadId,
              });
              if (!canReadThread) {
                return yield* new OrchestrationGetSnapshotError({
                  message: `Thread ${input.threadId} was not found`,
                  cause: input.threadId,
                });
              }
              const isThisThreadDetailEvent = (event: OrchestrationEvent) =>
                event.aggregateKind === "thread" &&
                event.aggregateId === input.threadId &&
                isThreadDetailEvent(event);
              const storageEpoch = orchestrationEventStore.storageEpoch;

              const liveStream = visibleOrchestrationEvents(
                orchestrationEngine.streamDomainEvents,
              ).pipe(
                Stream.filter(isThisThreadDetailEvent),
                Stream.map((event) => ({
                  kind: "event" as const,
                  storageEpoch,
                  event,
                })),
              );

              // When the client already loaded the snapshot over HTTP it passes
              // that snapshot's sequence, and we resume the live subscription by
              // replaying persisted events after it instead of re-sending the
              // (potentially multi-KB) snapshot frame over the socket.
              //
              // The live PubSub subscription must be attached *before* draining
              // the catch-up replay, otherwise events published during the replay
              // window are dropped (they are past the persisted tail the replay
              // read, but the live stream is not yet subscribed). So fork the
              // live stream into a buffer bound to this stream's scope, then emit
              // catch-up followed by the buffered/ongoing live events. Overlapping
              // events are deduped by sequence on the client.
              //
              // Read the full range after the cursor (not the store's default
              // page-bounded limit): the range is normally tiny (a fresh HTTP
              // snapshot sequence) and the per-thread filter runs after reading,
              // so a global cap could otherwise omit this thread's events.
              // A global replay cursor may legitimately exceed this idle
              // thread's marker, but it must never exceed the current store
              // high-water mark after a restore. Resume only when both cursors
              // and the exact per-thread history identity are still valid.
              const requestedAfterSequence = input.afterSequence;
              return Stream.unwrap(
                Effect.gen(function* () {
                  // Attach the live subscription before reading either the replay
                  // boundary or the fallback snapshot. Every path then drains the
                  // same buffer after its persisted recovery item(s), closing the
                  // snapshot-then-subscribe race for both warm and cold clients.
                  const liveBuffer = yield* Queue.unbounded<OrchestrationThreadStreamItem>();
                  yield* Effect.forkScoped(
                    liveStream.pipe(Stream.runForEach((item) => Queue.offer(liveBuffer, item))),
                  );
                  yield* Effect.yieldNow;
                  const [latestRevision, latestStoreSequence, subscribedThread] = yield* Effect.all(
                    [
                      orchestrationEventStore.getLatestThreadRevision(input.threadId),
                      orchestrationEventStore.getLatestSequence(),
                      projectionSnapshotQuery.getThreadShellByIdIncludingArchived(input.threadId),
                    ],
                  ).pipe(
                    Effect.mapError(
                      (cause) =>
                        new OrchestrationGetSnapshotError({
                          message: `Failed to validate thread ${input.threadId} replay cursor`,
                          cause,
                        }),
                    ),
                  );
                  const liveBufferStream = Stream.fromQueue(liveBuffer);
                  const observedIdentityMatches =
                    input.observedRevision === latestRevision.latestSequence &&
                    (input.observedRevision === 0
                      ? input.observedEventId === null && latestRevision.latestEventId === null
                      : input.observedEventId != null &&
                        latestRevision.latestEventId !== null &&
                        input.observedEventId === latestRevision.latestEventId);
                  const canResumeFromCursor =
                    requestedAfterSequence !== undefined &&
                    requestedAfterSequence <= latestStoreSequence &&
                    input.storageEpoch === storageEpoch &&
                    input.verifiedRevision !== undefined &&
                    input.verifiedRevision <= latestRevision.latestSequence &&
                    input.observedRevision !== undefined &&
                    Option.exists(
                      subscribedThread,
                      (thread) => input.observedDataAudience === thread.dataAudience,
                    ) &&
                    observedIdentityMatches;
                  if (canResumeFromCursor && requestedAfterSequence !== undefined) {
                    const catchUpStream = visibleOrchestrationEvents(
                      orchestrationEngine.readEvents(
                        requestedAfterSequence,
                        Number.MAX_SAFE_INTEGER,
                      ),
                    ).pipe(
                      Stream.filter(isThisThreadDetailEvent),
                      Stream.map((event) => ({
                        kind: "event" as const,
                        storageEpoch,
                        event,
                      })),
                      Stream.mapError(
                        (cause) =>
                          new OrchestrationGetSnapshotError({
                            message: `Failed to replay thread ${input.threadId} events`,
                            cause,
                          }),
                      ),
                    );
                    return Stream.concat(catchUpStream, liveBufferStream);
                  }

                  const snapshot = yield* projectionSnapshotQuery
                    .getThreadDetailSnapshot(input.threadId)
                    .pipe(
                      Effect.mapError(
                        (cause) =>
                          new OrchestrationGetSnapshotError({
                            message: `Failed to load thread ${input.threadId}`,
                            cause,
                          }),
                      ),
                    );

                  if (Option.isNone(snapshot)) {
                    // A cursorless subscription can still come from a warm
                    // unknown-epoch cache: the client deliberately discarded
                    // its untrustworthy cursor to force authoritative recovery.
                    // Recover an exact persisted tombstone for every missing
                    // snapshot so that path observes deletion instead of
                    // retrying the failed subscription every 250 ms.
                    const missingSnapshotRevision = yield* orchestrationEventStore
                      .getLatestThreadRevision(input.threadId)
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new OrchestrationGetSnapshotError({
                              message: `Failed to refresh missing thread ${input.threadId} revision`,
                              cause,
                            }),
                        ),
                      );
                    if (
                      missingSnapshotRevision.latestSequence > 0 &&
                      missingSnapshotRevision.latestEventId !== null
                    ) {
                      const latestDeletion = yield* orchestrationEngine
                        .readEvents(missingSnapshotRevision.latestSequence - 1, 1)
                        .pipe(
                          Stream.filter(
                            (event) =>
                              event.aggregateKind === "thread" &&
                              event.aggregateId === input.threadId &&
                              event.sequence === missingSnapshotRevision.latestSequence &&
                              event.eventId === missingSnapshotRevision.latestEventId &&
                              event.type === "thread.deleted",
                          ),
                          Stream.runHead,
                          Effect.mapError(
                            (cause) =>
                              new OrchestrationGetSnapshotError({
                                message: `Failed to recover deleted thread ${input.threadId}`,
                                cause,
                              }),
                          ),
                        );
                      if (Option.isSome(latestDeletion)) {
                        return Stream.concat(
                          Stream.make({
                            kind: "event" as const,
                            storageEpoch,
                            force: true,
                            event: latestDeletion.value,
                          }),
                          liveBufferStream,
                        );
                      }
                    }
                    return yield* new OrchestrationGetSnapshotError({
                      message: `Thread ${input.threadId} was not found`,
                      cause: input.threadId,
                    });
                  }

                  return Stream.concat(
                    Stream.make({
                      kind: "snapshot" as const,
                      storageEpoch,
                      ...(requestedAfterSequence !== undefined ? { force: true } : {}),
                      snapshot: {
                        ...snapshot.value,
                        storageEpoch,
                        ...coveredThreadRevision(snapshot.value.snapshotSequence, latestRevision),
                      },
                    }),
                    liveBufferStream,
                  );
                }),
              );
            }),
            { "rpc.aggregate": "orchestration" },
          ),
        // `scheduled_tasks` is not event-sourced, so freshness comes from the
        // repository's in-process liveness counter (revisionChanges, bumped on
        // every insert/update/delete/markRun — shared singleton). Each tick
        // re-reads the table and emits a full snapshot with a monotonic
        // `sequence`; the client folds snapshots exactly like the shell stream.
        [ORCHESTRATION_WS_METHODS.subscribeScheduledTasks]: (_input) =>
          observeRpcStreamEffect(
            ORCHESTRATION_WS_METHODS.subscribeScheduledTasks,
            Effect.sync(() => {
              let sequence = 0;
              const loadSnapshot = scheduledTaskRepository.listAll().pipe(
                Effect.flatMap((tasks) =>
                  Effect.forEach(
                    tasks,
                    (task) =>
                      canReadAggregate({
                        aggregateKind: "thread",
                        aggregateId: task.threadId,
                      }).pipe(Effect.map((visible) => (visible ? [task] : []))),
                    { concurrency: 8 },
                  ).pipe(Effect.map((visible) => visible.flat())),
                ),
                Effect.map((tasks) => ({
                  kind: "snapshot" as const,
                  snapshot: {
                    sequence: NonNegativeInt.make(sequence++),
                    tasks: tasks.map(toScheduleEntry),
                  },
                })),
                Effect.tapError((cause) =>
                  Effect.logError("scheduled tasks snapshot load failed", { cause }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationGetSnapshotError({
                      message: "Failed to load scheduled tasks snapshot",
                      cause,
                    }),
                ),
              );

              const refreshes =
                currentSession.audienceCeiling === "factory"
                  ? Stream.merge(
                      scheduledTaskRepository.revisionChanges,
                      orchestrationEngine.streamDomainEvents.pipe(
                        Stream.filter((event) => event.type === "project.data-audience-set"),
                        Stream.map(() => 0),
                      ),
                    )
                  : scheduledTaskRepository.revisionChanges;

              const snapshots = refreshes.pipe(Stream.mapEffect(() => loadSnapshot));
              return currentSession.audienceCeiling === "factory"
                ? scopeScheduledTaskStreamForAudience(snapshots)
                : snapshots;
            }),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.setScheduledTaskEnabled]: (input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.setScheduledTaskEnabled,
            setScheduledTaskEnabled(input).pipe(
              Effect.mapError((cause) =>
                isOrchestrationScheduledTaskMutationError(cause)
                  ? cause
                  : new OrchestrationScheduledTaskMutationError({
                      message: "Failed to update scheduled task",
                      cause,
                    }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [ORCHESTRATION_WS_METHODS.deleteScheduledTask]: (input) =>
          observeRpcEffect(
            ORCHESTRATION_WS_METHODS.deleteScheduledTask,
            scheduledTaskRepository.delete({ taskId: input.taskId }).pipe(
              Effect.as({ taskId: input.taskId, deleted: true }),
              Effect.mapError(
                (cause) =>
                  new OrchestrationScheduledTaskMutationError({
                    message: "Failed to delete scheduled task",
                    cause,
                  }),
              ),
            ),
            { "rpc.aggregate": "orchestration" },
          ),
        [WS_METHODS.serverGetConfig]: (_input) =>
          observeRpcEffect(WS_METHODS.serverGetConfig, loadServerConfig, {
            "rpc.aggregate": "server",
          }),
        [WS_METHODS.serverRefreshProviders]: (input) =>
          observeRpcEffect(
            WS_METHODS.serverRefreshProviders,
            (input.instanceId !== undefined
              ? providerRegistry.refreshInstance(input.instanceId)
              : providerRegistry.refresh()
            ).pipe(Effect.map((providers) => ({ providers }))),
            { "rpc.aggregate": "server" },
          ),
        [WS_METHODS.serverUpdateProvider]: (input) =>
          observeRpcEffect(
            WS_METHODS.serverUpdateProvider,
            providerMaintenanceRunner.updateProvider(input),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverUpsertKeybinding]: (rule) =>
          observeRpcEffect(
            WS_METHODS.serverUpsertKeybinding,
            Effect.gen(function* () {
              const keybindingsConfig = yield* keybindings.upsertKeybindingRule(rule);
              return { keybindings: keybindingsConfig, issues: [] };
            }),
            { "rpc.aggregate": "server" },
          ),
        [WS_METHODS.serverRemoveKeybinding]: (rule) =>
          observeRpcEffect(
            WS_METHODS.serverRemoveKeybinding,
            Effect.gen(function* () {
              const keybindingsConfig = yield* keybindings.removeKeybindingRule(rule);
              return { keybindings: keybindingsConfig, issues: [] };
            }),
            { "rpc.aggregate": "server" },
          ),
        [WS_METHODS.serverGetSettings]: (_input) =>
          observeRpcEffect(
            WS_METHODS.serverGetSettings,
            serverSettings.getSettings.pipe(
              Effect.map(ServerSettings.redactServerSettingsForClient),
            ),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverUpdateSettings]: ({ patch }) =>
          observeRpcEffect(
            WS_METHODS.serverUpdateSettings,
            serverSettings
              .updateSettings(patch)
              .pipe(Effect.map(ServerSettings.redactServerSettingsForClient)),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverDiscoverSourceControl]: (_input) =>
          observeRpcEffect(
            WS_METHODS.serverDiscoverSourceControl,
            sourceControlDiscovery.discover,
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverGetTraceDiagnostics]: (_input) =>
          observeRpcEffect(
            WS_METHODS.serverGetTraceDiagnostics,
            TraceDiagnostics.readTraceDiagnostics({
              traceFilePath: config.serverTracePath,
              maxFiles: config.traceMaxFiles,
            }),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverGetProcessDiagnostics]: (_input) =>
          observeRpcEffect(WS_METHODS.serverGetProcessDiagnostics, processDiagnostics.read, {
            "rpc.aggregate": "server",
          }),
        [WS_METHODS.serverGetProcessResourceHistory]: (input) =>
          observeRpcEffect(
            WS_METHODS.serverGetProcessResourceHistory,
            processResourceMonitor.readHistory(input),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverSignalProcess]: (input) =>
          observeRpcEffect(WS_METHODS.serverSignalProcess, processDiagnostics.signal(input), {
            "rpc.aggregate": "server",
          }),
        [WS_METHODS.serverGetNotificationConfig]: (_input) =>
          observeRpcEffect(WS_METHODS.serverGetNotificationConfig, deviceNotifications.getConfig, {
            "rpc.aggregate": "server",
          }),
        [WS_METHODS.serverRegisterNotificationDevice]: (input) =>
          observeRpcEffect(
            WS_METHODS.serverRegisterNotificationDevice,
            deviceNotifications.registerDevice(input, {
              audienceCeiling: currentSession.audienceCeiling,
            }),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.serverAckNotification]: (input) =>
          observeRpcEffect(
            WS_METHODS.serverAckNotification,
            deviceNotifications.ackNotification(input, {
              audienceCeiling: currentSession.audienceCeiling,
            }),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.cloudGetRelayClientStatus]: (_input) =>
          observeRpcEffect(WS_METHODS.cloudGetRelayClientStatus, relayClient.resolve, {
            "rpc.aggregate": "cloud",
          }),
        [WS_METHODS.cloudInstallRelayClient]: (_input) =>
          observeRpcStream(
            WS_METHODS.cloudInstallRelayClient,
            Stream.callback<RelayClientInstallProgressEvent, RelayClientInstallFailedError>(
              (queue) =>
                relayClient
                  .installWithProgress((event) => Queue.offer(queue, event).pipe(Effect.asVoid))
                  .pipe(
                    Effect.flatMap((status) =>
                      Queue.offer(queue, {
                        type: "complete",
                        status,
                      }),
                    ),
                    Effect.catchTag("RelayClientInstallError", (error) =>
                      Queue.fail(
                        queue,
                        new RelayClientInstallFailedError({
                          reason: error.reason,
                          message: error.message,
                        }),
                      ),
                    ),
                    Effect.andThen(Queue.end(queue)),
                    Effect.forkScoped,
                  ),
            ),
            { "rpc.aggregate": "cloud" },
          ),
        [WS_METHODS.sourceControlLookupRepository]: (input) =>
          observeRpcEffect(
            WS_METHODS.sourceControlLookupRepository,
            sourceControlRepositories.lookupRepository(input),
            {
              "rpc.aggregate": "source-control",
            },
          ),
        [WS_METHODS.sourceControlCloneRepository]: (input) =>
          observeRpcEffect(
            WS_METHODS.sourceControlCloneRepository,
            sourceControlRepositories.cloneRepository(input),
            {
              "rpc.aggregate": "source-control",
            },
          ),
        [WS_METHODS.sourceControlPublishRepository]: (input) =>
          observeRpcEffect(
            WS_METHODS.sourceControlPublishRepository,
            sourceControlRepositories
              .publishRepository(input)
              .pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
            {
              "rpc.aggregate": "source-control",
            },
          ),
        [WS_METHODS.projectsSearchEntries]: (input) =>
          observeRpcEffect(
            WS_METHODS.projectsSearchEntries,
            ensureProjectSearchVisible(input).pipe(
              Effect.andThen(
                workspaceEntries.search(input).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProjectSearchEntriesError({
                        cwd: input.cwd,
                        queryLength: input.query.length,
                        limit: input.limit,
                        ...projectEntriesFailureContext(cause),
                        cause,
                      }),
                  ),
                ),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.projectsListEntries]: (input) =>
          observeRpcEffect(
            WS_METHODS.projectsListEntries,
            ensureProjectEntriesVisible(input.cwd).pipe(
              Effect.andThen(
                workspaceEntries.list(input).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProjectListEntriesError({
                        ...input,
                        ...projectEntriesFailureContext(cause),
                        cause,
                      }),
                  ),
                ),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.projectsReadFile]: (input) =>
          observeRpcEffect(
            WS_METHODS.projectsReadFile,
            ensureProjectReadVisible(input).pipe(
              Effect.andThen(
                workspaceFileSystem.readFile(input).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProjectReadFileError({
                        ...input,
                        ...projectFileFailureContext(cause),
                        cause,
                      }),
                  ),
                ),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.projectsWriteFile]: (input) =>
          observeRpcEffect(
            WS_METHODS.projectsWriteFile,
            ensureProjectWriteVisible(input).pipe(
              Effect.andThen(
                withAuthenticatedPrincipal(workspaceFileSystem.writeFile(input)).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProjectWriteFileError({
                        cwd: input.cwd,
                        relativePath: input.relativePath,
                        ...projectFileFailureContext(cause),
                        cause,
                      }),
                  ),
                ),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.shellOpenInEditor]: (input) =>
          observeRpcEffect(WS_METHODS.shellOpenInEditor, externalLauncher.launchEditor(input), {
            "rpc.aggregate": "workspace",
          }),
        [WS_METHODS.filesystemBrowse]: (input) =>
          observeRpcEffect(
            WS_METHODS.filesystemBrowse,
            ensureBrowseVisible(input).pipe(
              Effect.andThen(
                workspaceEntries.browse(input).pipe(
                  Effect.mapError(
                    (cause) =>
                      new FilesystemBrowseError({
                        ...input,
                        ...filesystemBrowseFailureContext(cause),
                        cause,
                      }),
                  ),
                ),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.assetsCreateUrl]: (input) =>
          observeRpcEffect(
            WS_METHODS.assetsCreateUrl,
            ensureAssetResourceVisible(input.resource).pipe(
              Effect.mapError((cause) =>
                cause._tag === "AssetWorkspaceContextNotFoundError"
                  ? cause
                  : new AssetWorkspaceContextResolutionError({
                      resource: input.resource,
                      cause,
                    }),
              ),
              Effect.flatMap((context) =>
                issueAssetUrl({
                  resource: input.resource,
                  ...(context.workspaceRoot ? { workspaceRoot: context.workspaceRoot } : {}),
                  dataAudience: context.dataAudience,
                  audienceCeiling: currentSession.audienceCeiling,
                }),
              ),
            ),
            { "rpc.aggregate": "workspace" },
          ),
        [WS_METHODS.subscribeVcsStatus]: (input) =>
          observeRpcStreamEffect(
            WS_METHODS.subscribeVcsStatus,
            ensureGitCwdVisible({
              operation: "status",
              command: "git status",
              cwd: input.cwd,
            }).pipe(
              Effect.as(
                vcsStatusBroadcaster.streamStatus(input, {
                  automaticRemoteRefreshInterval: automaticGitFetchInterval,
                }),
              ),
            ),
            {
              "rpc.aggregate": "vcs",
            },
          ),
        [WS_METHODS.vcsRefreshStatus]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsRefreshStatus,
            ensureGitCwdVisible({
              operation: "status",
              command: "git status",
              cwd: input.cwd,
            }).pipe(Effect.andThen(vcsStatusBroadcaster.refreshStatus(input.cwd))),
            {
              "rpc.aggregate": "vcs",
            },
          ),
        [WS_METHODS.vcsPull]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsPull,
            ensureGitCwdVisible({
              operation: "pull",
              command: "git pull --ff-only",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(gitWorkflow.pullCurrentBranch(input.cwd)),
              Effect.matchCauseEffect({
                onFailure: (cause) => Effect.failCause(cause),
                onSuccess: (result) =>
                  refreshGitStatus(input.cwd).pipe(Effect.ignore({ log: true }), Effect.as(result)),
              }),
            ),
            { "rpc.aggregate": "git" },
          ),
        [WS_METHODS.gitRunStackedAction]: (input) =>
          observeRpcStream(
            WS_METHODS.gitRunStackedAction,
            Stream.callback<GitActionProgressEvent, GitManagerServiceError>((queue) =>
              ensureGitCwdVisible({
                operation: "runStackedAction",
                command: "git status",
                cwd: input.cwd,
              }).pipe(
                Effect.andThen(
                  gitWorkflow.runStackedAction(input, {
                    actionId: input.actionId,
                    progressReporter: {
                      publish: (event) => Queue.offer(queue, event).pipe(Effect.asVoid),
                    },
                  }),
                ),
                Effect.matchCauseEffect({
                  onFailure: (cause) => Queue.failCause(queue, cause),
                  onSuccess: () =>
                    refreshGitStatus(input.cwd).pipe(
                      Effect.andThen(Queue.end(queue).pipe(Effect.asVoid)),
                    ),
                }),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.gitResolvePullRequest]: (input) =>
          observeRpcEffect(
            WS_METHODS.gitResolvePullRequest,
            ensureGitCwdVisible({
              operation: "resolvePullRequest",
              command: "git remote get-url origin",
              cwd: input.cwd,
            }).pipe(Effect.andThen(gitWorkflow.resolvePullRequest(input))),
            {
              "rpc.aggregate": "git",
            },
          ),
        [WS_METHODS.gitPreparePullRequestThread]: (input) =>
          observeRpcEffect(
            WS_METHODS.gitPreparePullRequestThread,
            ensureGitCwdVisible({
              operation: "preparePullRequestThread",
              command: "git fetch",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(
                gitWorkflow
                  .preparePullRequestThread(input)
                  .pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "git" },
          ),
        [WS_METHODS.vcsListRefs]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsListRefs,
            ensureGitCwdVisible({
              operation: "listRefs",
              command: "git branch",
              cwd: input.cwd,
            }).pipe(Effect.andThen(gitWorkflow.listRefs(input))),
            {
              "rpc.aggregate": "vcs",
            },
          ),
        [WS_METHODS.vcsCreateWorktree]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsCreateWorktree,
            ensureGitCwdVisible({
              operation: "createWorktree",
              command: "git worktree add",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(
                ensureGitMutationTargetVisible({
                  operation: "createWorktree",
                  command: "git worktree add",
                  cwd: input.cwd,
                  targetPath: input.path,
                }),
              ),
              Effect.andThen(
                gitWorkflow
                  .createWorktree(input)
                  .pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.vcsRemoveWorktree]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsRemoveWorktree,
            ensureGitCwdVisible({
              operation: "removeWorktree",
              command: "git worktree remove",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(
                ensureGitMutationTargetVisible({
                  operation: "removeWorktree",
                  command: "git worktree remove",
                  cwd: input.cwd,
                  targetPath: input.path,
                }),
              ),
              Effect.andThen(
                gitWorkflow
                  .removeWorktree(input)
                  .pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.vcsCreateRef]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsCreateRef,
            ensureGitCwdVisible({
              operation: "createRef",
              command: "git branch",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(
                gitWorkflow.createRef(input).pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.vcsSwitchRef]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsSwitchRef,
            ensureGitCwdVisible({
              operation: "switchRef",
              command: "git checkout",
              cwd: input.cwd,
            }).pipe(
              Effect.andThen(
                gitWorkflow.switchRef(input).pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.vcsInit]: (input) =>
          observeRpcEffect(
            WS_METHODS.vcsInit,
            ensureVcsInitTargetVisible(input.cwd).pipe(
              Effect.andThen(
                vcsProvisioning
                  .initRepository(input)
                  .pipe(Effect.tap(() => refreshGitStatus(input.cwd))),
              ),
            ),
            { "rpc.aggregate": "vcs" },
          ),
        [WS_METHODS.reviewGetDiffPreview]: (input) =>
          observeRpcEffect(WS_METHODS.reviewGetDiffPreview, review.getDiffPreview(input), {
            "rpc.aggregate": "review",
          }),
        [WS_METHODS.terminalOpen]: (input) =>
          observeRpcEffect(WS_METHODS.terminalOpen, terminalManager.open(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.terminalAttach]: (input) =>
          observeRpcStream(
            WS_METHODS.terminalAttach,
            Stream.callback<TerminalAttachStreamEvent, TerminalError>((queue) =>
              Effect.acquireRelease(
                terminalManager.attachStream(input, (event) => Queue.offer(queue, event)),
                (unsubscribe) => Effect.sync(unsubscribe),
              ),
            ),
            { "rpc.aggregate": "terminal" },
          ),
        [WS_METHODS.terminalWrite]: (input) =>
          observeRpcEffect(WS_METHODS.terminalWrite, terminalManager.write(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.terminalResize]: (input) =>
          observeRpcEffect(WS_METHODS.terminalResize, terminalManager.resize(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.terminalClear]: (input) =>
          observeRpcEffect(WS_METHODS.terminalClear, terminalManager.clear(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.terminalRestart]: (input) =>
          observeRpcEffect(WS_METHODS.terminalRestart, terminalManager.restart(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.terminalClose]: (input) =>
          observeRpcEffect(WS_METHODS.terminalClose, terminalManager.close(input), {
            "rpc.aggregate": "terminal",
          }),
        [WS_METHODS.subscribeTerminalEvents]: (_input) =>
          observeRpcStream(
            WS_METHODS.subscribeTerminalEvents,
            Stream.callback<TerminalEvent>((queue) =>
              Effect.acquireRelease(
                terminalManager.subscribe((event) => Queue.offer(queue, event)),
                (unsubscribe) => Effect.sync(unsubscribe),
              ),
            ),
            { "rpc.aggregate": "terminal" },
          ),
        [WS_METHODS.subscribeTerminalMetadata]: (_input) =>
          observeRpcStream(
            WS_METHODS.subscribeTerminalMetadata,
            Stream.callback<TerminalMetadataStreamEvent>((queue) =>
              Effect.acquireRelease(
                terminalManager.subscribeMetadata((event) => Queue.offer(queue, event)),
                (unsubscribe) => Effect.sync(unsubscribe),
              ),
            ),
            { "rpc.aggregate": "terminal" },
          ),
        [WS_METHODS.previewOpen]: (input) =>
          observeRpcEffect(WS_METHODS.previewOpen, previewManager.open(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewNavigate]: (input) =>
          observeRpcEffect(WS_METHODS.previewNavigate, previewManager.navigate(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewResize]: (input) =>
          observeRpcEffect(WS_METHODS.previewResize, previewManager.resize(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewRefresh]: (input) =>
          observeRpcEffect(WS_METHODS.previewRefresh, previewManager.refresh(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewClose]: (input) =>
          observeRpcEffect(WS_METHODS.previewClose, previewManager.close(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewList]: (input) =>
          observeRpcEffect(WS_METHODS.previewList, previewManager.list(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.previewReportStatus]: (input) =>
          observeRpcEffect(WS_METHODS.previewReportStatus, previewManager.reportStatus(input), {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.subscribePreviewEvents]: (_input) =>
          observeRpcStream(WS_METHODS.subscribePreviewEvents, previewManager.events, {
            "rpc.aggregate": "preview",
          }),
        [WS_METHODS.subscribeDiscoveredLocalServers]: (_input) =>
          observeRpcStream(
            WS_METHODS.subscribeDiscoveredLocalServers,
            Stream.callback<DiscoveredLocalServerList>((queue) =>
              Effect.gen(function* () {
                yield* portDiscovery.retain;
                const initial = yield* portDiscovery.scan();
                const initialScannedAt = DateTime.formatIso(yield* DateTime.now);
                yield* Queue.offer(queue, {
                  servers: initial,
                  scannedAt: initialScannedAt,
                });
                yield* portDiscovery.subscribe((servers) =>
                  Effect.gen(function* () {
                    const scannedAt = DateTime.formatIso(yield* DateTime.now);
                    yield* Queue.offer(queue, { servers, scannedAt });
                  }),
                );
              }),
            ),
            { "rpc.aggregate": "preview" },
          ),
        [WS_METHODS.subscribeServerConfig]: (input) =>
          observeRpcStreamEffect(
            WS_METHODS.subscribeServerConfig,
            Effect.gen(function* () {
              const keybindingsUpdates = keybindings.streamChanges.pipe(
                Stream.map((event) => ({
                  version: 1 as const,
                  type: "keybindingsUpdated" as const,
                  payload: {
                    keybindings: event.keybindings,
                    issues: event.issues,
                  },
                })),
              );
              const providerStatuses = providerRegistry.streamChanges.pipe(
                Stream.map((providers) => ({
                  version: 1 as const,
                  type: "providerStatuses" as const,
                  payload: { providers },
                })),
                Stream.debounce(Duration.millis(PROVIDER_STATUS_DEBOUNCE_MS)),
              );
              const planUsageUpdates = planUsageSnapshot.changes.pipe(
                Stream.mapEffect((planUsage) =>
                  providerRegistry.getProviders.pipe(
                    Effect.map((providers) => ({
                      version: 1 as const,
                      type: "providerStatuses" as const,
                      payload: { providers, planUsage },
                    })),
                  ),
                ),
              );
              const settingsUpdates = serverSettings.streamChanges.pipe(
                Stream.map((settings) => ServerSettings.redactServerSettingsForClient(settings)),
                Stream.map((settings) => ({
                  version: 1 as const,
                  type: "settingsUpdated" as const,
                  payload: { settings },
                })),
              );

              yield* providerRegistry
                .refresh()
                .pipe(Effect.ignoreCause({ log: true }), Effect.forkScoped);

              const configUpdates = Stream.merge(
                keybindingsUpdates,
                Stream.merge(providerStatuses, Stream.merge(settingsUpdates, planUsageUpdates)),
              );
              // Only send keepalive heartbeats to clients that declared support
              // for the `heartbeat` union variant; older/version-skewed clients
              // would otherwise fail to decode it and lose the subscription.
              const liveUpdates = shouldSendServerConfigHeartbeat(input)
                ? Stream.merge(makeServerConfigHeartbeatStream(), configUpdates)
                : configUpdates;

              return Stream.concat(
                Stream.make({
                  version: 1 as const,
                  type: "snapshot" as const,
                  config: yield* loadServerConfig,
                }),
                liveUpdates,
              );
            }),
            { "rpc.aggregate": "server" },
          ),
        [WS_METHODS.subscribeServerLifecycle]: (_input) =>
          observeRpcStreamEffect(
            WS_METHODS.subscribeServerLifecycle,
            Effect.gen(function* () {
              const snapshot = yield* lifecycleEvents.snapshot;
              const snapshotEvents = Array.from(snapshot.events).toSorted(
                (left, right) => left.sequence - right.sequence,
              );
              const liveEvents = lifecycleEvents.stream.pipe(
                Stream.filter((event) => event.sequence > snapshot.sequence),
              );
              return Stream.concat(Stream.fromIterable(snapshotEvents), liveEvents);
            }),
            { "rpc.aggregate": "server" },
          ),
        [WS_METHODS.subscribeNotificationEvents]: (_input) =>
          observeRpcStream(
            WS_METHODS.subscribeNotificationEvents,
            deviceNotifications.eventsForAudience(currentSession.audienceCeiling),
            {
              "rpc.aggregate": "server",
            },
          ),
        [WS_METHODS.subscribeAuthAccess]: (_input) =>
          observeRpcStreamEffect(
            WS_METHODS.subscribeAuthAccess,
            Effect.gen(function* () {
              const initialSnapshot = yield* loadAuthAccessSnapshot();
              const revisionRef = yield* Ref.make(1);
              const accessChanges: Stream.Stream<
                PairingGrantStore.BootstrapCredentialChange | SessionStore.SessionCredentialChange
              > = Stream.merge(bootstrapCredentials.streamChanges, sessions.streamChanges);

              const liveEvents: Stream.Stream<AuthAccessStreamEvent> = accessChanges.pipe(
                Stream.mapEffect((change) =>
                  Ref.updateAndGet(revisionRef, (revision) => revision + 1).pipe(
                    Effect.map((revision) =>
                      toAuthAccessStreamEvent(change, revision, currentSessionId),
                    ),
                  ),
                ),
              );

              return Stream.concat(
                Stream.make({
                  version: 1 as const,
                  revision: 1,
                  type: "snapshot" as const,
                  payload: initialSnapshot,
                }),
                liveEvents,
              );
            }),
            { "rpc.aggregate": "auth" },
          ),
      });
    }),
  );

export const websocketRpcRouteLayer = HttpRouter.add(
  "GET",
  "/ws",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const sessions = yield* SessionStore.SessionStore;
    const session = yield* serverAuth.authenticateWebSocketUpgrade(request).pipe(
      Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
        failEnvironmentAuthInvalid(EnvironmentAuth.serverAuthCredentialReason(error)),
      ),
      Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
        failEnvironmentInternal("internal_error", error),
      ),
    );
    const rpcWebSocketHttpEffect = yield* RpcServer.toHttpEffectWebsocket(WsRpcGroup, {
      disableTracing: true,
    }).pipe(
      Effect.provide(
        makeWsRpcLayer(session).pipe(
          Layer.provideMerge(RpcSerialization.layerJson),
          Layer.provide(ProviderMaintenanceRunner.layer),
          Layer.provide(
            SourceControlDiscovery.layer.pipe(
              Layer.provide(
                SourceControlProviderRegistry.layer.pipe(
                  Layer.provide(
                    Layer.mergeAll(
                      AzureDevOpsCli.layer,
                      BitbucketApi.layer,
                      GitHubCli.layer,
                      GitLabCli.layer,
                    ),
                  ),
                  Layer.provideMerge(GitVcsDriver.layer),
                  Layer.provide(
                    VcsDriverRegistry.layer.pipe(Layer.provide(VcsProjectConfig.layer)),
                  ),
                ),
              ),
              Layer.provide(VcsProcess.layer),
            ),
          ),
        ),
      ),
    );
    return yield* Effect.acquireUseRelease(
      sessions.markConnected(session.sessionId),
      () => rpcWebSocketHttpEffect,
      () => sessions.markDisconnected(session.sessionId),
    );
  }).pipe(
    Effect.catchTags({
      EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
      EnvironmentInternalError: HttpServerRespondable.toResponse,
    }),
  ),
);
