import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Stdio from "effect/Stdio";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import * as RpcMessage from "effect/unstable/rpc/RpcMessage";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import * as AcpSchema from "./_generated/schema.gen.ts";
import { CLIENT_METHODS } from "./_generated/meta.gen.ts";
import * as AcpError from "./errors.ts";
const isAcpError = Schema.is(AcpError.AcpError);

export interface AcpProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly stage: "raw" | "decoded" | "decode_failed";
  readonly payload: unknown;
}

export type AcpIncomingNotification =
  | {
      readonly _tag: "SessionUpdate";
      readonly method: typeof CLIENT_METHODS.session_update;
      readonly params: AcpSchema.SessionNotification;
    }
  | {
      readonly _tag: "ElicitationComplete";
      readonly method: typeof CLIENT_METHODS.session_elicitation_complete;
      readonly params: AcpSchema.ElicitationCompleteNotification;
    }
  | {
      readonly _tag: "ExtNotification";
      readonly method: string;
      readonly params: unknown;
    };

export interface AcpPatchedProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly terminationError?: Effect.Effect<AcpError.AcpError>;
  readonly serverRequestMethods: ReadonlySet<string>;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  readonly logger?: (event: AcpProtocolLogEvent) => Effect.Effect<void, never>;
  readonly onNotification?: (
    notification: AcpIncomingNotification,
  ) => Effect.Effect<void, AcpError.AcpError, never>;
  readonly onExtRequest?: (
    method: string,
    params: unknown,
  ) => Effect.Effect<unknown, AcpError.AcpError, never>;
  readonly onTermination?: (error: AcpError.AcpError) => Effect.Effect<void, never, never>;
}

export interface AcpPatchedProtocol {
  readonly clientProtocol: RpcClient.Protocol["Service"];
  readonly serverProtocol: RpcServer.Protocol["Service"];
  readonly incoming: Stream.Stream<AcpIncomingNotification>;
  readonly request: (method: string, payload: unknown) => Effect.Effect<unknown, AcpError.AcpError>;
  readonly notify: (method: string, payload: unknown) => Effect.Effect<void, AcpError.AcpError>;
}

interface AcpPendingRequest {
  readonly deferred: Deferred.Deferred<unknown, AcpError.AcpError>;
  readonly method: string;
}

const decodeSessionUpdate = Schema.decodeUnknownEffect(AcpSchema.SessionNotification);
const decodeElicitationComplete = Schema.decodeUnknownEffect(
  AcpSchema.ElicitationCompleteNotification,
);
const parserFactory = RpcSerialization.ndJsonRpc();
const decodeWireLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeWireLine = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeBytes = (bytes: Uint8Array): string => new TextDecoder().decode(bytes);

export const makeAcpPatchedProtocol = Effect.fn("makeAcpPatchedProtocol")(function* (
  options: AcpPatchedProtocolOptions,
): Effect.fn.Return<AcpPatchedProtocol, never, Scope.Scope> {
  const parser = parserFactory.makeUnsafe();
  const inboundTextDecoder = new TextDecoder();
  const serverQueue = yield* Queue.unbounded<RpcMessage.FromClientEncoded>();
  const clientQueue = yield* Queue.unbounded<RpcMessage.FromServerEncoded>();
  const notificationQueue = yield* Queue.unbounded<AcpIncomingNotification>();
  const disconnects = yield* Queue.unbounded<number>();
  const outgoing = yield* Queue.unbounded<string | Uint8Array, Cause.Done<void>>();
  const nextRequestId = yield* Ref.make(1n);
  const terminationHandled = yield* Ref.make(false);
  const extPending = yield* Ref.make(new Map<string, AcpPendingRequest>());
  // Peer-chosen request ids that cannot cross the RPC bridge as-is (its
  // request-id type requires an integer, and duplicate keys collide) are
  // remapped to internally allocated alias ids and restored on the outgoing
  // response wire line, preserving the original JSON type — the wire type is
  // captured at raw-decode time because the parser stringifies every id.
  // `activeInboundRequestIds` tracks every id currently crossing the bridge —
  // forwarded peer ids and aliases alike — so an alias can never collide with
  // an in-flight peer id.
  let nextInboundAliasId = -1n;
  type InboundRequestIdAlias = {
    readonly originalId: string;
    readonly originalIdWasString: boolean;
  };
  type InboundAliasNode = {
    readonly aliasId: string;
    readonly originalKey: string;
    previous: InboundAliasNode | undefined;
    next: InboundAliasNode | undefined;
  };
  const inboundRequestIdAliases = new Map<string, InboundRequestIdAlias>();
  const inboundRequestIdAliasesByOriginal = new Map<string, InboundAliasNode>();
  const inboundAliasNodesByAliasId = new Map<string, InboundAliasNode>();
  const activeInboundRequestIds = new Set<string>();
  const originalRequestIdKey = (requestId: AcpError.AcpRequestId, idWasString: boolean) =>
    `${idWasString ? "string" : "number"}:${requestId}`;
  const isForwardableRequestId = (requestId: string): boolean => /^-?\d+$/.test(requestId);
  const allocateInboundAliasId = (): string => {
    let aliasId = String(nextInboundAliasId);
    nextInboundAliasId -= 1n;
    while (activeInboundRequestIds.has(aliasId) || inboundRequestIdAliases.has(aliasId)) {
      aliasId = String(nextInboundAliasId);
      nextInboundAliasId -= 1n;
    }
    return aliasId;
  };
  const registerInboundAlias = (originalId: string, originalIdWasString: boolean): string => {
    const aliasId = allocateInboundAliasId();
    const alias = { originalId, originalIdWasString } satisfies InboundRequestIdAlias;
    inboundRequestIdAliases.set(aliasId, alias);
    const originalKey = originalRequestIdKey(alias.originalId, alias.originalIdWasString);
    const previous = inboundRequestIdAliasesByOriginal.get(originalKey);
    const node: InboundAliasNode = { aliasId, originalKey, previous, next: undefined };
    if (previous !== undefined) {
      previous.next = node;
    }
    inboundRequestIdAliasesByOriginal.set(originalKey, node);
    inboundAliasNodesByAliasId.set(aliasId, node);
    activeInboundRequestIds.add(aliasId);
    return aliasId;
  };
  const unregisterInboundAliasControlMapping = (aliasId: string): void => {
    const node = inboundAliasNodesByAliasId.get(aliasId);
    if (node === undefined) {
      return;
    }
    if (node.previous !== undefined) {
      node.previous.next = node.next;
    }
    if (node.next !== undefined) {
      node.next.previous = node.previous;
    }
    if (inboundRequestIdAliasesByOriginal.get(node.originalKey) === node) {
      if (node.previous === undefined) {
        inboundRequestIdAliasesByOriginal.delete(node.originalKey);
      } else {
        inboundRequestIdAliasesByOriginal.set(node.originalKey, node.previous);
      }
    }
    inboundAliasNodesByAliasId.delete(aliasId);
  };
  const rollbackPreparedInboundIds = (requestIds: ReadonlyArray<string>): void => {
    for (const requestId of requestIds) {
      const alias = inboundRequestIdAliases.get(requestId);
      if (alias !== undefined) {
        unregisterInboundAliasControlMapping(requestId);
        inboundRequestIdAliases.delete(requestId);
      }
      activeInboundRequestIds.delete(requestId);
    }
  };

  // Normalize request ids before the JSON-RPC parser records batch keys. The
  // parser stringifies ids, so raw `42` and `"42"` otherwise collide before
  // the later routing layer can distinguish them, and extension replies also
  // need the original JSON type restored.
  let inboundWireBuffer = "";
  const prepareInboundChunk = (
    chunkText: string,
  ): {
    readonly encoded: string;
    readonly idWasStringFlags: Array<boolean | undefined>;
    readonly preparedRequestIds: ReadonlyArray<string>;
  } => {
    inboundWireBuffer += chunkText;
    const flags: Array<boolean | undefined> = [];
    const preparedRequestIds: Array<string> = [];
    let encoded = "";
    try {
      let position = 0;
      let nlIndex = inboundWireBuffer.indexOf("\n", position);
      while (nlIndex !== -1) {
        const item = decodeWireLine(inboundWireBuffer.slice(position, nlIndex));
        const preparedValues = (Array.isArray(item) ? item : [item]).map((value) => {
          if (typeof value !== "object" || value === null) {
            flags.push(undefined);
            return value;
          }
          const params = "params" in value ? value.params : undefined;
          if (
            "method" in value &&
            typeof value.method === "string" &&
            value.method.startsWith("@effect/rpc/") &&
            typeof params === "object" &&
            params !== null &&
            "requestId" in params
          ) {
            const requestIdWasString = typeof params.requestId === "string";
            flags.push(requestIdWasString);
            if (params.requestId === 0) {
              const routedRequestId = inboundRequestIdAliasesByOriginal.get(
                originalRequestIdKey("0", false),
              )?.aliasId;
              return {
                ...value,
                params: {
                  ...params,
                  requestId: routedRequestId === undefined ? "0" : Number(routedRequestId),
                },
              };
            }
            return value;
          }
          const idWasString = "id" in value ? typeof value.id === "string" : undefined;
          flags.push(idWasString);
          if (
            !("method" in value) ||
            typeof value.method !== "string" ||
            !("id" in value) ||
            (typeof value.id !== "string" && typeof value.id !== "number")
          ) {
            return value;
          }
          const originalId = String(value.id);
          const canForward =
            isForwardableRequestId(originalId) && idWasString !== true && originalId !== "0";
          if (canForward && !activeInboundRequestIds.has(originalId)) {
            activeInboundRequestIds.add(originalId);
            preparedRequestIds.push(originalId);
            return value;
          }
          const aliasId = registerInboundAlias(originalId, idWasString === true);
          preparedRequestIds.push(aliasId);
          return { ...value, id: Number(aliasId) };
        });
        encoded += `${encodeWireLine(Array.isArray(item) ? preparedValues : preparedValues[0])}\n`;
        position = nlIndex + 1;
        nlIndex = inboundWireBuffer.indexOf("\n", position);
      }
      inboundWireBuffer = inboundWireBuffer.slice(position);
      return { encoded, idWasStringFlags: flags, preparedRequestIds };
    } catch (cause) {
      rollbackPreparedInboundIds(preparedRequestIds);
      throw cause;
    }
  };

  const restoreAliasedReplyIds = (
    line: string | Uint8Array,
    transientAlias?: { readonly encodedId: string; readonly alias: InboundRequestIdAlias },
    omitEmptyNotificationId = false,
  ): string | Uint8Array => {
    if (
      inboundRequestIdAliases.size === 0 &&
      transientAlias === undefined &&
      !omitEmptyNotificationId
    ) {
      return line;
    }
    const lineText = typeof line === "string" ? line : decodeBytes(line);
    const decodedFrames = lineText
      .split("\n")
      .filter((frame) => frame.length > 0)
      .map((frame) => decodeWireLine(frame));
    let restoredAny = false;
    const restore = (value: unknown): unknown => {
      if (value === null || typeof value !== "object" || Array.isArray(value)) {
        return value;
      }
      const response = value as Record<string, unknown>;
      if (!("id" in response)) {
        return value;
      }
      if (omitEmptyNotificationId && response.id === "") {
        const { id: _, ...notification } = response;
        restoredAny = true;
        return notification;
      }
      const encodedId = String(response.id);
      const alias =
        transientAlias?.encodedId === encodedId
          ? transientAlias.alias
          : inboundRequestIdAliases.get(encodedId);
      if (alias === undefined) {
        return value;
      }
      if (response.chunk !== true && transientAlias?.encodedId !== encodedId) {
        inboundRequestIdAliases.delete(encodedId);
        activeInboundRequestIds.delete(encodedId);
      }
      restoredAny = true;
      return {
        ...response,
        id: alias.originalIdWasString ? alias.originalId : Number(alias.originalId),
      };
    };
    const restoredFrames = decodedFrames.map((decoded) =>
      Array.isArray(decoded) ? decoded.map(restore) : restore(decoded),
    );
    return restoredAny
      ? `${restoredFrames.map((frame) => encodeWireLine(frame)).join("\n")}\n`
      : line;
  };

  const logProtocol = (event: AcpProtocolLogEvent) => {
    if (event.direction === "incoming" && !options.logIncoming) {
      return Effect.void;
    }
    if (event.direction === "outgoing" && !options.logOutgoing) {
      return Effect.void;
    }
    return (
      options.logger?.(event) ??
      Effect.logDebug("ACP protocol event").pipe(Effect.annotateLogs({ event }))
    );
  };

  const offerOutgoing = Effect.fn("offerOutgoing")(function* (
    message: RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded,
  ) {
    yield* logProtocol({
      direction: "outgoing",
      stage: "decoded",
      payload: message,
    });

    const method = message._tag === "Request" ? message.tag : undefined;
    const encodedRequestId =
      message._tag === "Request"
        ? message.id
        : "requestId" in message
          ? message.requestId
          : undefined;
    const requestId = encodedRequestId === "" ? undefined : encodedRequestId;
    const omitEmptyNotificationId = message._tag === "Request" && message.id === "";
    const aliasedPeer =
      message._tag === "Exit" || message._tag === "Chunk"
        ? inboundRequestIdAliases.get(String(message.requestId))
        : undefined;
    const inboundChunkTarget =
      message._tag === "Chunk" && activeInboundRequestIds.has(String(message.requestId))
        ? (aliasedPeer ?? {
            originalId: String(message.requestId),
            originalIdWasString: false,
          })
        : undefined;
    const encoded = yield* Effect.try({
      try: () => {
        // A Chunk is not a terminal batch response. Encode it with a fresh id
        // that the parser cannot associate with an inbound batch, then restore
        // the real peer id on the emitted wire frame.
        const transientChunkAliasId =
          inboundChunkTarget !== undefined ? allocateInboundAliasId() : undefined;
        const messageForEncoding =
          transientChunkAliasId === undefined
            ? message
            : { ...message, requestId: transientChunkAliasId };
        const line = parser.encode(messageForEncoding);
        if (!line) {
          return line;
        }
        if (
          message._tag !== "Exit" &&
          transientChunkAliasId === undefined &&
          !omitEmptyNotificationId
        ) {
          return line;
        }
        return restoreAliasedReplyIds(
          line,
          inboundChunkTarget === undefined || transientChunkAliasId === undefined
            ? undefined
            : {
                alias: inboundChunkTarget,
                encodedId: transientChunkAliasId,
              },
          omitEmptyNotificationId,
        );
      },
      catch: (cause) => AcpError.AcpProtocolParseError.fromEncodingError(method, requestId, cause),
    });
    if (message._tag === "Exit") {
      if (aliasedPeer === undefined) {
        activeInboundRequestIds.delete(String(message.requestId));
      } else {
        unregisterInboundAliasControlMapping(String(message.requestId));
      }
    }

    if (encoded) {
      yield* logProtocol({
        direction: "outgoing",
        stage: "raw",
        payload: typeof encoded === "string" ? encoded : new TextDecoder().decode(encoded),
      });

      yield* Queue.offer(outgoing, encoded).pipe(Effect.asVoid);
    }
  });

  const resolveExtPending = (
    requestId: AcpError.AcpRequestId,
    onFound: (pendingRequest: AcpPendingRequest) => Effect.Effect<void>,
  ) =>
    Ref.modify(extPending, (pending) => {
      const pendingRequest = pending.get(String(requestId));
      if (!pendingRequest) {
        return [Effect.void, pending] as const;
      }
      const next = new Map(pending);
      next.delete(String(requestId));
      return [onFound(pendingRequest), next] as const;
    }).pipe(Effect.flatten);

  const removeExtPending = (requestId: AcpError.AcpRequestId) =>
    Ref.update(extPending, (pending) => {
      if (!pending.has(String(requestId))) {
        return pending;
      }
      const next = new Map(pending);
      next.delete(String(requestId));
      return next;
    });

  const completeExtPendingFailure = (requestId: AcpError.AcpRequestId, error: AcpError.AcpError) =>
    resolveExtPending(requestId, ({ deferred }) => Deferred.fail(deferred, error));

  const completeExtPendingSuccess = (requestId: AcpError.AcpRequestId, value: unknown) =>
    resolveExtPending(requestId, ({ deferred }) => Deferred.succeed(deferred, value));

  const failAllExtPending = (error: AcpError.AcpError) =>
    Ref.getAndSet(extPending, new Map()).pipe(
      Effect.flatMap((pending) =>
        Effect.forEach([...pending.values()], ({ deferred }) => Deferred.fail(deferred, error), {
          discard: true,
        }),
      ),
    );

  const dispatchNotification = (notification: AcpIncomingNotification) =>
    Queue.offer(notificationQueue, notification).pipe(
      Effect.andThen(
        options.onNotification
          ? options.onNotification(notification).pipe(Effect.catch(() => Effect.void))
          : Effect.void,
      ),
      Effect.asVoid,
    );

  const emitClientProtocolError = (error: AcpError.AcpError) =>
    Queue.offer(clientQueue, {
      _tag: "ClientProtocolError",
      error: new RpcClientError.RpcClientError({
        reason: new RpcClientError.RpcClientDefect({
          message: "ACP protocol terminated.",
          cause: error,
        }),
      }),
    }).pipe(Effect.asVoid);

  const handleTermination = (classify: () => Effect.Effect<AcpError.AcpError | undefined>) =>
    Ref.modify(terminationHandled, (handled) => {
      if (handled) {
        return [Effect.void, true] as const;
      }
      return [
        Effect.gen(function* () {
          yield* Queue.offer(disconnects, 0);
          const error = yield* classify();
          if (!error) {
            return;
          }
          yield* failAllExtPending(error);
          yield* emitClientProtocolError(error);
          if (options.onTermination) {
            yield* options.onTermination(error);
          }
        }),
        true,
      ] as const;
    }).pipe(Effect.flatten);

  const respondWithSuccess = (requestId: AcpError.AcpRequestId, value: unknown) =>
    offerOutgoing({
      _tag: "Exit",
      requestId,
      exit: {
        _tag: "Success",
        value,
      },
    });

  const respondWithError = (requestId: AcpError.AcpRequestId, error: AcpError.AcpRequestError) =>
    offerOutgoing({
      _tag: "Exit",
      requestId,
      exit: {
        _tag: "Failure",
        cause: [
          {
            _tag: "Fail",
            error: error.toProtocolError(),
          },
        ],
      },
    });

  // A notification whose payload fails schema decoding must never terminate
  // the transport or abort the remaining messages decoded from the same
  // buffer: agents ship new session/update variants ahead of this client, and
  // a prompt response following the poison message still has to be routed.
  const dropUndecodableNotification = (error: AcpError.AcpProtocolParseError) =>
    logProtocol({
      direction: "incoming",
      stage: "decode_failed",
      payload: {
        operation: error.operation,
        ...(error.method === undefined ? {} : { method: error.method }),
        ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
        ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
        ...(error.maximumPathDepth === undefined
          ? {}
          : { maximumPathDepth: error.maximumPathDepth }),
      },
    }).pipe(
      // Never log the error object itself: its schema cause embeds the
      // rejected notification values.
      Effect.andThen(
        Effect.logWarning("Dropped ACP notification with undecodable payload.").pipe(
          Effect.annotateLogs({
            operation: error.operation,
            ...(error.method === undefined ? {} : { method: error.method }),
            ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
          }),
        ),
      ),
    );

  const handleExtRequest = (message: RpcMessage.RequestEncoded) => {
    if (!options.onExtRequest) {
      return respondWithError(message.id, AcpError.AcpRequestError.methodNotFound(message.tag));
    }
    return options.onExtRequest(message.tag, message.payload).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          respondWithError(
            message.id,
            AcpError.AcpRequestError.fromExtensionHandlerError(error, message.tag),
          ),
        onSuccess: (value) => respondWithSuccess(message.id, value),
      }),
    );
  };

  const handleRequestEncoded = (message: RpcMessage.RequestEncoded) => {
    if (message.id === "") {
      if (message.tag === CLIENT_METHODS.session_update) {
        return decodeSessionUpdate(message.payload).pipe(
          Effect.map(
            (params) =>
              ({
                _tag: "SessionUpdate",
                method: CLIENT_METHODS.session_update,
                params,
              }) satisfies AcpIncomingNotification,
          ),
          Effect.mapError((cause) =>
            AcpError.AcpProtocolParseError.fromSchemaError(
              "decode-notification-payload",
              CLIENT_METHODS.session_update,
              cause,
            ),
          ),
          Effect.flatMap(dispatchNotification),
          Effect.catchTag("AcpProtocolParseError", dropUndecodableNotification),
        );
      }
      if (message.tag === CLIENT_METHODS.session_elicitation_complete) {
        return decodeElicitationComplete(message.payload).pipe(
          Effect.map(
            (params) =>
              ({
                _tag: "ElicitationComplete",
                method: CLIENT_METHODS.session_elicitation_complete,
                params,
              }) satisfies AcpIncomingNotification,
          ),
          Effect.mapError((cause) =>
            AcpError.AcpProtocolParseError.fromSchemaError(
              "decode-notification-payload",
              CLIENT_METHODS.session_elicitation_complete,
              cause,
            ),
          ),
          Effect.flatMap(dispatchNotification),
          Effect.catchTag("AcpProtocolParseError", dropUndecodableNotification),
        );
      }
      return dispatchNotification({
        _tag: "ExtNotification",
        method: message.tag,
        params: message.payload,
      });
    }

    if (!options.serverRequestMethods.has(message.tag)) {
      return handleExtRequest(message).pipe(
        Effect.catchTags({
          AcpProtocolParseError: (error) =>
            Effect.logWarning(error).pipe(
              Effect.annotateLogs({
                method: message.tag,
                requestId: message.id,
                operation: error.operation,
              }),
              Effect.andThen(
                respondWithError(
                  message.id,
                  AcpError.AcpRequestError.fromExtensionResponseEncodingError(
                    message.tag,
                    message.id,
                    error,
                  ),
                ),
              ),
            ),
        }),
        Effect.asVoid,
      );
    }

    // Request ids were normalized before parser.decode so the parser and RPC
    // bridge share the same unique batch/request key.
    return Queue.offer(serverQueue, message).pipe(Effect.asVoid);
  };

  // Every request id this client issues is an integer (both the RPC client's
  // counter and `nextRequestId` for extension requests), and the RPC client
  // converts forwarded response ids with `BigInt`. A response whose id is
  // anything else answers no outstanding request and is a peer bug — grok CLI
  // 0.2.93 broadcasts a response with the literal id "skills-reload" to every
  // session when its skills directory changes — and forwarding it would kill
  // the receive loop with a BigInt conversion defect, silently hanging every
  // in-flight request.
  const dropForeignPeerMessage = (tag: string, requestId: AcpError.AcpRequestId) =>
    Effect.logWarning("Dropping ACP message with a foreign request id.").pipe(
      Effect.annotateLogs({ tag, requestId }),
    );

  const isForeignPeerResponseId = (requestId: AcpError.AcpRequestId, idWasString?: boolean) =>
    idWasString === true || !isForwardableRequestId(String(requestId));

  const handleExitEncoded = (message: RpcMessage.ResponseExitEncoded, idWasString?: boolean) => {
    if (isForeignPeerResponseId(message.requestId, idWasString)) {
      return dropForeignPeerMessage("Exit", message.requestId);
    }
    return Ref.get(extPending).pipe(
      Effect.flatMap((pending) => {
        const pendingRequest = pending.get(String(message.requestId));
        if (!pendingRequest) {
          return Queue.offer(clientQueue, message).pipe(Effect.asVoid);
        }
        if (message.exit._tag === "Success") {
          return completeExtPendingSuccess(message.requestId, message.exit.value);
        }
        const failure = message.exit.cause.find((entry) => entry._tag === "Fail");
        if (failure && isProtocolError(failure.error)) {
          return completeExtPendingFailure(
            message.requestId,
            AcpError.AcpRequestError.fromProtocolError(failure.error, {
              method: pendingRequest.method,
              requestId: message.requestId,
              cause: message.exit.cause,
            }),
          );
        }
        return completeExtPendingFailure(
          message.requestId,
          AcpError.AcpRequestError.fromExtensionResponseFailure(
            pendingRequest.method,
            message.requestId,
            message.exit.cause,
          ),
        );
      }),
    );
  };

  const routeDecodedMessage = (
    message: RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded,
    idWasString?: boolean,
  ): Effect.Effect<void, AcpError.AcpError> => {
    switch (message._tag) {
      case "Request":
        return handleRequestEncoded(message);
      case "Exit":
        return handleExitEncoded(message, idWasString);
      case "Chunk":
        if (isForeignPeerResponseId(message.requestId, idWasString)) {
          return dropForeignPeerMessage("Chunk", message.requestId);
        }
        return Ref.get(extPending).pipe(
          Effect.flatMap((pending) => {
            const pendingRequest = pending.get(String(message.requestId));
            if (pendingRequest) {
              return completeExtPendingFailure(
                message.requestId,
                AcpError.AcpRequestError.unsupportedStreamingResponse(
                  pendingRequest.method,
                  message.requestId,
                ),
              );
            }
            return Queue.offer(clientQueue, message).pipe(Effect.asVoid);
          }),
        );
      case "Defect":
      case "ClientProtocolError":
      case "Pong":
        return Queue.offer(clientQueue, message).pipe(Effect.asVoid);
      case "Ack":
      case "Interrupt":
        return Queue.offer(serverQueue, {
          ...message,
          requestId:
            inboundRequestIdAliasesByOriginal.get(
              originalRequestIdKey(message.requestId, idWasString === true),
            )?.aliasId ?? message.requestId,
        }).pipe(Effect.asVoid);
      case "Ping":
      case "Eof":
        return Queue.offer(serverQueue, message).pipe(Effect.asVoid);
    }
  };

  yield* options.stdio.stdin.pipe(
    Stream.runForEach((data) => {
      const chunkText =
        typeof data === "string"
          ? `${inboundTextDecoder.decode()}${data}`
          : inboundTextDecoder.decode(data, { stream: true });
      return logProtocol({
        direction: "incoming",
        stage: "raw",
        payload: chunkText,
      }).pipe(
        Effect.flatMap(() =>
          Effect.try({
            try: () => {
              const prepared = prepareInboundChunk(chunkText);
              try {
                const messages = parser.decode(prepared.encoded) as ReadonlyArray<
                  RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded
                >;
                return { messages, idWasStringFlags: prepared.idWasStringFlags };
              } catch (cause) {
                rollbackPreparedInboundIds(prepared.preparedRequestIds);
                throw cause;
              }
            },
            catch: (cause) =>
              new AcpError.AcpProtocolParseError({
                operation: "decode-wire-message",
                cause,
              }),
          }),
        ),
        Effect.tap(({ messages }) =>
          logProtocol({
            direction: "incoming",
            stage: "decoded",
            payload: messages,
          }),
        ),
        Effect.tapErrorTag("AcpProtocolParseError", (error) =>
          logProtocol({
            direction: "incoming",
            stage: "decode_failed",
            payload: {
              operation: error.operation,
              ...(error.method === undefined ? {} : { method: error.method }),
              ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
              ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
              ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
              ...(error.maximumPathDepth === undefined
                ? {}
                : { maximumPathDepth: error.maximumPathDepth }),
            },
          }),
        ),
        Effect.flatMap(({ messages, idWasStringFlags }) =>
          Effect.forEach(
            messages,
            (message, index) => routeDecodedMessage(message, idWasStringFlags[index]),
            {
              discard: true,
            },
          ),
        ),
      );
    }),
    Effect.matchEffect({
      onFailure: (error) => {
        const normalized: AcpError.AcpError = isAcpError(error)
          ? error
          : new AcpError.AcpTransportError({
              operation: "read-input-stream",
              cause: error,
            });
        return handleTermination(() => Effect.succeed(normalized));
      },
      onSuccess: () => {
        inboundTextDecoder.decode();
        return handleTermination(
          () =>
            options.terminationError ?? Effect.succeed(new AcpError.AcpInputStreamEndedError({})),
        );
      },
    }),
    Effect.forkScoped,
  );

  yield* Stream.fromQueue(outgoing).pipe(Stream.run(options.stdio.stdout()), Effect.forkScoped);

  const clientProtocol = RpcClient.Protocol.of({
    run: (_clientId, f) =>
      Stream.fromQueue(clientQueue).pipe(
        Stream.runForEach((message) => f(message)),
        Effect.forever,
      ),
    send: (_clientId, request) =>
      offerOutgoing(request).pipe(
        Effect.mapError(
          (error) =>
            new RpcClientError.RpcClientError({
              reason: new RpcClientError.RpcClientDefect({
                message: "Failed to send ACP protocol message.",
                cause: error,
              }),
            }),
        ),
      ),
    supportsAck: true,
    supportsTransferables: false,
  });

  const serverProtocol = RpcServer.Protocol.of({
    run: (f) =>
      Stream.fromQueue(serverQueue).pipe(
        Stream.runForEach((message) => f(0, message)),
        Effect.forever,
      ),
    disconnects,
    send: (_clientId, response) => offerOutgoing(response).pipe(Effect.orDie),
    end: (_clientId) => Queue.end(outgoing),
    clientIds: Effect.succeed(new Set([0])),
    initialMessage: Effect.succeedNone,
    supportsAck: true,
    supportsTransferables: false,
    supportsSpanPropagation: true,
  });

  const sendNotification = Effect.fn("sendNotification")(function* (
    method: string,
    payload: unknown,
  ) {
    yield* offerOutgoing({
      _tag: "Request",
      id: "",
      tag: method,
      payload,
      headers: [],
    });
  });

  const sendRequest = Effect.fn("sendRequest")(function* (method: string, payload: unknown) {
    const requestId = yield* Ref.modify(
      nextRequestId,
      (current) => [current, current + 1n] as const,
    );
    const deferred = yield* Deferred.make<unknown, AcpError.AcpError>();
    yield* Ref.update(extPending, (pending) =>
      new Map(pending).set(String(requestId), { deferred, method }),
    );
    yield* offerOutgoing({
      _tag: "Request",
      id: Number(requestId),
      tag: method,
      payload,
      headers: [],
    }).pipe(Effect.tapError(() => removeExtPending(String(requestId))));
    return yield* Deferred.await(deferred).pipe(
      Effect.onInterrupt(() => removeExtPending(String(requestId))),
    );
  });

  return {
    clientProtocol,
    serverProtocol,
    get incoming() {
      return Stream.fromQueue(notificationQueue);
    },
    request: sendRequest,
    notify: sendNotification,
  } satisfies AcpPatchedProtocol;
});

function isProtocolError(
  value: unknown,
): value is { code: number; message: string; data?: unknown } {
  return (
    typeof value === "object" &&
    value !== null &&
    "code" in value &&
    typeof value.code === "number" &&
    "message" in value &&
    typeof value.message === "string"
  );
}
