import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

import { AuthSessionId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { DataAudience, type DataAudience as DataAudienceType } from "./orchestration.ts";

const ASSET_SESSION_BINDING_ID_MAX_LENGTH = 256;

export const AssetSessionBindingId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(ASSET_SESSION_BINDING_ID_MAX_LENGTH),
);
export type AssetSessionBindingId = typeof AssetSessionBindingId.Type;

// Claims issued before audience binding landed had no dataAudience. Decode those
// as private so compatibility can never widen access; every newly encoded claim
// is required to carry the field explicitly.
const AssetClaimDataAudience = Schema.optionalKey(DataAudience).pipe(
  Schema.decodeTo(Schema.toType(DataAudience), {
    decode: SchemaGetter.withDefault(Effect.succeed<DataAudienceType>("private")),
    encode: SchemaGetter.required(),
  }),
);

const NullableAssetSessionBindingId = Schema.NullOr(AssetSessionBindingId);
const AssetClaimSessionBindingId = Schema.optionalKey(NullableAssetSessionBindingId).pipe(
  Schema.decodeTo(Schema.toType(NullableAssetSessionBindingId), {
    decode: SchemaGetter.withDefault(Effect.succeed<AssetSessionBindingId | null>(null)),
    encode: SchemaGetter.required(),
  }),
);

const AudienceBoundAssetClaimFields = {
  dataAudience: AssetClaimDataAudience,
  surfaceBindingId: AssetClaimSessionBindingId,
};

export const AssetAttachmentClaim = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("attachment"),
  attachmentId: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
  expiresAt: Schema.Number,
  ...AudienceBoundAssetClaimFields,
});
export type AssetAttachmentClaim = typeof AssetAttachmentClaim.Type;

export const AssetClaim = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file"),
    workspaceRoot: Schema.String,
    baseRelativePath: Schema.String,
    expiresAt: Schema.Number,
    ...AudienceBoundAssetClaimFields,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file-exact"),
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    expiresAt: Schema.Number,
    ...AudienceBoundAssetClaimFields,
  }),
  AssetAttachmentClaim,
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("project-favicon"),
    workspaceRoot: Schema.String,
    relativePath: Schema.NullOr(Schema.String),
    expiresAt: Schema.Number,
    ...AudienceBoundAssetClaimFields,
  }),
]);
export type AssetClaim = typeof AssetClaim.Type;

export const AssetClaimJson = Schema.fromJsonString(AssetClaim);

export const AssetSurfaceCredentialClaim = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("asset-surface"),
  surfaceSessionId: AuthSessionId,
  surfaceBindingId: AssetSessionBindingId,
  expiresAt: Schema.Number,
});
export type AssetSurfaceCredentialClaim = typeof AssetSurfaceCredentialClaim.Type;

export const AssetSurfaceCredentialClaimJson = Schema.fromJsonString(AssetSurfaceCredentialClaim);
