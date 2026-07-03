import * as Schema from "effect/Schema";

export const PlanUsageProviderSchema = Schema.Literals(["codex", "claude"]);
export type PlanUsageProvider = typeof PlanUsageProviderSchema.Type;

export const PlanUsageSeveritySchema = Schema.Literals(["normal", "info", "warning", "critical"]);
export type PlanUsageSeverity = typeof PlanUsageSeveritySchema.Type;

export const PlanUsageWindowSchema = Schema.Struct({
  id: Schema.String,
  provider: PlanUsageProviderSchema,
  kind: Schema.String,
  title: Schema.String,
  usedPercent: Schema.Number,
  resetAt: Schema.NullOr(Schema.String),
  used: Schema.NullOr(Schema.Number),
  limit: Schema.NullOr(Schema.Number),
  unit: Schema.NullOr(Schema.String),
  severity: Schema.NullOr(PlanUsageSeveritySchema),
});
export type PlanUsageWindow = typeof PlanUsageWindowSchema.Type;

export const PlanUsageSnapshotSchema = Schema.Struct({
  updatedAt: Schema.String,
  providers: Schema.Array(
    Schema.Struct({
      provider: PlanUsageProviderSchema,
      plan: Schema.NullOr(Schema.String),
      windows: Schema.Array(PlanUsageWindowSchema),
    }),
  ),
});
export type PlanUsageSnapshot = typeof PlanUsageSnapshotSchema.Type;
