#!/usr/bin/env node
import * as NodeFS from "node:fs";

const targets = ["apps/server/src/usage/PlanUsage.ts"];
const forbidden = [
  {
    pattern: /auth\.json/u,
    label: "Codex usage must not read auth.json directly",
  },
  {
    pattern: /\b(?:access_token|accessToken|refresh_token|refreshToken)\b/u,
    label: "Codex usage must not inspect OAuth token fields",
  },
  {
    pattern: /chatgpt\.com\/backend-api|auth\.openai\.com\/oauth\/token/u,
    label: "Codex usage must not call private ChatGPT/OAuth HTTP endpoints",
  },
  {
    pattern: /\bgrant_type\b|Authorization\s*:/u,
    label: "Codex usage must not construct OAuth refresh or bearer headers",
  },
];

const findings = [];

for (const target of targets) {
  const text = NodeFS.readFileSync(target, "utf8");
  const lines = text.split(/\r?\n/u);
  for (const rule of forbidden) {
    for (const [index, line] of lines.entries()) {
      if (rule.pattern.test(line)) {
        findings.push(`${target}:${index + 1}: ${rule.label}`);
      }
    }
  }
}

if (findings.length > 0) {
  console.error("Codex usage auth guard failed:");
  for (const finding of findings) {
    console.error(`  ${finding}`);
  }
  process.exit(1);
}
