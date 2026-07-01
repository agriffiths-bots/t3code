import * as NodeChildProcess from "node:child_process";

const repository = process.env.GITHUB_REPOSITORY ?? "";
const prNumber = Number(process.env.PR_NUMBER ?? process.argv[2] ?? "");
const triggerCommentId = process.env.GREPTILE_TRIGGER_COMMENT_ID ?? process.argv[3] ?? "";

if (!repository.includes("/")) {
  throw new Error("GITHUB_REPOSITORY must be set to owner/repo.");
}
if (!Number.isInteger(prNumber) || prNumber <= 0) {
  throw new Error("Pass a PR number as argv[2] or PR_NUMBER.");
}

const ghJson = (args) =>
  JSON.parse(NodeChildProcess.execFileSync("gh", ["api", ...args], { encoding: "utf8" }));

const ghJsonPages = (path, extraArgs = []) => {
  const items = [];
  for (let page = 1; ; page += 1) {
    const separator = path.includes("?") ? "&" : "?";
    const pageItems = ghJson([`${path}${separator}per_page=100&page=${page}`, ...extraArgs]);
    if (!Array.isArray(pageItems)) {
      throw new Error(`Expected ${path} page ${page} to return a JSON array.`);
    }

    items.push(...pageItems);
    if (pageItems.length < 100) {
      return items;
    }
  }
};

const ghJsonObjectPages = (path, arrayKey, extraArgs = []) => {
  const items = [];
  for (let page = 1; ; page += 1) {
    const separator = path.includes("?") ? "&" : "?";
    const pageJson = ghJson([`${path}${separator}per_page=100&page=${page}`, ...extraArgs]);
    const pageItems = pageJson[arrayKey];
    if (!Array.isArray(pageItems)) {
      throw new Error(`Expected ${path} page ${page} to return a '${arrayKey}' JSON array.`);
    }

    items.push(...pageItems);
    if (pageItems.length < 100) {
      return items;
    }
  }
};

const [owner, repo] = repository.split("/", 2);
const pr = ghJson([`repos/${owner}/${repo}/pulls/${prNumber}`]);
const comments = ghJsonPages(`repos/${owner}/${repo}/issues/${prNumber}/comments`);
const reviews = ghJsonPages(`repos/${owner}/${repo}/pulls/${prNumber}/reviews`);

const isGreptile = (login) => login === "greptile-apps" || login === "greptile-apps[bot]";
const scoreOf = (body) => {
  const match = body.match(
    /\b(?:confidence\s+score|score|grade)\s*:?\s*(\d+(?:\.\d+)?)\s*\/\s*5\b/i,
  );
  return match ? Number(match[1]) : null;
};
const reviewedCommitOf = (body) =>
  body.match(/github\.com\/[^/]+\/[^/]+\/commit\/([0-9a-f]{7,40})/i)?.[1] ?? null;

const signals = [
  ...comments
    .filter((comment) => isGreptile(comment.user?.login))
    .map((comment) => ({
      kind: "comment",
      id: comment.id,
      body: comment.body ?? "",
      updatedAt: comment.updated_at ?? comment.created_at,
    })),
  ...reviews
    .filter((review) => isGreptile(review.user?.login))
    .map((review) => ({
      kind: "review",
      id: review.id,
      body: review.body ?? "",
      state: review.state ?? null,
      reviewedCommit: review.commit_id ?? reviewedCommitOf(review.body ?? ""),
      updatedAt: review.submitted_at,
    })),
]
  .map((signal) => ({
    ...signal,
    reviewedCommit: signal.reviewedCommit ?? reviewedCommitOf(signal.body),
    score: scoreOf(signal.body),
    timestamp: Date.parse(signal.updatedAt ?? ""),
  }))
  .filter((signal) => signal.body.trim().length > 0 || signal.state === "APPROVED")
  .sort((left, right) => {
    const leftTimestamp = Number.isNaN(left.timestamp) ? 0 : left.timestamp;
    const rightTimestamp = Number.isNaN(right.timestamp) ? 0 : right.timestamp;
    return rightTimestamp - leftTimestamp;
  });

let triggerAccepted = false;
if (triggerCommentId) {
  const reactions = ghJsonPages(
    `repos/${owner}/${repo}/issues/comments/${triggerCommentId}/reactions`,
    ["-H", "Accept: application/vnd.github+json"],
  );
  triggerAccepted = reactions.some(
    (reaction) => isGreptile(reaction.user?.login) && reaction.content === "+1",
  );
}

const headSha = pr.head?.sha ?? "";
const timeline = ghJsonPages(`repos/${owner}/${repo}/issues/${prNumber}/timeline`, [
  "-H",
  "Accept: application/vnd.github+json",
]);
const headRefReachedEventTimestamp = (event) => {
  if (event.event === "head_ref_force_pushed" && event.commit_id === headSha) {
    return Date.parse(event.created_at ?? "");
  }

  return Number.NaN;
};
const headCheckRuns = ghJsonObjectPages(
  `repos/${owner}/${repo}/commits/${headSha}/check-runs?filter=all`,
  "check_runs",
  ["-H", "Accept: application/vnd.github+json"],
);
const earliestHeadCheckRunTimestamp = headCheckRuns
  .map((checkRun) => Date.parse(checkRun.started_at ?? checkRun.completed_at ?? ""))
  .filter((timestamp) => !Number.isNaN(timestamp))
  .reduce(
    (earliest, timestamp) => (earliest === null || timestamp < earliest ? timestamp : earliest),
    null,
  );
// Normal `committed` timeline events expose commit author dates, not PR arrival
// times. Check runs are the safest available proxy; without them, fail closed.
const headRefReachedTimestamps = timeline
  .map(headRefReachedEventTimestamp)
  .filter((timestamp) => !Number.isNaN(timestamp));
if (earliestHeadCheckRunTimestamp !== null) {
  headRefReachedTimestamps.push(earliestHeadCheckRunTimestamp);
}
const headRefReachedTimestamp = headRefReachedTimestamps.reduce(
  (latest, timestamp) => (latest === null || timestamp > latest ? timestamp : latest),
  null,
);
const commitMatchesHead = (reviewedCommit) =>
  typeof reviewedCommit === "string" &&
  reviewedCommit.length >= 7 &&
  headSha.startsWith(reviewedCommit);
const appliesToHead = (signal) =>
  commitMatchesHead(signal.reviewedCommit) ||
  (signal.kind === "comment" &&
    signal.score !== null &&
    signal.reviewedCommit === null &&
    headRefReachedTimestamp !== null &&
    signal.timestamp >= headRefReachedTimestamp);
const latestForHead = signals.find(appliesToHead);
const latestForHeadApproved = latestForHead?.state === "APPROVED";
const latestScored = signals.find((signal) => signal.score !== null);
const stale =
  latestScored !== undefined &&
  latestScored.reviewedCommit !== headSha &&
  !appliesToHead(latestScored);
const state = latestForHeadApproved
  ? "approved"
  : latestForHead !== undefined
    ? "current"
    : triggerAccepted
      ? "trigger-accepted"
      : stale
        ? "stale"
        : "unknown";

const result = {
  pr: prNumber,
  headSha,
  state,
  score: latestForHead?.score ?? latestScored?.score ?? null,
  approved: latestForHeadApproved,
  latestReviewedCommit: latestForHead?.reviewedCommit ?? latestScored?.reviewedCommit ?? null,
  triggerAccepted,
  latestGreptileSignalAt: latestForHead?.updatedAt ?? latestScored?.updatedAt ?? null,
};

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
