import { execFileSync } from "node:child_process";

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
  JSON.parse(execFileSync("gh", ["api", ...args], { encoding: "utf8" }));

const [owner, repo] = repository.split("/", 2);
const pr = ghJson([`repos/${owner}/${repo}/pulls/${prNumber}`]);
const comments = ghJson([`repos/${owner}/${repo}/issues/${prNumber}/comments?per_page=100`]);
const reviews = ghJson([`repos/${owner}/${repo}/pulls/${prNumber}/reviews?per_page=100`]);

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
  const reactions = ghJson([
    `repos/${owner}/${repo}/issues/comments/${triggerCommentId}/reactions`,
    "-H",
    "Accept: application/vnd.github+json",
  ]);
  triggerAccepted = reactions.some(
    (reaction) => isGreptile(reaction.user?.login) && reaction.content === "+1",
  );
}

const headSha = pr.head?.sha ?? "";
const latestForHead = signals.find((signal) => signal.reviewedCommit === headSha);
const latestApprovalForHead = signals.find(
  (signal) => signal.reviewedCommit === headSha && signal.state === "APPROVED",
);
const latestScored = signals.find((signal) => signal.score !== null);
const stale = latestScored !== undefined && latestScored.reviewedCommit !== headSha;
const state =
  latestApprovalForHead !== undefined
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
  approved: latestApprovalForHead !== undefined,
  latestReviewedCommit:
    latestApprovalForHead?.reviewedCommit ??
    latestForHead?.reviewedCommit ??
    latestScored?.reviewedCommit ??
    null,
  triggerAccepted,
  latestGreptileSignalAt: latestForHead?.updatedAt ?? latestScored?.updatedAt ?? null,
};

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
