import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";

const prNumber = Number(process.env.PR_NUMBER ?? "");
const repository = process.env.GITHUB_REPOSITORY ?? "";
const outputPath = process.env.GITHUB_OUTPUT;

if (!Number.isInteger(prNumber) || prNumber <= 0) {
  throw new Error("PR_NUMBER must be a positive integer.");
}
if (!repository.includes("/")) {
  throw new Error("GITHUB_REPOSITORY must be set.");
}

const [owner, repo] = repository.split("/", 2);

const query = `
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      isDraft
      headRefOid
      mergeStateStatus
      reviewDecision
      comments(last: 100) {
        nodes {
          body
          createdAt
          updatedAt
          author { login }
        }
      }
      reviews(last: 100) {
        nodes {
          body
          createdAt
          submittedAt
          updatedAt
          state
          commit { oid }
          author { login }
        }
      }
      reviewThreads(first: 100) {
        nodes {
          isResolved
          comments(first: 20) {
            nodes {
              body
              author { login }
            }
          }
        }
      }
    }
  }
}`;

const response = NodeChildProcess.execFileSync(
  "gh",
  [
    "api",
    "graphql",
    "-f",
    `query=${query}`,
    "-F",
    `owner=${owner}`,
    "-F",
    `repo=${repo}`,
    "-F",
    `number=${prNumber}`,
  ],
  { encoding: "utf8" },
);

const parsed = JSON.parse(response);
const pr = parsed.data?.repository?.pullRequest;
if (!pr) {
  throw new Error(`Pull request #${prNumber} was not found.`);
}

const codexAuthorLogins = new Set(["chatgpt-codex-connector", "chatgpt-codex-connector[bot]"]);
const greptileAuthorLogins = new Set(["greptile-apps", "greptile-apps[bot]"]);

const isCodex = (login) => codexAuthorLogins.has(login ?? "");
const isGreptile = (login) => greptileAuthorLogins.has(login ?? "");
const readGreptileScore = (body) => {
  const scoreMatch = body.match(
    /\b(?:confidence\s+score|score|grade)\s*:?\s*(\d+(?:\.\d+)?)\s*\/\s*5\b/i,
  );
  return scoreMatch ? Number(scoreMatch[1]) : null;
};
const readReviewedCommit = (body) =>
  body.match(/github\.com\/[^/]+\/[^/]+\/commit\/([0-9a-f]{7,40})/i)?.[1] ?? null;

const unresolvedCodexThreads = pr.reviewThreads.nodes.filter(
  (thread) =>
    !thread.isResolved && thread.comments.nodes.some((comment) => isCodex(comment.author?.login)),
);

const greptileSignals = [
  ...pr.comments.nodes
    .filter((comment) => isGreptile(comment.author?.login))
    .map((comment) => ({
      kind: "comment",
      body: comment.body,
      state: null,
      score: readGreptileScore(comment.body),
      reviewedCommit: readReviewedCommit(comment.body),
      timestamp: Date.parse(comment.updatedAt ?? comment.createdAt ?? ""),
    })),
  ...pr.reviews.nodes
    .filter((review) => isGreptile(review.author?.login))
    .map((review) => ({
      kind: "review",
      body: review.body,
      state: review.state,
      score: readGreptileScore(review.body),
      reviewedCommit: review.commit?.oid ?? readReviewedCommit(review.body),
      timestamp: Date.parse(review.updatedAt ?? review.submittedAt ?? review.createdAt ?? ""),
    })),
]
  .filter((signal) => signal.body.trim().length > 0 || signal.state === "APPROVED")
  .map((signal) => ({
    ...signal,
    normalizedTimestamp: Number.isNaN(signal.timestamp) ? 0 : signal.timestamp,
  }));

const currentGreptileSignals = greptileSignals.filter(
  (signal) => signal.reviewedCommit === pr.headRefOid,
);

const latestScoredGreptileSignal = currentGreptileSignals
  .filter((signal) => signal.score !== null)
  .reduce(
    (latest, signal) =>
      latest === null || signal.normalizedTimestamp > latest.normalizedTimestamp ? signal : latest,
    null,
  );

const greptileScore = latestScoredGreptileSignal?.score ?? null;
const greptileApproved = currentGreptileSignals.some(
  (signal) => signal.kind === "review" && signal.state === "APPROVED",
);
const greptilePassed = greptileApproved || (greptileScore !== null && greptileScore >= 5);
const hasGreptileSignal = currentGreptileSignals.length > 0;
const readyToMerge =
  !pr.isDraft && unresolvedCodexThreads.length === 0 && hasGreptileSignal && greptilePassed;

const summary = [
  `draft=${pr.isDraft}`,
  `head=${pr.headRefOid}`,
  `mergeState=${pr.mergeStateStatus}`,
  `reviewDecision=${pr.reviewDecision ?? "UNKNOWN"}`,
  `unresolvedCodexThreads=${unresolvedCodexThreads.length}`,
  `greptileSignal=${hasGreptileSignal}`,
  `greptileScore=${greptileScore ?? "UNKNOWN"}`,
  `greptileApproved=${greptileApproved}`,
  `greptilePassed=${greptilePassed}`,
].join(" ");

process.stdout.write(`${summary}\n`);

if (outputPath) {
  NodeFS.appendFileSync(outputPath, `ready_to_merge=${readyToMerge ? "true" : "false"}\n`);
  NodeFS.appendFileSync(outputPath, `summary=${summary}\n`);
}
