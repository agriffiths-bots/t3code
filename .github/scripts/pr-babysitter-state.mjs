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
query($owner: String!, $repo: String!, $number: Int!, $reviewThreadsAfter: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      isDraft
      headRefOid
      mergeStateStatus
      reviewDecision
      reviewThreads(first: 100, after: $reviewThreadsAfter) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          isResolved
          isOutdated
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

const ghGraphql = (graphqlQuery, variables) => {
  const args = ["graphql", "-f", `query=${graphqlQuery}`];
  for (const [name, value] of Object.entries(variables)) {
    if (value !== null && value !== undefined) {
      args.push("-F", `${name}=${value}`);
    }
  }

  return ghJson(args);
};

const fetchPullRequestState = () => {
  let prState = null;
  let reviewThreadsAfter = null;
  const reviewThreads = [];

  do {
    const parsed = ghGraphql(query, { owner, repo, number: prNumber, reviewThreadsAfter });
    const pagePr = parsed.data?.repository?.pullRequest;
    if (!pagePr) {
      throw new Error(`Pull request #${prNumber} was not found.`);
    }

    prState = {
      isDraft: pagePr.isDraft,
      headRefOid: pagePr.headRefOid,
      mergeStateStatus: pagePr.mergeStateStatus,
      reviewDecision: pagePr.reviewDecision,
    };

    const pageInfo = pagePr.reviewThreads?.pageInfo;
    reviewThreads.push(...(pagePr.reviewThreads?.nodes ?? []));
    reviewThreadsAfter = pageInfo?.hasNextPage ? pageInfo.endCursor : null;
  } while (reviewThreadsAfter);

  return { ...prState, reviewThreads };
};

const pr = fetchPullRequestState();
const timeline = ghJsonPages(`repos/${owner}/${repo}/issues/${prNumber}/timeline`, [
  "-H",
  "Accept: application/vnd.github+json",
]);
const headRefReachedTimestamp = timeline
  .filter((event) => event.event === "head_ref_force_pushed" && event.commit_id === pr.headRefOid)
  .map((event) => Date.parse(event.created_at ?? ""))
  .filter((timestamp) => !Number.isNaN(timestamp))
  .reduce(
    (latest, timestamp) => (latest === null || timestamp > latest ? timestamp : latest),
    null,
  );
const comments = ghJsonPages(`repos/${owner}/${repo}/issues/${prNumber}/comments`);
const reviews = ghJsonPages(`repos/${owner}/${repo}/pulls/${prNumber}/reviews`);

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

const unresolvedCodexThreads = pr.reviewThreads.filter(
  (thread) =>
    !thread.isResolved &&
    !thread.isOutdated &&
    (thread.comments?.nodes ?? []).some((comment) => isCodex(comment.author?.login)),
);

const greptileSignals = [
  ...comments
    .filter((comment) => isGreptile(comment.user?.login))
    .map((comment) => ({
      kind: "comment",
      body: comment.body ?? "",
      state: null,
      score: readGreptileScore(comment.body ?? ""),
      reviewedCommit: readReviewedCommit(comment.body ?? ""),
      timestamp: Date.parse(comment.updated_at ?? comment.created_at ?? ""),
    })),
  ...reviews
    .filter((review) => isGreptile(review.user?.login))
    .map((review) => ({
      kind: "review",
      body: review.body ?? "",
      state: review.state ?? null,
      score: readGreptileScore(review.body ?? ""),
      reviewedCommit: review.commit_id ?? readReviewedCommit(review.body ?? ""),
      timestamp: Date.parse(review.updated_at ?? review.submitted_at ?? ""),
    })),
]
  .filter((signal) => signal.body.trim().length > 0 || signal.state === "APPROVED")
  .map((signal) => ({
    ...signal,
    normalizedTimestamp: Number.isNaN(signal.timestamp) ? 0 : signal.timestamp,
  }));

const greptileSignalAppliesToHead = (signal) =>
  signal.reviewedCommit === pr.headRefOid ||
  (signal.kind === "comment" &&
    signal.score !== null &&
    signal.reviewedCommit === null &&
    headRefReachedTimestamp !== null &&
    signal.normalizedTimestamp >= headRefReachedTimestamp);

const currentGreptileSignals = greptileSignals.filter(greptileSignalAppliesToHead);

const scoredOrApprovedGreptileSignals = currentGreptileSignals.filter(
  (signal) => signal.score !== null || (signal.kind === "review" && signal.state === "APPROVED"),
);

const latestGreptileSignal = scoredOrApprovedGreptileSignals.reduce(
  (latest, signal) =>
    latest === null || signal.normalizedTimestamp > latest.normalizedTimestamp ? signal : latest,
  null,
);

const greptileScore = latestGreptileSignal?.score ?? null;
const greptileApproved =
  latestGreptileSignal?.kind === "review" && latestGreptileSignal.state === "APPROVED";
const greptilePassed = greptileApproved || (greptileScore !== null && greptileScore >= 5);
const hasGreptileSignal = latestGreptileSignal !== null;
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
