import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";

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
      mergeStateStatus
      reviewDecision
      comments(last: 100) {
        nodes {
          body
          author { login }
        }
      }
      reviews(last: 100) {
        nodes {
          body
          state
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

const response = execFileSync(
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

const unresolvedCodexThreads = pr.reviewThreads.nodes.filter(
  (thread) =>
    !thread.isResolved && thread.comments.nodes.some((comment) => isCodex(comment.author?.login)),
);

const greptileBodies = [
  ...pr.comments.nodes
    .filter((comment) => isGreptile(comment.author?.login))
    .map((comment) => comment.body),
  ...pr.reviews.nodes
    .filter((review) => isGreptile(review.author?.login))
    .map((review) => review.body),
].join("\n\n");

const greptilePassed =
  /\b5\s*\/\s*5\b/i.test(greptileBodies) ||
  /\bscore\D+5(?:\.0)?\b/i.test(greptileBodies) ||
  /\bgrade\D+5(?:\.0)?\b/i.test(greptileBodies);

const hasGreptileSignal = greptileBodies.trim().length > 0;
const readyToMerge =
  !pr.isDraft && unresolvedCodexThreads.length === 0 && hasGreptileSignal && greptilePassed;

const summary = [
  `draft=${pr.isDraft}`,
  `mergeState=${pr.mergeStateStatus}`,
  `reviewDecision=${pr.reviewDecision ?? "UNKNOWN"}`,
  `unresolvedCodexThreads=${unresolvedCodexThreads.length}`,
  `greptileSignal=${hasGreptileSignal}`,
  `greptilePassed=${greptilePassed}`,
].join(" ");

process.stdout.write(`${summary}\n`);

if (outputPath) {
  appendFileSync(outputPath, `ready_to_merge=${readyToMerge ? "true" : "false"}\n`);
  appendFileSync(outputPath, `summary=${summary}\n`);
}
