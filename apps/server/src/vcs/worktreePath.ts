import type * as Path from "effect/Path";

export function resolveCreateWorktreePath(input: {
  readonly cwd: string;
  readonly refName: string;
  readonly newRefName?: string | undefined;
  readonly path: string | null;
  readonly worktreesDir: string;
  readonly pathService: Path.Path;
}): string {
  const targetBranch = input.newRefName ?? input.refName;
  const sanitizedBranch = targetBranch.replace(/\//g, "-");
  const repoName = input.pathService.basename(input.cwd);
  return input.path ?? input.pathService.join(input.worktreesDir, repoName, sanitizedBranch);
}
