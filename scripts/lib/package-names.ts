export function resolveBarePackageName(id: string): string | undefined {
  if (
    id.length === 0 ||
    id.startsWith(".") ||
    id.startsWith("/") ||
    id.startsWith("node:") ||
    id.includes("\0")
  ) {
    return undefined;
  }

  const [first, second] = id.split("/");
  if (!first) return undefined;
  if (first.startsWith("@")) {
    return second ? `${first}/${second}` : first;
  }
  return first;
}

export function packageNameMatchesPrefix(
  packageName: string | undefined,
  prefixes: ReadonlyArray<string>,
): boolean {
  return packageName !== undefined && prefixes.some((prefix) => packageName.startsWith(prefix));
}
