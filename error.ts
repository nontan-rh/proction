export class SubFunError extends Error {}

export class SubFunLogicError extends SubFunError {}

export class SubFunAssertionError extends SubFunError {}

export function unreachable(_x: never): never {
  throw new SubFunLogicError("unreachable");
}
