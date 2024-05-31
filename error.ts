export class BaseError extends Error {}

export class LogicError extends BaseError {}

export class PreconditionError extends BaseError {}

export class AssertionError extends BaseError {}

export function unreachable(_x: never): never {
  throw new LogicError("unreachable");
}
