/**
 * A base class for errors in Proction.
 */
export class BaseError extends Error {}

/**
 * An error indicating a logic error.
 * A logic error is an internal error and should not happen.
 */
export class LogicError extends BaseError {}

/**
 * An error indicating a precondition error.
 * A precondition error is caused by wrong usage of Proction.
 */
export class PreconditionError extends BaseError {}

/**
 * An error indicating an assertion error.
 * An assertion error is caused by a bug in the code and should not happen.
 */
export class AssertionError extends BaseError {}

/**
 * An internal utility function to indicate a value that should never be reached.
 * Used in switch statements to indicate a case that should never be reached.
 * @param _x a value used to switch on.
 * @returns Never.
 */
export function unreachable(_x: never): never {
  throw new LogicError("unreachable");
}
