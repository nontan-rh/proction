import { LogicError } from "./_error.ts";

/**
 * A function to acquire a resource. Returns a bare resource.
 * @typeparam T The type of the acquired resource.
 * @typeparam Args The types of the arguments AcquireFn takes.
 * @param args The arguments AcquireFn takes.
 * @returns Acquired bare resource.
 */
export type AcquireFn<T, Args extends readonly unknown[]> = (
  ...args: Args
) => T;

/**
 * A function to release a resource.
 * @typeparam T The type of the released resource.
 * @param x The resource to release.
 */
export type ReleaseFn<T> = (x: T) => void;

/**
 * A wrapper that holds a resource and provides a way to dispose of it.
 * @typeparam T The type of the resource held by the DisposableWrap.
 */
export interface DisposableWrap<T> {
  /**
   * Gets the resource held by the DisposableWrap.
   * @returns The resource held by the DisposableWrap.
   */
  get body(): T;

  /**
   * Disposes of the resource held by the DisposableWrap.
   */
  [Symbol.dispose]: () => void;
}

/**
 * A function to acquire a resource. Returns a resource wrapped by DisposableWrap.
 * @typeparam T The type of the acquired resource.
 * @typeparam Args The types of the arguments ProvideFn takes.
 * @param args The arguments used to acquire the resource.
 * @returns A DisposableWrap that holds the acquired resource.
 */
export type ProvideFn<T, Args extends readonly unknown[]> = (
  ...args: Args
) => DisposableWrap<T>;

/**
 * Creates a ProvideFn combining an AcquireFn and a ReleaseFn.
 * @typeparam T The type of the acquired resource.
 * @typeparam Args The types of the arguments ProvideFn takes.
 * @param acquire A function to acquire a resource.
 * @param release A function to release a resource.
 * @returns A ProvideFn that acquires a resource wrapped by DisposableWrap.
 */
export function provider<T, Args extends readonly unknown[]>(
  acquire: AcquireFn<T, Args>,
  release: ReleaseFn<T>,
): ProvideFn<T, Args> {
  return (...args: Args) => {
    const body = acquire(...args);
    return new DisposableWrapImpl(release, body);
  };
}

class DisposableWrapImpl<T> implements DisposableWrap<T> {
  #disposed: boolean;
  #body?: T;
  #release: ReleaseFn<T>;

  constructor(release: ReleaseFn<T>, body: T) {
    this.#disposed = false;
    this.#body = body;
    this.#release = release;
  }

  get body(): T {
    const body = this.#body;
    if (body == null) {
      throw new LogicError("Provided is already released");
    }
    return body;
  }

  [Symbol.dispose]() {
    if (this.#disposed) {
      return;
    }

    const body = this.#body!;

    this.#disposed = true;
    this.#body = undefined;

    (this.#release)(body);
  }
}
