import { LogicError } from "./_error.ts";

export type AcquireFn<T, Args extends readonly unknown[]> = (
  ...args: Args
) => T;
export type ReleaseFn<T> = (x: T) => void;

export interface DisposableWrap<T> {
  get body(): T;
  [Symbol.dispose]: () => void;
}
export type ProvideFn<T, Args extends readonly unknown[]> = (
  ...args: Args
) => DisposableWrap<T>;

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
