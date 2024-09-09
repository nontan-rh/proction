import { LogicError } from "./_error.ts";

export type Acquire<T, Args extends readonly unknown[]> = (...args: Args) => T;
export type Release<T> = (x: T) => void;

export interface DisposableWrap<T> {
  get body(): T;
  [Symbol.dispose]: () => void;
}
export type Provide<T, Args extends readonly unknown[]> = (
  ...args: Args
) => DisposableWrap<T>;

export function provider<T, Args extends readonly unknown[]>(
  acquire: Acquire<T, Args>,
  release: Release<T>,
): Provide<T, Args> {
  return (...args: Args) => {
    const body = acquire(...args);
    return new DisposableWrapImpl(release, body);
  };
}

class DisposableWrapImpl<T> implements DisposableWrap<T> {
  #disposed: boolean;
  #body?: T;
  #release: Release<T>;

  constructor(release: Release<T>, body: T) {
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
