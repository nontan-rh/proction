import { LogicError } from "./error.ts";

interface Releaser<T> {
  release(x: T): void;
}

// Invariant
export interface Provider<T, Args extends readonly unknown[]>
  extends Releaser<T> {
  acquire(...args: Args): T;
}

// Covariant wrapper for Provider<T>
export class ProviderWrap<T, Args extends readonly unknown[]> {
  acquire: (...args: Args) => ProvidedWrap<T>;

  constructor(provider: Provider<T, Args>) {
    this.acquire = (...args: Args) => {
      const body = provider.acquire(...args);
      return new ProvidedWrap(provider, body);
    };
  }
}

export interface AllocatorResult<T> {
  get body(): T;
  [Symbol.dispose](): void;
}

export class ProvidedWrap<T> {
  #disposed: boolean;
  #body?: T;
  #releaser: Releaser<T>;

  constructor(releaser: Releaser<T>, body: T) {
    this.#disposed = false;
    this.#body = body;
    this.#releaser = releaser;
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

    this.#releaser.release(body);
  }
}
