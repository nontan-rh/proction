import { SubFunLogicError } from "./error.ts";

// Invariant
export interface Provider<T> {
  acquire(): T;
  release(x: T): void;
}

// Covariant wrapper for Provider<T>
export class ProviderWrap<T> {
  acquire: () => Provided<T>;

  constructor(provider: Provider<T>) {
    this.acquire = () => {
      const body = provider.acquire();
      return new Provided(provider, body);
    };
  }
}

export class Provided<T> {
  #body?: T;
  release: () => void;

  constructor(provider: Provider<T>, body: T) {
    this.#body = body;
    this.release = () => {
      const body = this.#body;
      if (body == null) {
        return;
      }
      this.#body = undefined;
      provider.release(body);
    };
  }

  get body(): T {
    const body = this.#body;
    if (body == null) {
      throw new SubFunLogicError("Provided is already released");
    }
    return body;
  }
}
