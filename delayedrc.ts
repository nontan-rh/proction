import { LogicError } from "./error.ts";

export class DelayedRc<T> {
  #initialized: boolean;
  #body?: T;
  #count: number;
  #destroy: (x: T) => void;
  #reportError: (e: unknown) => void;

  constructor(
    destroy: (x: T) => void,
    reportError: (e: unknown) => void,
  ) {
    this.#initialized = false;
    this.#body = undefined;
    this.#count = 1;
    this.#destroy = destroy;
    this.#reportError = reportError;
  }

  initialize(body: T) {
    this.#assertNotInitialized();

    this.#initialized = true;
    this.#body = body;
  }

  get body(): T {
    this.#assertIsValid();

    return this.#body!;
  }

  incRef(): void {
    this.#assertNotFreed();

    this.#count++;
  }

  decRef(): void {
    this.#assertIsValid();

    this.#count--;

    if (this.#count <= 0) {
      try {
        this.#destroy(this.#body!);
      } catch (e: unknown) {
        try {
          this.#reportError(e);
        } catch {
          // cannot recover
        }
      } finally {
        this.#body = undefined;
      }
    }
  }

  get isFreed(): boolean {
    return this.#count <= 0;
  }

  #assertNotInitialized(): void {
    if (this.#initialized) {
      throw new LogicError("this reference counter is already initialized");
    }
  }

  #assertNotFreed(): void {
    if (this.isFreed) {
      throw new LogicError("this reference counter is already freed");
    }
  }

  #assertIsValid(): void {
    if (!this.#initialized) {
      throw new LogicError("this reference counter is not initialized yet");
    }
    if (this.isFreed) {
      throw new LogicError("this reference counter is already freed");
    }
  }
}
