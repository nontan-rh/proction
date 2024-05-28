import { LogicError } from "./error.ts";

export class Rc<T> {
  #body: T;
  #count: number;
  #destroy: (x: T) => void;
  #reportError: (e: unknown) => void;

  constructor(
    body: T,
    destroy: (x: T) => void,
    reportError: (e: unknown) => void,
  ) {
    this.#body = body;
    this.#count = 1;
    this.#destroy = destroy;
    this.#reportError = reportError;
  }

  get body(): T {
    this.#assertNotFreed();

    return this.#body;
  }

  incRef(): void {
    this.#assertNotFreed();

    this.#count++;
  }

  decRef(): void {
    this.#assertNotFreed();

    this.#count--;

    if (this.#count <= 0) {
      try {
        this.#destroy(this.#body);
      } catch (e: unknown) {
        try {
          this.#reportError(e);
        } catch {
          // cannot recover
        }
      }
    }
  }

  get isFreed(): boolean {
    return this.#count <= 0;
  }

  #assertNotFreed(): void {
    if (this.isFreed) {
      throw new LogicError("this reference counter is already freed");
    }
  }
}
