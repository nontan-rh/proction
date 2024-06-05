import { LogicError } from "../_error.ts";

export class Box<T> {
  #isSet: boolean = false;
  #value?: T;

  static withValue<T>(value: T): Box<T> {
    const box = new Box<T>();
    box.value = value;
    return box;
  }

  get value(): T {
    const v = this.#value;
    if (!this.#isSet) {
      throw new LogicError("Box is not initialized");
    }
    return v!;
  }

  set value(v: T) {
    this.#isSet = true;
    this.#value = v;
  }

  get isSet(): boolean {
    return this.#isSet;
  }

  clear(): void {
    this.#isSet = false;
    this.#value = undefined;
  }
}
