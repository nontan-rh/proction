import { SubFunLogicError } from "./error.ts";

export class Box<T> {
  #value: [T] | [] = [];

  static withValue<T>(value: T): Box<T> {
    const box = new Box<T>();
    box.value = value;
    return box;
  }

  get value(): T {
    const v = this.#value;
    if (v.length === 0) {
      throw new SubFunLogicError("Box is not initialized");
    }
    return v[0];
  }

  set value(v: T) {
    this.#value[0] = v;
  }

  get isSet(): boolean {
    return this.#value.length !== 0;
  }

  clear() {
    this.#value.length = 0;
  }
}
