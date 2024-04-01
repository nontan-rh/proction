import { Box } from "./box.ts";

export function pipeBox<T>(): [PipeBoxReader<T>, PipeBoxWriter<T>] {
  const box = new Box<T>();
  return [new PipeBoxReader(box), new PipeBoxWriter(box)];
}

export class PipeBoxWriter<T> {
  #box: Box<T>;

  constructor(box: Box<T>) {
    this.#box = box;
  }

  set value(v: T) {
    this.#box.value = v;
  }
}

export class PipeBoxReader<T> {
  #box: Box<T>;

  constructor(box: Box<T>) {
    this.#box = box;
  }

  get value(): T {
    return this.#box.value;
  }
}
