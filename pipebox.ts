import { Box } from "./box.ts";

export function pipeBox<T>(): [IPipeBoxR<T>, IPipeBoxW<T>] {
  const box = new Box<T>();
  return [new PipeBoxR(box), new PipeBoxW(box)];
}

export function pipeBoxR<T>(value: T): IPipeBoxR<T> {
  const box = new Box<T>();
  box.value = value;
  return new PipeBoxR(box);
}

export function pipeBoxRW<T>(): IPipeBoxRW<T> {
  const box = new Box<T>();
  return new PipeBoxRW(box);
}

export interface IPipeBoxW<T> {
  setValue(v: T): void;
  clear(): void;
}

export interface IPipeBoxR<T> {
  getValue(): T;
}

export interface IPipeBoxRW<T> extends IPipeBoxR<T>, IPipeBoxW<T> {}

class PipeBoxW<T> implements IPipeBoxW<T> {
  #box: Box<T>;

  constructor(box: Box<T>) {
    this.#box = box;
  }

  setValue(v: T): void {
    this.#box.value = v;
  }

  clear(): void {
    this.#box.clear();
  }
}

class PipeBoxR<T> implements IPipeBoxR<T> {
  #box: Box<T>;

  constructor(box: Box<T>) {
    this.#box = box;
  }

  getValue(): T {
    return this.#box.value;
  }
}

class PipeBoxRW<T> implements IPipeBoxRW<T> {
  #box: Box<T>;

  constructor(box: Box<T>) {
    this.#box = box;
  }

  getValue(): T {
    return this.#box.value;
  }

  setValue(v: T): void {
    this.#box.value = v;
  }

  clear(): void {
    this.#box.clear();
  }
}
