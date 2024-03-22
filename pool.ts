import { Provider } from "./provider.ts";

export class Pool<T> implements Provider<T> {
  #create: () => T;
  #cleanup: (x: T) => void;
  #reportError: (e: unknown) => void;
  #pooledCells: { body: T }[] = [];
  #vacantCells: { body: T }[] = [];
  #taintedCells: { body: T }[] = [];

  constructor(
    create: () => T,
    cleanup: (x: T) => void,
    errorReport: (e: unknown) => void,
  ) {
    this.#create = create;
    this.#cleanup = cleanup;
    this.#reportError = errorReport;
  }

  acquire(): T {
    const pooledCell = this.#pooledCells.pop();
    if (pooledCell == null) {
      return this.#create();
    }

    const body = pooledCell.body;
    this.#releaseCell(pooledCell);
    return body;
  }

  release(x: T): void {
    try {
      this.#cleanup(x);
    } catch (e: unknown) {
      try {
        this.#reportError(e);
      } catch {
        // cannot recover
      }

      const taintedCell = this.#acquireCell(x);
      this.#taintedCells.push(taintedCell);
      return;
    }

    const pooledCell = this.#acquireCell(x);
    this.#pooledCells.push(pooledCell);
  }

  #acquireCell(x: T) {
    let vacantCell = this.#vacantCells.pop();
    if (vacantCell == null) {
      vacantCell = { body: x };
    } else {
      vacantCell.body = x;
    }
    return vacantCell;
  }

  #releaseCell(cell: { body: T }) {
    this.#vacantCells.push(cell);
  }
}
