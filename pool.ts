import { Provider } from "./provider.ts";

export class Pool<T, Args extends readonly unknown[]> implements Provider<T, Args> {
  #create: (...args: Args) => T;
  #cleanup: (x: T) => void;
  #reportError: (e: unknown) => void;
  #pooledCells: { body: T }[] = [];
  #vacantCells: { body: T }[] = [];
  #taintedCells: { body: T }[] = [];
  #pooledCount = 0;
  #acquiredCount = 0;
  #taintedCount = 0;

  constructor(
    create: (...args: Args) => T,
    cleanup: (x: T) => void,
    reportError: (e: unknown) => void,
  ) {
    this.#create = create;
    this.#cleanup = cleanup;
    this.#reportError = reportError;
  }

  get pooledCount(): number {
    return this.#pooledCount;
  }

  get acquiredCount(): number {
    return this.#acquiredCount;
  }

  get taintedCount(): number {
    return this.#taintedCount;
  }

  acquire(...args: Args): T {
    const pooledCell = this.#pooledCells.pop();
    if (pooledCell == null) {
      const body = this.#create(...args);
      this.#acquiredCount++;
      return body;
    }

    const body = pooledCell.body;
    this.#releaseCell(pooledCell);
    this.#pooledCount--;
    this.#acquiredCount++;
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
      this.#acquiredCount--;
      this.#taintedCount++;
      return;
    }

    const pooledCell = this.#acquireCell(x);
    this.#pooledCells.push(pooledCell);
    this.#acquiredCount--;
    this.#pooledCount++;
  }

  #acquireCell(x: T): { body: T } {
    let vacantCell = this.#vacantCells.pop();
    if (vacantCell == null) {
      vacantCell = { body: x };
    } else {
      vacantCell.body = x;
    }
    return vacantCell;
  }

  #releaseCell(cell: { body: T }): void {
    this.#vacantCells.push(cell);
  }
}
