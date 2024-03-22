export interface Provider<T> {
  acquire(): T;
  release(x: T): void;
}
