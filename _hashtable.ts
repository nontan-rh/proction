/**
 * A hash table that supports structural keys.
 *
 * Unlike ECMAScript's `Map`, which uses reference identity for object keys,
 * this table delegates key equality and hashing to user-provided functions.
 *
 * Collisions are handled by chaining.
 */
export class HashTable<K, V> {
  #table: Map<number, [K, V][]> = new Map();
  #hash: (key: K) => number;
  #equals: (x: K, y: K) => boolean;

  /**
   * Creates a hash table with custom hashing and equality, enabling structural keys.
   */
  constructor(hash: (key: K) => number, equals: (x: K, y: K) => boolean) {
    this.#hash = hash;
    this.#equals = equals;
  }

  /**
   * Gets the value associated with a key equal to `key`.
   */
  get(key: K): V | undefined {
    const hash = (this.#hash)(key);
    const assoc = this.#table.get(hash);
    if (assoc == null) {
      return;
    }

    for (let i = 0; i < assoc.length; i++) {
      if ((this.#equals)(key, assoc[i][0])) {
        return assoc[i][1];
      }
    }
  }

  /**
   * Inserts or updates the entry for a key equal to `key`.
   */
  set(key: K, value: V): void {
    const hash = (this.#hash)(key);
    let assoc = this.#table.get(hash);
    if (assoc == null) {
      assoc = [];
      this.#table.set(hash, assoc);
    }

    for (let i = 0; i < assoc.length; i++) {
      if ((this.#equals)(key, assoc[i][0])) {
        assoc[i][1] = value;
        return;
      }
    }

    assoc.push([key, value]);
  }

  /**
   * Deletes the entry for a key equal to `key`.
   */
  delete(key: K): boolean {
    const hash = (this.#hash)(key);
    const assoc = this.#table.get(hash);
    if (assoc == null) {
      return false;
    }

    if (assoc.length === 1 && (this.#equals)(key, assoc[0][0])) {
      this.#table.delete(hash);
      return true;
    }

    for (let i = 0; i < assoc.length; i++) {
      if ((this.#equals)(key, assoc[i][0])) {
        assoc[i] = assoc[assoc.length - 1];
        assoc.pop();
        return true;
      }
    }

    return false;
  }
}
