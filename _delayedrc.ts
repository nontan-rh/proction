import { LogicError } from "./_error.ts";

/**
 * An internal class to manage a reference counter that delays the initialization of the held object.
 * @typeparam T The type of the held object.
 */
export class DelayedRc<T> {
  #initialized: boolean;
  #managedObject?: T;
  #count: number;
  #destroy: (x: T) => void;
  #reportError: (e: unknown) => void;

  /**
   * Creates a DelayedRc. This does not initialize the held object yet.
   * @param destroy A function to destroy the held object.
   * @param reportError A function to report an error.
   */
  constructor(
    destroy: (x: T) => void,
    reportError: (e: unknown) => void,
  ) {
    this.#initialized = false;
    this.#managedObject = undefined;
    this.#count = 1;
    this.#destroy = destroy;
    this.#reportError = reportError;
  }

  /**
   * Initializes the held object.
   * @param managedObject The object to hold.
   */
  initialize(managedObject: T) {
    this.#assertInitializable();

    this.#initialized = true;
    this.#managedObject = managedObject;
  }

  /**
   * Gets the held object.
   * @returns The held object.
   */
  get managedObject(): T {
    this.#assertIsValid();

    return this.#managedObject!;
  }

  /**
   * Increments the reference count.
   */
  incRef(): void {
    this.#assertNotFreed();

    this.#count++;
  }

  /**
   * Decrements the reference count.
   * If the reference count reaches 0 and it holds a value, the held object is destroyed.
   * If some exception is thrown during the destruction, it is reported to the reportError function
   * and the exception is not rethrown.
   */
  decRef(): void {
    this.#assertNotFreed();

    this.#count--;
    if (this.#initialized && this.#count <= 0) {
      this.#destroyManagedObject();
    }
  }

  /**
   * Checks if the reference counter is freed.
   * @returns True if the reference counter is freed, false otherwise.
   */
  get isFreed(): boolean {
    return this.#count <= 0;
  }

  /**
   * Checks if the reference counter equals to one.
   * @returns True if the reference counter equals to one, false otherwise.
   */
  get hasOneRef(): boolean {
    return this.#count === 1;
  }

  /**
   * Extracts ownership of the managed object without triggering destruction.
   * After this call, the container is invalidated.
   * @returns The managed object.
   */
  extract(): T {
    this.#assertHasOneRef();

    const obj = this.#managedObject!;
    this.#managedObject = undefined;
    this.#count = 0;
    return obj;
  }

  /**
   * Clean up the managed object forcibly.
   * It frees the content and marks the container no longer usable regardless of the current state
   * i.e. it sets the initialized flag and count to 0.
   */
  forceCleanUp(): void {
    if (this.#count >= 1 && this.#initialized) {
      this.#destroyManagedObject();
    }

    this.#initialized = true;
    this.#count = 0;
  }

  #destroyManagedObject(): void {
    try {
      this.#destroy(this.#managedObject!);
    } catch (e: unknown) {
      try {
        this.#reportError(e);
      } catch {
        // cannot recover
      }
    } finally {
      this.#managedObject = undefined;
    }
  }

  #assertInitializable(): void {
    if (this.#initialized) {
      throw new LogicError("this reference counter is already initialized");
    }
    if (this.#count <= 0) {
      throw new LogicError("this reference counter is already freed");
    }
  }

  #assertNotFreed(): void {
    if (this.isFreed) {
      throw new LogicError("this reference counter is already freed");
    }
  }

  #assertHasOneRef(): void {
    if (!this.#initialized) {
      throw new LogicError("this reference counter is not initialized yet");
    }
    if (!this.hasOneRef) {
      throw new LogicError("this reference counter is not one");
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
