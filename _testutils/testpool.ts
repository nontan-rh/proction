import { assertEquals, assertFalse, assertGreaterOrEqual } from "@std/assert";
import { type ContextOptions, type ProvideFn, provider } from "../mod.ts";
import { Pool } from "./pool.ts";
import { Box } from "./box.ts";

export const contextOptions: Partial<ContextOptions> = {
  reportError: console.error,
  assertNoLeak: true,
};

export type TestPool<T, Args extends readonly unknown[]> = {
  provide: ProvideFn<T, Args>;
  assertNoError(): void;
};

export function createTestPool<T, Args extends readonly unknown[]>(
  create: (...args: Args) => T,
  cleanup: (x: T) => void,
): TestPool<T, Args> {
  let errorReported = false;

  const pool = new Pool<T, Args>(
    create,
    cleanup,
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const provide = provider(
    (...args: Args) => pool.acquire(...args),
    (x) => pool.release(x),
  );

  return {
    provide,
    assertNoError() {
      assertEquals(pool.acquiredCount, 0);
      assertGreaterOrEqual(pool.pooledCount, 0);
      assertEquals(pool.taintedCount, 0);
      assertFalse(errorReported);
    },
  };
}

export function createBoxedNumberTestPool(): TestPool<Box<number>, []> {
  return createTestPool(() => new Box<number>(), (x) => x.clear());
}
