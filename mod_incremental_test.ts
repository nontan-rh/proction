import {
  assert,
  assertEquals,
  assertFalse,
  assertGreaterOrEqual,
  assertNotEquals,
  assertRejects,
} from "@std/assert";
import {
  Context,
  type ContextOptions,
  proc,
  procI,
  procN,
  type ProvideFn,
  provider,
  run,
  type SetVersionFn,
  toFunc,
  type Version,
} from "./mod.ts";
import { Pool } from "./_testutils/pool.ts";
import { Box } from "./_testutils/box.ts";

const contextOptions: Partial<ContextOptions> = {
  reportError: console.error,
  assertNoLeak: true,
};

type TestPool<T, Args extends readonly unknown[]> = {
  provide: ProvideFn<T, Args>;
  assertNoError(): void;
};

function createTestPool<T, Args extends readonly unknown[]>(
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

function createBoxedNumberTestPool(): TestPool<Box<number>, []> {
  return createTestPool(() => new Box<number>(), (x) => x.clear());
}

type VersionTracker = {
  version: Version | undefined;
  calls: number;
  setVersion: SetVersionFn;
};

function createVersionTracker(): VersionTracker {
  const tracker: VersionTracker = {
    version: undefined,
    calls: 0,
    setVersion: (version) => {
      tracker.version = version;
      tracker.calls++;
    },
  };
  return tracker;
}

function createCountingAdd(testPool: TestPool<Box<number>, []>) {
  let count = 0;

  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      count++;
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());

  return { add, pureAdd, getCount: () => count };
}

const v = (x: number) => x as Version;

Deno.test(async function identicalRunSkipsEverything() {
  const testPool = createBoxedNumberTestPool();
  const { add, pureAdd, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c = Box.withValue(5);
  const out = new Box<number>();
  const tracker = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const s = pureAdd($s(a, v(1)), $s(b, v(1)));
      add($d(out, tracker.version, tracker.setVersion), s, $s(c, v(1)));
    });

  await doRun();
  assertEquals(out.value, 8);
  assertEquals(getCount(), 2);
  assertEquals(tracker.calls, 1);
  const firstVersion = tracker.version;
  assert(firstVersion != null);
  testPool.assertNoError();

  await doRun();
  assertEquals(out.value, 8);
  assertEquals(getCount(), 2);
  // setVersion still fires on a skipped run, with a stable version.
  assertEquals(tracker.calls, 2);
  assertEquals(tracker.version, firstVersion);
  testPool.assertNoError();
});

Deno.test(async function sourceVersionBumpRerunsOnlyAffectedChain() {
  const testPool = createBoxedNumberTestPool();
  const chain1 = createCountingAdd(testPool);
  const chain2 = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a1 = Box.withValue(1);
  const b1 = Box.withValue(2);
  const c1 = Box.withValue(3);
  const a2 = Box.withValue(10);
  const b2 = Box.withValue(20);
  const c2 = Box.withValue(30);
  const out1 = new Box<number>();
  const out2 = new Box<number>();
  const tracker1 = createVersionTracker();
  const tracker2 = createVersionTracker();
  let a1Version = 1;

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const s1 = chain1.pureAdd($s(a1, v(a1Version)), $s(b1, v(1)));
      chain1.add(
        $d(out1, tracker1.version, tracker1.setVersion),
        s1,
        $s(c1, v(1)),
      );

      const s2 = chain2.pureAdd($s(a2, v(1)), $s(b2, v(1)));
      chain2.add(
        $d(out2, tracker2.version, tracker2.setVersion),
        s2,
        $s(c2, v(1)),
      );
    });

  await doRun();
  assertEquals(out1.value, 6);
  assertEquals(out2.value, 60);
  assertEquals(chain1.getCount(), 2);
  assertEquals(chain2.getCount(), 2);
  testPool.assertNoError();

  a1.value = 100;
  a1Version = 2;
  await doRun();
  assertEquals(out1.value, 105);
  assertEquals(out2.value, 60);
  assertEquals(chain1.getCount(), 4);
  assertEquals(chain2.getCount(), 2);
  testPool.assertNoError();
});

Deno.test(async function unversionedSourcesAlwaysRerun() {
  const testPool = createBoxedNumberTestPool();
  const { add, pureAdd, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c = Box.withValue(5);
  const out = new Box<number>();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const s = pureAdd($s(a), $s(b));
      add($d(out), s, $s(c));
    });

  await doRun();
  assertEquals(getCount(), 2);
  await doRun();
  assertEquals(getCount(), 4);
  assertEquals(out.value, 8);
  testPool.assertNoError();
});

Deno.test(async function unversionedDestinationAlwaysReruns() {
  const testPool = createBoxedNumberTestPool();
  const { add, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out = new Box<number>();
  const tracker = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      add($d(out, undefined, tracker.setVersion), $s(a, v(1)), $s(b, v(1)));
    });

  await doRun();
  assertEquals(getCount(), 1);
  // The destination version is not round-tripped, so the invocation cannot
  // be skipped even though the sources are unchanged.
  await doRun();
  assertEquals(getCount(), 2);
  assertEquals(out.value, 3);
  // setVersion still reports the version of the written content.
  assertEquals(tracker.calls, 2);
  assertNotEquals(tracker.version, undefined);
  testPool.assertNoError();
});

Deno.test(async function sharedIntermediateWithMixedConsumers() {
  const testPool = createBoxedNumberTestPool();
  const producer = createCountingAdd(testPool);
  const consumer1 = createCountingAdd(testPool);
  const consumer2 = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c1 = Box.withValue(10);
  const c2 = Box.withValue(20);
  const out1 = new Box<number>();
  const out2 = new Box<number>();
  const tracker1 = createVersionTracker();
  const tracker2 = createVersionTracker();
  let c2Version = 1;

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const s = producer.pureAdd($s(a, v(1)), $s(b, v(1)));
      consumer1.add(
        $d(out1, tracker1.version, tracker1.setVersion),
        s,
        $s(c1, v(1)),
      );
      consumer2.add(
        $d(out2, tracker2.version, tracker2.setVersion),
        s,
        $s(c2, v(c2Version)),
      );
    });

  await doRun();
  assertEquals(producer.getCount(), 1);
  assertEquals(consumer1.getCount(), 1);
  assertEquals(consumer2.getCount(), 1);
  testPool.assertNoError();

  c2.value = 200;
  c2Version = 2;
  const tracker1VersionBefore = tracker1.version;
  await doRun();
  // The producer is unchanged but must re-run to feed the changed consumer;
  // the unchanged consumer is still skipped.
  assertEquals(producer.getCount(), 2);
  assertEquals(consumer1.getCount(), 1);
  assertEquals(consumer2.getCount(), 2);
  assertEquals(out1.value, 13);
  assertEquals(out2.value, 203);
  // The skipped consumer reports a stable version.
  assertEquals(tracker1.version, tracker1VersionBefore);
  testPool.assertNoError();
});

Deno.test(async function procNWithMixedDestinations() {
  const testPool = createBoxedNumberTestPool();
  let count = 0;
  const split = procN(
    function splitBody(
      [double, triple]: [Box<number>, Box<number>],
      x: Box<number>,
    ) {
      count++;
      double.value = x.value * 2;
      triple.value = x.value * 3;
    },
  );

  const ctx = new Context(contextOptions);
  const x = Box.withValue(5);
  const outDouble = new Box<number>();
  const outTriple = new Box<number>();
  const trackerDouble = createVersionTracker();
  const trackerTriple = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      split(
        [
          $d(outDouble, trackerDouble.version, trackerDouble.setVersion),
          $d(outTriple, trackerTriple.version, trackerTriple.setVersion),
        ],
        $s(x, v(1)),
      );
    });

  await doRun();
  assertEquals(count, 1);
  await doRun();
  assertEquals(count, 1);
  testPool.assertNoError();

  // One destination is externally overwritten: its version claim no longer
  // matches, so the invocation re-runs and rewrites both destinations.
  outTriple.value = -1;
  trackerTriple.version = v(0);
  const doubleVersionBefore = trackerDouble.version;
  await doRun();
  assertEquals(count, 2);
  assertEquals(outDouble.value, 10);
  assertEquals(outTriple.value, 15);
  // The inputs are unchanged, so the re-run reproduces the recorded content
  // and the recorded versions are kept: the sibling destination is not
  // invalidated spuriously.
  assertEquals(trackerDouble.version, doubleVersionBefore);
  assertEquals(trackerTriple.version, doubleVersionBefore);
  assertNotEquals(trackerTriple.version, v(0));
  testPool.assertNoError();

  // With the reported versions round-tripped, the resubmission is skipped.
  await doRun();
  assertEquals(count, 2);
  testPool.assertNoError();
});

Deno.test(async function inPlaceInteractsWithPruning() {
  const testPool = createBoxedNumberTestPool();
  const producer = createCountingAdd(testPool);
  const sibling = createCountingAdd(testPool);
  const final = createCountingAdd(testPool);

  const variantsUsed: string[] = [];
  const double = procI(
    function doubleOutOfPlace(output: Box<number>, input0: Box<number>) {
      variantsUsed.push("out-of-place");
      output.value = input0.value * 2;
    },
    function doubleInPlace(inout: Box<number>) {
      variantsUsed.push("in-place");
      inout.value = inout.value * 2;
    },
  );
  const pureDouble = toFunc(double, () => testPool.provide());

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c = Box.withValue(10);
  const d = Box.withValue(20);
  const out1 = new Box<number>();
  const out2 = new Box<number>();
  const tracker1 = createVersionTracker();
  const tracker2 = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const s = producer.pureAdd($s(a, v(1)), $s(b, v(1)));
      const doubled = pureDouble(s);
      final.add(
        $d(out1, tracker1.version, tracker1.setVersion),
        doubled,
        $s(c, v(1)),
      );
      sibling.add(
        $d(out2, tracker2.version, tracker2.setVersion),
        s,
        $s(d, v(1)),
      );
    });

  await doRun();
  // The intermediate has two consumers, so the in-place variant is not used.
  assertEquals(variantsUsed, ["out-of-place"]);
  assertEquals(out1.value, 16);
  assertEquals(out2.value, 23);
  testPool.assertNoError();

  await doRun();
  // Everything is up-to-date.
  assertEquals(variantsUsed, ["out-of-place"]);
  assertEquals(producer.getCount(), 1);
  assertEquals(sibling.getCount(), 1);
  assertEquals(final.getCount(), 1);
  testPool.assertNoError();

  // Invalidate out1 only. The sibling consumer is skipped, so the surviving
  // consumer becomes the single consumer of the intermediate and the
  // in-place variant is chosen.
  out1.value = -1;
  tracker1.version = v(0);
  await doRun();
  assertEquals(variantsUsed, ["out-of-place", "in-place"]);
  assertEquals(producer.getCount(), 2);
  assertEquals(sibling.getCount(), 1);
  assertEquals(final.getCount(), 2);
  assertEquals(out1.value, 16);
  testPool.assertNoError();
});

Deno.test(async function errorMidRunLeavesGraphConsistent() {
  const testPool = createBoxedNumberTestPool();
  let count = 0;
  let shouldThrow = false;
  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      count++;
      if (shouldThrow) {
        throw new Error("failure requested");
      }
      result.value = l.value + r.value;
    },
  );

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out = new Box<number>();
  const tracker = createVersionTracker();
  let aVersion = 1;

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      add(
        $d(out, tracker.version, tracker.setVersion),
        $s(a, v(aVersion)),
        $s(b, v(1)),
      );
    });

  await doRun();
  assertEquals(count, 1);
  assertEquals(tracker.calls, 1);
  const versionBeforeFailure = tracker.version;
  testPool.assertNoError();

  a.value = 100;
  aVersion = 2;
  shouldThrow = true;
  await assertRejects(doRun, Error, "failure requested");
  assertEquals(count, 2);
  // A failed run does not report versions.
  assertEquals(tracker.calls, 1);
  assertEquals(tracker.version, versionBeforeFailure);
  testPool.assertNoError();

  // The same submission re-executes after the failure and succeeds.
  shouldThrow = false;
  await doRun();
  assertEquals(count, 3);
  assertEquals(out.value, 102);
  assertEquals(tracker.calls, 2);
  assertNotEquals(tracker.version, versionBeforeFailure);
  testPool.assertNoError();

  // And the run after that is skipped again.
  await doRun();
  assertEquals(count, 3);
  testPool.assertNoError();
});

Deno.test(async function swappedDestinationReruns() {
  const testPool = createBoxedNumberTestPool();
  const { add, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out1 = new Box<number>();
  const out2 = new Box<number>();
  const tracker1 = createVersionTracker();
  const tracker2 = createVersionTracker();

  const doRun = (out: Box<number>, tracker: VersionTracker) =>
    run(ctx, ({ $s, $d }) => {
      add(
        $d(out, tracker.version, tracker.setVersion),
        $s(a, v(1)),
        $s(b, v(1)),
      );
    });

  await doRun(out1, tracker1);
  assertEquals(getCount(), 1);

  // A different destination object is a different data node, so the
  // invocation runs even though the sources are unchanged.
  await doRun(out2, tracker2);
  assertEquals(getCount(), 2);
  assertEquals(out2.value, 3);

  await doRun(out1, tracker1);
  assertEquals(getCount(), 2);
  await doRun(out2, tracker2);
  assertEquals(getCount(), 2);
  testPool.assertNoError();
});

Deno.test(async function zeroConsumerInvocationIsSkipped() {
  const testPool = createBoxedNumberTestPool();
  const { pureAdd, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);

  const doRun = () =>
    run(ctx, ({ $s }) => {
      pureAdd($s(a, v(1)), $s(b, v(1)));
    });

  await doRun();
  assertEquals(getCount(), 1);
  testPool.assertNoError();

  await doRun();
  assertEquals(getCount(), 1);
  testPool.assertNoError();
});

Deno.test(async function destinationAsInputWiredBeforeProducer() {
  const testPool = createBoxedNumberTestPool();
  const writer = createCountingAdd(testPool);
  const reader = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c = Box.withValue(10);
  const mid = new Box<number>();
  const out = new Box<number>();
  const trackerMid = createVersionTracker();
  const trackerOut = createVersionTracker();
  let aVersion = 1;

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      const midHandle = $d(mid, trackerMid.version, trackerMid.setVersion);
      // The consumer of the destination is wired before its producer.
      reader.add(
        $d(out, trackerOut.version, trackerOut.setVersion),
        midHandle,
        $s(c, v(1)),
      );
      writer.add(midHandle, $s(a, v(aVersion)), $s(b, v(1)));
    });

  await doRun();
  assertEquals(mid.value, 3);
  assertEquals(out.value, 13);
  assertEquals(writer.getCount(), 1);
  assertEquals(reader.getCount(), 1);
  testPool.assertNoError();

  await doRun();
  assertEquals(writer.getCount(), 1);
  assertEquals(reader.getCount(), 1);
  testPool.assertNoError();

  a.value = 100;
  aVersion = 2;
  await doRun();
  assertEquals(mid.value, 102);
  assertEquals(out.value, 112);
  assertEquals(writer.getCount(), 2);
  assertEquals(reader.getCount(), 2);
  testPool.assertNoError();
});

Deno.test(async function conflictingSourceVersionsThrow() {
  const testPool = createBoxedNumberTestPool();
  const { add } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);

  await assertRejects(
    () =>
      run(ctx, ({ $s, $d }) => {
        add($d(new Box<number>()), $s(a, 1), $s(b, 1));
        add($d(new Box<number>()), $s(a, 2), $s(b, 1));
      }),
    Error,
    "different version",
  );
  testPool.assertNoError();
});

Deno.test(async function unversionedSourceClaimWinsOverVersioned() {
  // A versioned $s mixed with an unversioned $s on the same object falls
  // back to unversioned: the consumer re-runs every time instead of trusting
  // the stale claim.
  const testPool = createBoxedNumberTestPool();
  const { add, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const out = new Box<number>();
  const tracker = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      add($d(out, tracker.version, tracker.setVersion), $s(a, 1), $s(a));
    });

  await doRun();
  assertEquals(getCount(), 1);
  await doRun();
  assertEquals(getCount(), 2);
  assertEquals(out.value, 2);
  testPool.assertNoError();
});

Deno.test(async function repeatedDestinationCallbacksAllFire() {
  const testPool = createBoxedNumberTestPool();
  const { add } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out = new Box<number>();
  const tracker1 = createVersionTracker();
  const tracker2 = createVersionTracker();

  await run(ctx, ({ $s, $d }) => {
    const handle = $d(out, undefined, tracker1.setVersion);
    $d(out, undefined, tracker2.setVersion);
    add(handle, $s(a, 1), $s(b, 1));
  });

  assertEquals(out.value, 3);
  assertEquals(tracker1.calls, 1);
  assertEquals(tracker2.calls, 1);
  assertEquals(tracker1.version, tracker2.version);
  testPool.assertNoError();
});

Deno.test(async function invalidVersionsAreRejected() {
  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);

  for (const version of [NaN, 0.5, Infinity, -1]) {
    await assertRejects(
      () =>
        run(ctx, ({ $s }) => {
          $s(a, version);
        }),
      Error,
      "non-negative integer",
    );
    await assertRejects(
      () =>
        run(ctx, ({ $d }) => {
          $d(a, version as Version);
        }),
      Error,
      "non-negative integer",
    );
  }
});

Deno.test(async function overlappingVersionedRunsAreRejected() {
  const testPool = createBoxedNumberTestPool();
  const { add, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out = new Box<number>();
  const tracker = createVersionTracker();

  const doRun = () =>
    run(ctx, ({ $s, $d }) => {
      add($d(out, tracker.version, tracker.setVersion), $s(a, 1), $s(b, 1));
    });

  const inFlight = doRun();
  await assertRejects(doRun, Error, "must not overlap");
  await inFlight;
  assertEquals(getCount(), 1);

  // After the in-flight run finished, versioned runs are accepted again.
  await doRun();
  assertEquals(getCount(), 1);
  testPool.assertNoError();
});

Deno.test(async function failedRunIsNotSkippedOnResubmission() {
  const testPool = createBoxedNumberTestPool();
  let pCount = 0;
  let cCount = 0;
  const p = proc(function pBody(out: Box<number>, x: Box<number>) {
    pCount++;
    out.value = x.value + 1;
  });
  const pureP = toFunc(p, () => testPool.provide());
  const c = proc(function cBody(_out: Box<number>, _x: Box<number>) {
    cCount++;
    throw new Error("deterministic failure");
  });
  const pureC = toFunc(c, () => testPool.provide());

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);

  const doRun = () =>
    run(ctx, ({ $s }) => {
      pureC(pureP($s(a, 1)));
    });

  // The failure leaves no record of success: an identical resubmission
  // re-executes and reports the failure again instead of being pruned.
  await assertRejects(doRun, Error, "deterministic failure");
  assertEquals(pCount, 1);
  assertEquals(cCount, 1);
  await assertRejects(doRun, Error, "deterministic failure");
  assertEquals(pCount, 2);
  assertEquals(cCount, 2);
  testPool.assertNoError();
});

Deno.test(async function swappedProvideReruns() {
  const testPool = createBoxedNumberTestPool();
  let count = 0;
  const double = proc(function doubleBody(out: Box<number>, x: Box<number>) {
    count++;
    out.value = x.value * 2;
  });
  const pureDoubleA = toFunc(double, () => testPool.provide());
  const pureDoubleB = toFunc(double, () => testPool.provide());
  const consumer = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(3);
  const c = Box.withValue(10);
  const out = new Box<number>();
  const tracker = createVersionTracker();

  const doRun = (pureDouble: typeof pureDoubleA) =>
    run(ctx, ({ $s, $d }) => {
      const s = pureDouble($s(a, 1));
      consumer.add(
        $d(out, tracker.version, tracker.setVersion),
        s,
        $s(c, 1),
      );
    });

  await doRun(pureDoubleA);
  assertEquals(count, 1);
  await doRun(pureDoubleA);
  assertEquals(count, 1);

  // A func wrapping the same proc with a different provide is a different
  // computation and must not be skipped.
  await doRun(pureDoubleB);
  assertEquals(count, 2);
  assertEquals(consumer.getCount(), 2);
  assertEquals(out.value, 16);
  testPool.assertNoError();
});

Deno.test(async function throwingSetVersionDoesNotFailRun() {
  const testPool = createBoxedNumberTestPool();
  const writer1 = createCountingAdd(testPool);
  const writer2 = createCountingAdd(testPool);

  const reported: unknown[] = [];
  const ctx = new Context({
    reportError: (e) => reported.push(e),
    assertNoLeak: true,
  });
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const out1 = new Box<number>();
  const out2 = new Box<number>();
  const tracker2 = createVersionTracker();

  await run(ctx, ({ $s, $d }) => {
    writer1.add(
      $d(out1, undefined, () => {
        throw new Error("callback failure");
      }),
      $s(a, 1),
      $s(b, 1),
    );
    writer2.add($d(out2, undefined, tracker2.setVersion), $s(a, 1), $s(b, 1));
  });

  // The run succeeds, the remaining destination is still notified, and the
  // callback error is reported.
  assertEquals(out1.value, 3);
  assertEquals(out2.value, 3);
  assertEquals(tracker2.calls, 1);
  assertEquals(reported.length, 1);
  testPool.assertNoError();
});

Deno.test(async function duplicateWiringExecutesOnFirstRun() {
  const testPool = createBoxedNumberTestPool();
  const { pureAdd, getCount } = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);

  const doRun = () =>
    run(ctx, ({ $s }) => {
      pureAdd($s(a, 1), $s(b, 1));
      pureAdd($s(a, 1), $s(b, 1));
    });

  // Identically-wired siblings both execute on their first submission.
  await doRun();
  assertEquals(getCount(), 2);
  testPool.assertNoError();

  // On resubmission both are up-to-date and skipped.
  await doRun();
  assertEquals(getCount(), 2);
  testPool.assertNoError();
});

Deno.test(async function sourceVersionCannotCollideWithGeneratedVersion() {
  const testPool = createBoxedNumberTestPool();
  const writer = createCountingAdd(testPool);
  const reader = createCountingAdd(testPool);

  const ctx = new Context(contextOptions);
  const a = Box.withValue(1);
  const b = Box.withValue(2);
  const c = Box.withValue(10);
  const mid = new Box<number>();
  const out = new Box<number>();
  const trackerOut = createVersionTracker();

  // Run 1: mid is produced by the plan, so the reader's record stores a
  // generated version for it.
  await run(ctx, ({ $s, $d }) => {
    const midHandle = $d(mid);
    writer.add(midHandle, $s(a, 1), $s(b, 1));
    reader.add(
      $d(out, trackerOut.version, trackerOut.setVersion),
      midHandle,
      $s(c, 1),
    );
  });
  assertEquals(out.value, 13);
  assertEquals(reader.getCount(), 1);

  // Run 2: mid is mutated directly and passed as a source with a
  // caller-managed version whose number equals the generated one. The
  // namespaces are disjoint, so the reader must re-run.
  mid.value = 100;
  await run(ctx, ({ $s, $d }) => {
    reader.add(
      $d(out, trackerOut.version, trackerOut.setVersion),
      $s(mid, 2),
      $s(c, 1),
    );
  });
  assertEquals(reader.getCount(), 2);
  assertEquals(out.value, 110);
  testPool.assertNoError();
});
