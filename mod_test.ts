import {
  assertEquals,
  assertFalse,
  assertGreaterOrEqual,
  assertThrows,
} from "@std/assert";
import { delay } from "@std/async";
import {
  Context,
  ContextOptions,
  DisposableWrap,
  Handle,
  proction,
  proctionN,
  Provide,
  provider,
  purify,
  purifyN,
} from "./mod.ts";
import { Pool } from "./_testutils/pool.ts";
import { Box } from "./_testutils/box.ts";
import {
  IPipeBoxR,
  IPipeBoxRW,
  IPipeBoxW,
  pipeBox,
  pipeBoxR,
  pipeBoxRW,
} from "./_testutils/pipebox.ts";
import { assertIsChildTypeOf, testValue } from "./_testutils/types.ts";

const contextOptions: Partial<ContextOptions> = {
  reportError: console.error,
  assertNoLeak: true,
};

type TestPool<T, Args extends readonly unknown[]> = {
  provide: Provide<T, Args>;
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
  const provide = provider(pool.acquire, pool.release);

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

function createNumberArrayTestPool(): TestPool<number[], [number]> {
  return createTestPool((l) => new Array(l).fill(0), (x) => x.fill(0));
}

function createNumberPipeBoxTestPool(): TestPool<IPipeBoxRW<number>, []> {
  return createTestPool(() => pipeBoxRW<number>(), (x) => x.clear());
}

Deno.test(async function calc() {
  const testPool = createBoxedNumberTestPool();

  const add = proction()(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = purify(add, () => testPool.provide());
  const mul = proction()(
    function mulBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value * r.value;
    },
  );
  const pureMul = purify(mul, () => testPool.provide());

  const resultBody = new Box<number>();

  await new Context(contextOptions).run(({ source, sink }) => {
    const result = sink(resultBody);
    const result1 = pureAdd(
      source(Box.withValue(1)),
      source(Box.withValue(2)),
    );
    const result2 = pureAdd(
      source(Box.withValue(3)),
      source(Box.withValue(4)),
    );
    const result3 = pureMul(result1, result2);
    add(result, result3, source(Box.withValue(5)));
  });

  assertEquals(resultBody.value, 26);
  testPool.assertNoError();
});

Deno.test(async function parameterizedAllocation() {
  const testPool = createNumberArrayTestPool();

  const add = proction()(
    function addBody(result: number[], l: number[], r: number[]) {
      const minLength = Math.min(result.length, l.length, r.length);
      for (let i = 0; i < minLength; i++) {
        result[i] = l[i] + r[i];
      }
    },
  );
  const pureAdd = purify(
    add,
    (l, r) => testPool.provide(Math.min(l.length, r.length)),
  );

  const resultBody = new Array(5);
  await new Context(contextOptions).run(({ source, sink }) => {
    const result = sink(resultBody);
    const result1 = pureAdd(
      source([1, 2, 3, 4, 5]),
      source([10, 20, 30, 40, 50]),
    );
    add(result, result1, source([100, 200, 300, 400, 500]));
  });

  assertEquals(resultBody, [111, 222, 333, 444, 555]);
  testPool.assertNoError();
});

Deno.test(async function empty() {
  await new Context(contextOptions).run(() => {});
});

Deno.test(async function twoOutputs(t) {
  const testPool = createBoxedNumberTestPool();

  const divmod = proctionN()(
    function divmodBody(
      [div, mod]: [Box<number>, Box<number>],
      l: Box<number>,
      r: Box<number>,
    ) {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const pureDivmod = purifyN(divmod, [
    () => testPool.provide(),
    () => testPool.provide(),
  ]);
  const add = proction()(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );

  await t.step(async function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    await new Context(contextOptions).run(({ source, sink }) => {
      const div = sink(divBody);
      const mod = sink(modBody);

      divmod([div, mod], source(Box.withValue(42)), source(Box.withValue(5)));
    });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    testPool.assertNoError();
  });

  await t.step(async function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    await new Context(contextOptions).run(({ source, sink }) => {
      const result = sink(resultBody);
      const [, mod] = pureDivmod(
        source(Box.withValue(42)),
        source(Box.withValue(5)),
      );
      add(result, mod, source(Box.withValue(100)));
    });

    assertEquals(resultBody.value, 102);
    testPool.assertNoError();
  });
});

Deno.test(async function outputUsage(t) {
  const testPool = createBoxedNumberTestPool();

  const add = proction()(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = purify(add, () => testPool.provide());
  const mul = proction()(
    function mulBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value * r.value;
    },
  );
  const pureMul = purify(mul, () => testPool.provide());

  await t.step(async function noOutputsAreUsed() {
    await new Context(contextOptions).run(({ source }) => {
      const input1 = source(Box.withValue(42));
      const input2 = source(Box.withValue(5));

      pureAdd(input1, input2);
      pureMul(input1, input2);
    });

    testPool.assertNoError();
  });

  await t.step(async function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    await new Context(contextOptions).run(({ source, sink }) => {
      const result1 = sink(result1Body);
      const result2 = sink(result2Body);

      const sum = pureAdd(
        source(Box.withValue(42)),
        source(Box.withValue(2)),
      );
      mul(result1, sum, source(Box.withValue(3)));
      add(result2, sum, source(Box.withValue(4)));
    });

    testPool.assertNoError();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(async function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    await new Context(contextOptions).run(({ source, sink }) => {
      const sum = sink(sumBody);
      const result = sink(resultBody);

      add(sum, source(Box.withValue(42)), source(Box.withValue(2)));
      mul(result, sum, source(Box.withValue(3)));
    });

    testPool.assertNoError();
    assertEquals(sumBody.value, 44);
    assertEquals(resultBody.value, 132);
  });
});

Deno.test(async function calcIO() {
  const testPool = createNumberPipeBoxTestPool();

  const add = proction()(
    function addBody(
      result: IPipeBoxW<number>,
      l: IPipeBoxR<number>,
      r: IPipeBoxR<number>,
    ) {
      result.setValue(l.getValue() + r.getValue());
    },
  );
  const pureAdd = purify(add, () => testPool.provide());
  const mul = proction()(
    function mulBody(
      result: IPipeBoxW<number>,
      l: IPipeBoxR<number>,
      r: IPipeBoxR<number>,
    ) {
      result.setValue(l.getValue() * r.getValue());
    },
  );
  const pureMul = purify(mul, () => testPool.provide());

  const [input1R, input1W] = pipeBox<number>();
  input1W.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [result1R, result1W] = pipeBox<number>();
  const result2RW = pipeBoxRW<number>();
  await new Context(contextOptions).run(({ source, sink }) => {
    const result1Handle = sink(result1W);
    const result2Handle = sink(result2RW);

    const result1 = pureAdd(
      source(input1R),
      source(input2RW),
    );
    const result2 = pureAdd(
      source(pipeBoxR(3)),
      source(pipeBoxR(4)),
    );
    const result3 = pureMul(result1, result2);

    const input5 = source(pipeBoxR(5));
    add(result1Handle, result3, input5);
    mul(result2Handle, result3, input5);
  });

  assertEquals(result1R.getValue(), 26);
  assertEquals(result2RW.getValue(), 105);
  testPool.assertNoError();
});

Deno.test(async function asyncCalc() {
  const testPool = createBoxedNumberTestPool();

  const add = proction()(
    async function addBody(
      result: Box<number>,
      l: Box<number>,
      r: Box<number>,
    ) {
      await delay(Math.min(l.value, r.value));
      result.value = l.value + r.value;
    },
  );
  const pureAdd = purify(add, () => testPool.provide());
  const mul = proction()(
    async function mulBody(
      result: Box<number>,
      l: Box<number>,
      r: Box<number>,
    ) {
      await delay(Math.min(l.value, r.value));
      result.value = l.value * r.value;
    },
  );
  const pureMul = purify(mul, () => testPool.provide());

  const resultBody = new Box<number>();

  await new Context(contextOptions).run(({ source, sink }) => {
    const result = sink(resultBody);
    const result1 = pureAdd(
      source(Box.withValue(1)),
      source(Box.withValue(2)),
    );
    const result2 = pureAdd(
      source(Box.withValue(3)),
      source(Box.withValue(4)),
    );
    const result3 = pureMul(result1, result2);
    add(result, result3, source(Box.withValue(5)));
  });

  assertEquals(resultBody.value, 26);
  testPool.assertNoError();
});

Deno.test(async function middleware(t) {
  const addLog: string[] = [];
  const add = proction({
    middlewares: [async (next) => {
      addLog.push("1 before");
      await next();
      addLog.push("1 after");
    }, async (next) => {
      addLog.push("2 before");
      await next();
      addLog.push("2 after");
    }],
  })(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const divmodLog: string[] = [];
  const divmod = proctionN({
    middlewares: [async (next) => {
      divmodLog.push("1 before");
      await next();
      divmodLog.push("1 after");
    }, async (next) => {
      divmodLog.push("2 before");
      await next();
      divmodLog.push("2 after");
    }],
  })(
    function divmodBody(
      [div, mod]: [Box<number>, Box<number>],
      l: Box<number>,
      r: Box<number>,
    ) {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );

  await t.step(async function single() {
    const output = new Box<number>();
    await new Context(contextOptions).run(({ source, sink }) => {
      const input1 = source(Box.withValue(42));
      const input2 = source(Box.withValue(5));

      add(sink(output), input1, input2);
    });
    assertEquals(output.value, 47);
    assertEquals(addLog, ["1 before", "2 before", "2 after", "1 after"]);
  });

  await t.step(async function multiple() {
    const output1 = new Box<number>();
    const output2 = new Box<number>();
    await new Context(contextOptions).run(({ source, sink }) => {
      const input1 = source(Box.withValue(42));
      const input2 = source(Box.withValue(5));

      divmod([sink(output1), sink(output2)], input1, input2);
    });
    assertEquals(output1.value, 8);
    assertEquals(output2.value, 2);
    assertEquals(divmodLog, ["1 before", "2 before", "2 after", "1 after"]);
  });
});

Deno.test(function types() {
  const so = proction()(
    function soBody(_x: Box<number>, _a: Box<string>, _b: Box<boolean>) {},
  );
  const mo = proctionN()(
    function moBody(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<boolean>,
      _b: Box<bigint>,
    ) {},
  );
  const sop = purify(
    so,
    (_a: Box<string>, _b: Box<boolean>) =>
      testValue<DisposableWrap<Box<number>>>(),
  );
  const mop = purifyN(mo, [
    (_a: Box<boolean>, _b: Box<bigint>) =>
      testValue<DisposableWrap<Box<number>>>(),
    (_a: Box<boolean>, _b: Box<bigint>) =>
      testValue<DisposableWrap<Box<string>>>(),
  ]);

  // testValue throws an error
  assertThrows(() => {
    // so: OK
    so(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );

    // mo: OK
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );

    // sop: OK
    sop(
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    assertIsChildTypeOf<ReturnType<typeof sop>, Handle<Box<number>>>();
    assertIsChildTypeOf<Handle<Box<number>>, ReturnType<typeof sop>>();

    // mop: OK
    mop(
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    assertIsChildTypeOf<ReturnType<typeof mop>[0], Handle<Box<number>>>();
    assertIsChildTypeOf<Handle<Box<number>>, ReturnType<typeof mop>[0]>();
    assertIsChildTypeOf<ReturnType<typeof mop>[1], Handle<Box<string>>>();
    assertIsChildTypeOf<Handle<Box<string>>, ReturnType<typeof mop>[1]>();

    // so: NG
    so(
      // @ts-expect-error: output type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    so(
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: 1st input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    so(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      // @ts-expect-error: 2nd input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient args
    so(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
    );
    so(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: excessive args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
    so(
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: args swapped
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<string>>>(),
    );

    // mo: NG
    mo(
      // @ts-expect-error: 1st output type is wrong
      [testValue<Handle<Box<symbol>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mo(
      // @ts-expect-error: 2st output type is wrong
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<symbol>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      // @ts-expect-error: 1st input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: 2nd input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    mo(
      // @ts-expect-error: insufficient output args
      [testValue<Handle<Box<number>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    // @ts-expect-error: insufficient input args
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<boolean>>>(),
    );
    mo(
      // @ts-expect-error: excessive output args
      [
        testValue<Handle<Box<number>>>(),
        testValue<Handle<Box<string>>>(),
        // deno-lint-ignore no-explicit-any
        testValue<Handle<Box<any>>>(),
      ],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
      // @ts-expect-error: excessive input args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
    mo(
      // @ts-expect-error: output args swapped
      [testValue<Handle<Box<string>>>(), testValue<Handle<Box<number>>>()],
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mo(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      // @ts-expect-error: input args swapped
      testValue<Handle<Box<bigint>>>(),
      testValue<Handle<Box<boolean>>>(),
    );

    // sop: NG
    sop(
      // @ts-expect-error: 1st input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    sop(
      testValue<Handle<Box<string>>>(),
      // @ts-expect-error: 2nd input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient input args
    sop(
      testValue<Handle<Box<string>>>(),
    );
    sop(
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: excessive input args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );

    // mop: NG
    mop(
      // @ts-expect-error: 1st input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    mop(
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: 2nd input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient input args
    mop(
      testValue<Handle<Box<boolean>>>(),
    );
    mop(
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
      // @ts-expect-error: excessive input args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
  });
});
