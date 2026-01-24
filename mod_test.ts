import {
  assertEquals,
  assertFalse,
  assertGreaterOrEqual,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { delay } from "@std/async";
import {
  Context,
  type ContextOptions,
  type DisposableWrap,
  type Handle,
  proc,
  procI,
  procN,
  procNI1,
  procNIAll,
  type ProvideFn,
  provider,
  run,
  toFunc,
  toFuncN,
} from "./mod.ts";
import { Pool } from "./_testutils/pool.ts";
import { Box } from "./_testutils/box.ts";
import {
  type IPipeBoxR,
  type IPipeBoxRW,
  type IPipeBoxW,
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

function createNumberArrayTestPool(): TestPool<number[], [number]> {
  return createTestPool((l) => new Array(l).fill(0), (x) => x.fill(0));
}

function createNumberPipeBoxTestPool(): TestPool<IPipeBoxRW<number>, []> {
  return createTestPool(() => pipeBoxRW<number>(), (x) => x.clear());
}

Deno.test(async function calc() {
  const testPool = createBoxedNumberTestPool();

  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());
  const mul = proc(
    function mulBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value * r.value;
    },
  );
  const pureMul = toFunc(mul, () => testPool.provide());

  const resultBody = new Box<number>();

  await run(new Context(contextOptions), ({ $s, $d }) => {
    const result = $d(resultBody);
    const result1 = pureAdd($s(Box.withValue(1)), $s(Box.withValue(2)));
    const result2 = pureAdd($s(Box.withValue(3)), $s(Box.withValue(4)));
    const result3 = pureMul(result1, result2);
    add(result, result3, $s(Box.withValue(5)));
  });

  assertEquals(resultBody.value, 26);
  testPool.assertNoError();
});

Deno.test(async function parameterizedAllocation() {
  const testPool = createNumberArrayTestPool();

  const add = proc(
    function addBody(result: number[], l: number[], r: number[]) {
      const minLength = Math.min(result.length, l.length, r.length);
      for (let i = 0; i < minLength; i++) {
        result[i] = l[i] + r[i];
      }
    },
  );
  const pureAdd = toFunc(
    add,
    (l, r) => testPool.provide(Math.min(l.length, r.length)),
  );

  const resultBody = new Array(5);
  await run(new Context(contextOptions), ({ $s, $d }) => {
    const result = $d(resultBody);
    const result1 = pureAdd($s([1, 2, 3, 4, 5]), $s([10, 20, 30, 40, 50]));
    add(result, result1, $s([100, 200, 300, 400, 500]));
  });

  assertEquals(resultBody, [111, 222, 333, 444, 555]);
  testPool.assertNoError();
});

Deno.test(async function empty() {
  await run(new Context(contextOptions), () => {});
});

Deno.test(async function twoOutputs(t) {
  const testPool = createBoxedNumberTestPool();

  const divmod = procN(
    function divmodBody(
      [div, mod]: [Box<number>, Box<number>],
      l: Box<number>,
      r: Box<number>,
    ) {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const pureDivmod = toFuncN(divmod, [
    () => testPool.provide(),
    () => testPool.provide(),
  ]);
  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );

  await t.step(async function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const div = $d(divBody);
      const mod = $d(modBody);

      divmod([div, mod], $s(Box.withValue(42)), $s(Box.withValue(5)));
    });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    testPool.assertNoError();
  });

  await t.step(async function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result = $d(resultBody);
      const [, mod] = pureDivmod($s(Box.withValue(42)), $s(Box.withValue(5)));
      add(result, mod, $s(Box.withValue(100)));
    });

    assertEquals(resultBody.value, 102);
    testPool.assertNoError();
  });
});

Deno.test(async function outputUsage(t) {
  const testPool = createBoxedNumberTestPool();

  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());
  const mul = proc(
    function mulBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value * r.value;
    },
  );
  const pureMul = toFunc(mul, () => testPool.provide());

  await t.step(async function noOutputsAreUsed() {
    await run(new Context(contextOptions), ({ $s }) => {
      const input1 = $s(Box.withValue(42));
      const input2 = $s(Box.withValue(5));

      pureAdd(input1, input2);
      pureMul(input1, input2);
    });

    testPool.assertNoError();
  });

  await t.step(async function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result1 = $d(result1Body);
      const result2 = $d(result2Body);

      const sum = pureAdd($s(Box.withValue(42)), $s(Box.withValue(2)));
      mul(result1, sum, $s(Box.withValue(3)));
      add(result2, sum, $s(Box.withValue(4)));
    });

    testPool.assertNoError();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(async function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const sum = $d(sumBody);
      const result = $d(resultBody);

      add(sum, $s(Box.withValue(42)), $s(Box.withValue(2)));
      mul(result, sum, $s(Box.withValue(3)));
    });

    testPool.assertNoError();
    assertEquals(sumBody.value, 44);
    assertEquals(resultBody.value, 132);
  });
});

Deno.test(async function calcIO() {
  const testPool = createNumberPipeBoxTestPool();

  const add = proc(
    function addBody(
      result: IPipeBoxW<number>,
      l: IPipeBoxR<number>,
      r: IPipeBoxR<number>,
    ) {
      result.setValue(l.getValue() + r.getValue());
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());
  const mul = proc(
    function mulBody(
      result: IPipeBoxW<number>,
      l: IPipeBoxR<number>,
      r: IPipeBoxR<number>,
    ) {
      result.setValue(l.getValue() * r.getValue());
    },
  );
  const pureMul = toFunc(mul, () => testPool.provide());

  const [input1R, input1W] = pipeBox<number>();
  input1W.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [result1R, result1W] = pipeBox<number>();
  const result2RW = pipeBoxRW<number>();
  await run(new Context(contextOptions), ({ $s, $d }) => {
    const result1Handle = $d(result1W);
    const result2Handle = $d(result2RW);

    const result1 = pureAdd($s(input1R), $s(input2RW));
    const result2 = pureAdd($s(pipeBoxR(3)), $s(pipeBoxR(4)));
    const result3 = pureMul(result1, result2);

    const input5 = $s(pipeBoxR(5));
    add(result1Handle, result3, input5);
    mul(result2Handle, result3, input5);
  });

  assertEquals(result1R.getValue(), 26);
  assertEquals(result2RW.getValue(), 105);
  testPool.assertNoError();
});

Deno.test(async function asyncCalc() {
  const testPool = createBoxedNumberTestPool();

  const add = proc(
    async function addBody(
      result: Box<number>,
      l: Box<number>,
      r: Box<number>,
    ) {
      await delay(Math.min(l.value, r.value));
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());
  const mul = proc(
    async function mulBody(
      result: Box<number>,
      l: Box<number>,
      r: Box<number>,
    ) {
      await delay(Math.min(l.value, r.value));
      result.value = l.value * r.value;
    },
  );
  const pureMul = toFunc(mul, () => testPool.provide());

  const resultBody = new Box<number>();

  await run(new Context(contextOptions), ({ $s, $d }) => {
    const result = $d(resultBody);
    const result1 = pureAdd($s(Box.withValue(1)), $s(Box.withValue(2)));
    const result2 = pureAdd($s(Box.withValue(3)), $s(Box.withValue(4)));
    const result3 = pureMul(result1, result2);
    add(result, result3, $s(Box.withValue(5)));
  });

  assertEquals(resultBody.value, 26);
  testPool.assertNoError();
});

Deno.test(async function middleware(t) {
  const addLog: string[] = [];
  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
    {
      middlewares: [async (next) => {
        addLog.push("1 before");
        await next();
        addLog.push("1 after");
      }, async (next) => {
        addLog.push("2 before");
        await next();
        addLog.push("2 after");
      }],
    },
  );
  const divmodLog: string[] = [];
  const divmod = procN(
    function divmodBody(
      [div, mod]: [Box<number>, Box<number>],
      l: Box<number>,
      r: Box<number>,
    ) {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
    {
      middlewares: [async (next) => {
        divmodLog.push("1 before");
        await next();
        divmodLog.push("1 after");
      }, async (next) => {
        divmodLog.push("2 before");
        await next();
        divmodLog.push("2 after");
      }],
    },
  );
  const doubleLog: string[] = [];
  const double = procI(
    function doubleOutOfPlace(output: Box<number>, input0: Box<number>) {
      doubleLog.push("out-of-place");
      output.value = input0.value * 2;
    },
    function doubleInPlace(inout: Box<number>) {
      doubleLog.push("in-place");
      inout.value = inout.value * 2;
    },
    {
      middlewares: [async (next) => {
        doubleLog.push("1 before");
        await next();
        doubleLog.push("1 after");
      }, async (next) => {
        doubleLog.push("2 before");
        await next();
        doubleLog.push("2 after");
      }],
    },
  );

  await t.step(async function single() {
    const output = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const input1 = $s(Box.withValue(42));
      const input2 = $s(Box.withValue(5));

      add($d(output), input1, input2);
    });
    assertEquals(output.value, 47);
    assertEquals(addLog, ["1 before", "2 before", "2 after", "1 after"]);
  });

  await t.step(async function multiple() {
    const output1 = new Box<number>();
    const output2 = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const input1 = $s(Box.withValue(42));
      const input2 = $s(Box.withValue(5));

      divmod([$d(output1), $d(output2)], input1, input2);
    });
    assertEquals(output1.value, 8);
    assertEquals(output2.value, 2);
    assertEquals(divmodLog, ["1 before", "2 before", "2 after", "1 after"]);
  });

  await t.step(async function inPlaceSingle() {
    const testPool = createBoxedNumberTestPool();
    const output = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d, $i }) => {
      const input = $s(Box.withValue(1));
      const intermediate1 = $i(() => testPool.provide());
      const intermediate2 = $i(() => testPool.provide());
      double(intermediate1, input);
      double(intermediate2, intermediate1);
      double($d(output), intermediate2);
    });

    assertEquals(output.value, 8);
    assertEquals(doubleLog, [
      "1 before",
      "2 before",
      "out-of-place",
      "2 after",
      "1 after",
      "1 before",
      "2 before",
      "in-place",
      "2 after",
      "1 after",
      "1 before",
      "2 before",
      "out-of-place",
      "2 after",
      "1 after",
    ]);
    testPool.assertNoError();
  });
});

Deno.test(async function procIInPlace(t) {
  const testPool = createBoxedNumberTestPool();

  function getDoubler() {
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

    const getVariantsUsed = () => variantsUsed;

    return { double, pureDouble, getVariantsUsed };
  }

  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());

  await t.step(async function inPlaceSingleConsumer() {
    const { pureDouble, getVariantsUsed } = getDoubler();

    const resultBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result = $d(resultBody);
      const a = pureAdd($s(Box.withValue(10)), $s(Box.withValue(11)));
      const b = pureDouble(a);
      add(result, b, $s(Box.withValue(1)));
    });

    assertEquals(resultBody.value, 43);
    assertEquals(getVariantsUsed(), ["in-place"]);
    testPool.assertNoError();
  });

  await t.step(async function sourceInputFallsBackToOutOfPlace() {
    const { pureDouble, getVariantsUsed } = getDoubler();

    const resultBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result = $d(resultBody);
      const a = $s(Box.withValue(21));
      const b = pureDouble(a);
      add(result, b, $s(Box.withValue(1)));
    });

    assertEquals(resultBody.value, 43);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    testPool.assertNoError();
  });

  await t.step(async function destinationOutputFallsBackToOutOfPlace() {
    const { double, getVariantsUsed } = getDoubler();

    const resultBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result = $d(resultBody);
      const a = pureAdd($s(Box.withValue(10)), $s(Box.withValue(11)));
      double(result, a);
    });

    assertEquals(resultBody.value, 42);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    testPool.assertNoError();
  });

  await t.step(async function multipleConsumersAllUseOutOfPlace() {
    const { pureDouble, getVariantsUsed } = getDoubler();

    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const result1 = $d(result1Body);
      const result2 = $d(result2Body);
      const a = pureAdd($s(Box.withValue(10)), $s(Box.withValue(11)));
      const b = pureDouble(a);
      const c = pureDouble(a);
      add(result1, b, $s(Box.withValue(1)));
      add(result2, c, $s(Box.withValue(2)));
    });

    assertEquals(result1Body.value, 43);
    assertEquals(result2Body.value, 44);
    assertEquals(getVariantsUsed(), ["out-of-place", "out-of-place"]);
    testPool.assertNoError();
  });
});

Deno.test(async function procIWithRestInputs() {
  const testPool = createBoxedNumberTestPool();
  let variantUsed = "";

  const mul = procI(
    function addScalarOutOfPlace(
      output: Box<number>,
      input0: Box<number>,
      input1: Box<number>,
    ) {
      variantUsed = "out-of-place";
      output.value = input0.value * input1.value;
    },
    function addScalarInPlace(inout: Box<number>, input1: Box<number>) {
      variantUsed = "in-place";
      inout.value = inout.value * input1.value;
    },
  );
  const pureMul = toFunc(mul, () => testPool.provide());

  const add = proc(
    function addBody(result: Box<number>, l: Box<number>, r: Box<number>) {
      result.value = l.value + r.value;
    },
  );
  const pureAdd = toFunc(add, () => testPool.provide());

  const resultBody = new Box<number>();

  await run(new Context(contextOptions), ({ $s, $d }) => {
    const result = $d(resultBody);
    const a = pureAdd($s(Box.withValue(10)), $s(Box.withValue(20)));
    const b = pureMul(a, $s(Box.withValue(5)));
    add(result, b, $s(Box.withValue(1)));
  });

  assertEquals(resultBody.value, 151);
  assertEquals(variantUsed, "in-place");
  testPool.assertNoError();
});

Deno.test(async function procNI1InPlace(t) {
  const arrayPool = createNumberArrayTestPool();
  const boxPool = createBoxedNumberTestPool();

  function getNormalizer() {
    const variantsUsed: string[] = [];

    const normalizeAndNorm = procNI1(
      function outOfPlace(
        [normalized, norm]: [number[], Box<number>],
        input: number[],
      ) {
        variantsUsed.push("out-of-place");
        const n = Math.sqrt(input.reduce((s, x) => s + x * x, 0));
        norm.value = n;
        for (let i = 0; i < input.length; i++) {
          normalized[i] = input[i] / n;
        }
      },
      function inPlace(
        inout: number[],
        [norm]: [Box<number>],
      ) {
        variantsUsed.push("in-place");
        const n = Math.sqrt(inout.reduce((s, x) => s + x * x, 0));
        norm.value = n;
        for (let i = 0; i < inout.length; i++) {
          inout[i] /= n;
        }
      },
    );

    const pureNormalizeAndNorm = toFuncN(normalizeAndNorm, [
      (input) => arrayPool.provide(input.length),
      () => boxPool.provide(),
    ]);

    const getVariantsUsed = () => variantsUsed;

    return { normalizeAndNorm, pureNormalizeAndNorm, getVariantsUsed };
  }

  const scale = proc(
    function scaleBody(result: number[], input: number[], factor: Box<number>) {
      for (let i = 0; i < input.length; i++) {
        result[i] = input[i] * factor.value;
      }
    },
  );
  const pureScale = toFunc(scale, (input) => arrayPool.provide(input.length));

  const add = proc(
    function addBody(result: number[], l: number[], r: number[]) {
      const minLength = Math.min(result.length, l.length, r.length);
      for (let i = 0; i < minLength; i++) {
        result[i] = l[i] + r[i];
      }
    },
  );
  const pureAdd = toFunc(
    add,
    (l, r) => arrayPool.provide(Math.min(l.length, r.length)),
  );

  const copy = proc(
    function copyBody(result: Box<number>, input: Box<number>) {
      result.value = input.value;
    },
  );

  await t.step(async function inPlaceSingleConsumer() {
    const { pureNormalizeAndNorm, getVariantsUsed } = getNormalizer();

    const resultBody = [0, 0];
    const normBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediate = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const [normalized, norm] = pureNormalizeAndNorm(intermediate);
      scale($d(resultBody), normalized, $s(Box.withValue(1)));
      copy($d(normBody), norm);
    });

    assertEquals(normBody.value, 5);
    assertEquals(resultBody[0], 3 / 5);
    assertEquals(resultBody[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["in-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function sourceInputFallsBackToOutOfPlace() {
    const { pureNormalizeAndNorm, getVariantsUsed } = getNormalizer();

    const resultBody = [0, 0];
    const normBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const [normalized, norm] = pureNormalizeAndNorm($s([3, 4]));
      scale($d(resultBody), normalized, $s(Box.withValue(1)));
      copy($d(normBody), norm);
    });

    assertEquals(normBody.value, 5);
    assertEquals(resultBody[0], 3 / 5);
    assertEquals(resultBody[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function destinationOutputFallsBackToOutOfPlace() {
    const { normalizeAndNorm, getVariantsUsed } = getNormalizer();

    const normalizedBody = [0, 0];
    const normBody = new Box<number>();
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediate = pureScale($s([3, 4]), $s(Box.withValue(1)));
      normalizeAndNorm([$d(normalizedBody), $d(normBody)], intermediate);
    });

    assertEquals(normBody.value, 5);
    assertEquals(normalizedBody[0], 3 / 5);
    assertEquals(normalizedBody[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function multipleConsumersAllUseOutOfPlace() {
    const { pureNormalizeAndNorm, getVariantsUsed } = getNormalizer();

    const result1Body = [0, 0];
    const result2Body = [0, 0];
    const norm1Body = new Box<number>();
    const norm2Body = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediate = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const [normalized1, norm1] = pureNormalizeAndNorm(intermediate);
      const [normalized2, norm2] = pureNormalizeAndNorm(intermediate);
      scale($d(result1Body), normalized1, $s(Box.withValue(1)));
      scale($d(result2Body), normalized2, $s(Box.withValue(1)));
      copy($d(norm1Body), norm1);
      copy($d(norm2Body), norm2);
    });

    assertEquals(norm1Body.value, 5);
    assertEquals(norm2Body.value, 5);
    assertEquals(result1Body[0], 3 / 5);
    assertEquals(result1Body[1], 4 / 5);
    assertEquals(result2Body[0], 3 / 5);
    assertEquals(result2Body[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["out-of-place", "out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function bothOutputsAreGlobal() {
    const { normalizeAndNorm, getVariantsUsed } = getNormalizer();

    const normalizedBody = [0, 0];
    const normBody = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      normalizeAndNorm(
        [$d(normalizedBody), $d(normBody)],
        $s([3, 4]),
      );
    });

    assertEquals(normBody.value, 5);
    assertEquals(normalizedBody[0], 3 / 5);
    assertEquals(normalizedBody[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function restOutputIsIntermediate() {
    const { normalizeAndNorm, getVariantsUsed } = getNormalizer();

    const normalizedBody = [0, 0];
    const resultBody = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d, $i }) => {
      const normIntermediate = $i(() => boxPool.provide());
      normalizeAndNorm(
        [$d(normalizedBody), normIntermediate],
        $s([3, 4]),
      );
      copy($d(resultBody), normIntermediate);
    });

    assertEquals(resultBody.value, 5);
    assertEquals(normalizedBody[0], 3 / 5);
    assertEquals(normalizedBody[1], 4 / 5);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function chainedInPlace() {
    const { pureNormalizeAndNorm, getVariantsUsed } = getNormalizer();

    const resultBody = [0, 0];
    const norm1Body = new Box<number>();
    const norm2Body = new Box<number>();

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediate1 = pureAdd($s([3, 0]), $s([0, 4]));
      const [normalized1, norm1] = pureNormalizeAndNorm(intermediate1);

      const [normalized2, norm2] = pureNormalizeAndNorm(normalized1);

      scale($d(resultBody), normalized2, $s(Box.withValue(10)));
      copy($d(norm1Body), norm1);
      copy($d(norm2Body), norm2);
    });

    assertEquals(norm1Body.value, 5);
    assertEquals(norm2Body.value, 1);
    assertEquals(resultBody[0], 6);
    assertEquals(resultBody[1], 8);
    assertEquals(getVariantsUsed(), ["in-place", "in-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });
});

Deno.test(async function procNIAllInPlace(t) {
  const arrayPool = createNumberArrayTestPool();
  const boxPool = createBoxedNumberTestPool();

  function getDualNormalizer() {
    const variantsUsed: string[] = [];

    const normalizeDual = procNIAll(
      function outOfPlace(
        [normalizedA, normalizedB]: [number[], number[]],
        inputA: number[],
        inputB: number[],
      ) {
        variantsUsed.push("out-of-place");
        const normA = Math.sqrt(inputA.reduce((s, x) => s + x * x, 0));
        const normB = Math.sqrt(inputB.reduce((s, x) => s + x * x, 0));
        for (let i = 0; i < inputA.length; i++) {
          normalizedA[i] = inputA[i] / normA;
        }
        for (let i = 0; i < inputB.length; i++) {
          normalizedB[i] = inputB[i] / normB;
        }
      },
      function inPlace([inoutA, inoutB]: [number[], number[]]) {
        variantsUsed.push("in-place");
        const normA = Math.sqrt(inoutA.reduce((s, x) => s + x * x, 0));
        const normB = Math.sqrt(inoutB.reduce((s, x) => s + x * x, 0));
        for (let i = 0; i < inoutA.length; i++) {
          inoutA[i] /= normA;
        }
        for (let i = 0; i < inoutB.length; i++) {
          inoutB[i] /= normB;
        }
      },
    );

    const pureNormalizeDual = toFuncN(normalizeDual, [
      (inputA) => arrayPool.provide(inputA.length),
      (_, inputB) => arrayPool.provide(inputB.length),
    ]);

    const getVariantsUsed = () => variantsUsed;

    return { normalizeDual, pureNormalizeDual, getVariantsUsed };
  }

  const scale = proc(
    function scaleBody(result: number[], input: number[], factor: Box<number>) {
      for (let i = 0; i < input.length; i++) {
        result[i] = input[i] * factor.value;
      }
    },
  );
  const pureScale = toFunc(scale, (input) => arrayPool.provide(input.length));

  const add = proc(
    function addBody(result: number[], l: number[], r: number[]) {
      const minLength = Math.min(result.length, l.length, r.length);
      for (let i = 0; i < minLength; i++) {
        result[i] = l[i] + r[i];
      }
    },
  );
  const pureAdd = toFunc(
    add,
    (l, r) => arrayPool.provide(Math.min(l.length, r.length)),
  );

  await t.step(async function inPlaceBothSingleConsumer() {
    const { pureNormalizeDual, getVariantsUsed } = getDualNormalizer();

    const resultABody = [0, 0];
    const resultBBody = [0, 0];
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediateA = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const intermediateB = pureScale($s([5, 12]), $s(Box.withValue(1)));
      const [normalizedA, normalizedB] = pureNormalizeDual(
        intermediateA,
        intermediateB,
      );
      scale($d(resultABody), normalizedA, $s(Box.withValue(1)));
      scale($d(resultBBody), normalizedB, $s(Box.withValue(1)));
    });

    assertEquals(resultABody[0], 3 / 5);
    assertEquals(resultABody[1], 4 / 5);
    assertEquals(resultBBody[0], 5 / 13);
    assertEquals(resultBBody[1], 12 / 13);
    assertEquals(getVariantsUsed(), ["in-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function sourceInputFallsBackToOutOfPlace() {
    const { pureNormalizeDual, getVariantsUsed } = getDualNormalizer();

    const resultABody = [0, 0];
    const resultBBody = [0, 0];
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediateB = pureScale($s([5, 12]), $s(Box.withValue(1)));
      const [normalizedA, normalizedB] = pureNormalizeDual(
        $s([3, 4]),
        intermediateB,
      );
      scale($d(resultABody), normalizedA, $s(Box.withValue(1)));
      scale($d(resultBBody), normalizedB, $s(Box.withValue(1)));
    });

    assertEquals(resultABody[0], 3 / 5);
    assertEquals(resultABody[1], 4 / 5);
    assertEquals(resultBBody[0], 5 / 13);
    assertEquals(resultBBody[1], 12 / 13);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function destinationOutputFallsBackToOutOfPlace() {
    const { normalizeDual, getVariantsUsed } = getDualNormalizer();

    const resultABody = [0, 0];
    const resultBBody = [0, 0];
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediateA = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const intermediateB = pureScale($s([5, 12]), $s(Box.withValue(1)));
      normalizeDual(
        [$d(resultABody), $d(resultBBody)],
        intermediateA,
        intermediateB,
      );
    });

    assertEquals(resultABody[0], 3 / 5);
    assertEquals(resultABody[1], 4 / 5);
    assertEquals(resultBBody[0], 5 / 13);
    assertEquals(resultBBody[1], 12 / 13);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function oneOutputIsGlobalFallsBackToOutOfPlace() {
    const { normalizeDual, getVariantsUsed } = getDualNormalizer();

    const resultABody = [0, 0];
    const resultBBody = [0, 0];
    await run(new Context(contextOptions), ({ $s, $d, $i }) => {
      const intermediateA = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const intermediateB = pureScale($s([5, 12]), $s(Box.withValue(1)));
      const outputA = $i(() => arrayPool.provide(2));
      normalizeDual(
        [outputA, $d(resultBBody)],
        intermediateA,
        intermediateB,
      );
      scale($d(resultABody), outputA, $s(Box.withValue(1)));
    });

    assertEquals(resultABody[0], 3 / 5);
    assertEquals(resultABody[1], 4 / 5);
    assertEquals(resultBBody[0], 5 / 13);
    assertEquals(resultBBody[1], 12 / 13);
    assertEquals(getVariantsUsed(), ["out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function multipleConsumersFallsBackToOutOfPlace() {
    const { pureNormalizeDual, getVariantsUsed } = getDualNormalizer();

    const result1ABody = [0, 0];
    const result1BBody = [0, 0];
    const result2ABody = [0, 0];
    const result2BBody = [0, 0];

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediateA = pureScale($s([3, 4]), $s(Box.withValue(1)));
      const intermediateB = pureScale($s([5, 12]), $s(Box.withValue(1)));
      const [normalized1A, normalized1B] = pureNormalizeDual(
        intermediateA,
        intermediateB,
      );
      const [normalized2A, normalized2B] = pureNormalizeDual(
        intermediateA,
        intermediateB,
      );
      scale($d(result1ABody), normalized1A, $s(Box.withValue(1)));
      scale($d(result1BBody), normalized1B, $s(Box.withValue(1)));
      scale($d(result2ABody), normalized2A, $s(Box.withValue(1)));
      scale($d(result2BBody), normalized2B, $s(Box.withValue(1)));
    });

    assertEquals(result1ABody[0], 3 / 5);
    assertEquals(result1ABody[1], 4 / 5);
    assertEquals(result1BBody[0], 5 / 13);
    assertEquals(result1BBody[1], 12 / 13);
    assertEquals(result2ABody[0], 3 / 5);
    assertEquals(result2ABody[1], 4 / 5);
    assertEquals(result2BBody[0], 5 / 13);
    assertEquals(result2BBody[1], 12 / 13);
    assertEquals(getVariantsUsed(), ["out-of-place", "out-of-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function chainedInPlace() {
    const { pureNormalizeDual, getVariantsUsed } = getDualNormalizer();

    const resultABody = [0, 0];
    const resultBBody = [0, 0];

    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediate1A = pureAdd($s([3, 0]), $s([0, 4]));
      const intermediate1B = pureAdd($s([5, 0]), $s([0, 12]));
      const [normalized1A, normalized1B] = pureNormalizeDual(
        intermediate1A,
        intermediate1B,
      );
      const [normalized2A, normalized2B] = pureNormalizeDual(
        normalized1A,
        normalized1B,
      );
      scale($d(resultABody), normalized2A, $s(Box.withValue(10)));
      scale($d(resultBBody), normalized2B, $s(Box.withValue(13)));
    });

    assertEquals(resultABody[0], 6);
    assertEquals(resultABody[1], 8);
    assertEquals(resultBBody[0], 5);
    assertEquals(resultBBody[1], 12);
    assertEquals(getVariantsUsed(), ["in-place", "in-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });

  await t.step(async function withRestInputs() {
    const variantsUsed: string[] = [];

    const scaleTwo = procNIAll(
      function outOfPlace(
        [scaledA, scaledB]: [number[], number[]],
        inputA: number[],
        inputB: number[],
        factorA: Box<number>,
        factorB: Box<number>,
      ) {
        variantsUsed.push("out-of-place");
        for (let i = 0; i < inputA.length; i++) {
          scaledA[i] = inputA[i] * factorA.value;
        }
        for (let i = 0; i < inputB.length; i++) {
          scaledB[i] = inputB[i] * factorB.value;
        }
      },
      function inPlace(
        [inoutA, inoutB]: [number[], number[]],
        factorA: Box<number>,
        factorB: Box<number>,
      ) {
        variantsUsed.push("in-place");
        for (let i = 0; i < inoutA.length; i++) {
          inoutA[i] *= factorA.value;
        }
        for (let i = 0; i < inoutB.length; i++) {
          inoutB[i] *= factorB.value;
        }
      },
    );

    const pureScaleTwo = toFuncN(scaleTwo, [
      (inputA) => arrayPool.provide(inputA.length),
      (_, inputB) => arrayPool.provide(inputB.length),
    ]);

    const resultABody = [0, 0];
    const resultBBody = [0, 0];
    await run(new Context(contextOptions), ({ $s, $d }) => {
      const intermediateA = pureAdd($s([1, 2]), $s([0, 0]));
      const intermediateB = pureAdd($s([3, 4]), $s([0, 0]));
      const [scaledA, scaledB] = pureScaleTwo(
        intermediateA,
        intermediateB,
        $s(Box.withValue(2)),
        $s(Box.withValue(3)),
      );
      scale($d(resultABody), scaledA, $s(Box.withValue(1)));
      scale($d(resultBBody), scaledB, $s(Box.withValue(1)));
    });

    assertEquals(resultABody[0], 2);
    assertEquals(resultABody[1], 4);
    assertEquals(resultBBody[0], 9);
    assertEquals(resultBBody[1], 12);
    assertEquals(variantsUsed, ["in-place"]);
    arrayPool.assertNoError();
    boxPool.assertNoError();
  });
});

Deno.test(async function errorPaths(t) {
  await t.step("proc: output provide throws", async () => {
    const arrayPool = createNumberArrayTestPool();

    const copy = proc(function copyBody(result: number[], input: number[]) {
      for (let i = 0; i < result.length; i++) {
        result[i] = input[i];
      }
    });

    await assertRejects(
      () =>
        run(new Context(contextOptions), ({ $s, $i }) => {
          const output = $i<number[]>(() => {
            throw new Error("test");
          });
          copy(output, $s([1, 2]));
        }),
      Error,
      "test",
    );

    arrayPool.assertNoError();
  });

  await t.step("procN: second output provide throws", async () => {
    const boxPool = createBoxedNumberTestPool();

    const two = procN(function twoBody(
      _outputs: [Box<number>, Box<number>],
      _input: Box<number>,
    ) {});

    let provide0Called = false;
    let provide1Called = false;

    await assertRejects(
      () =>
        run(new Context(contextOptions), ({ $s, $i }) => {
          const output0 = $i(() => {
            provide0Called = true;
            return boxPool.provide();
          });
          const output1 = $i<Box<number>>(() => {
            provide1Called = true;
            throw new Error("test");
          });
          two([output0, output1], $s(Box.withValue(1)));
        }),
      Error,
      "test",
    );

    assertEquals(provide0Called, true);
    assertEquals(provide1Called, true);
    boxPool.assertNoError();
  });

  await t.step("procI: in-place body throws", async () => {
    const arrayPool = createNumberArrayTestPool();

    const copy = proc(function copyBody(result: number[], input: number[]) {
      for (let i = 0; i < result.length; i++) {
        result[i] = input[i];
      }
    });
    const pureCopy = toFunc(copy, (input) => arrayPool.provide(input.length));

    const explode = procI(
      function outOfPlace(_output: number[], _input0: number[]) {
        // not used in this test
      },
      function inPlace(_inout: number[]) {
        throw new Error("test");
      },
    );

    await assertRejects(
      () =>
        run(new Context(contextOptions), ({ $s, $i }) => {
          const input0 = pureCopy($s([1, 2]));
          const output = $i(() => arrayPool.provide(2));
          explode(output, input0);
        }),
      Error,
      "test",
    );

    arrayPool.assertNoError();
  });

  await t.step(
    "procNI1: in-place does not leak when rest output provide throws",
    async () => {
      const arrayPool = createNumberArrayTestPool();

      const copy = proc(function copyBody(result: number[], input: number[]) {
        for (let i = 0; i < result.length; i++) {
          result[i] = input[i];
        }
      });
      const pureCopy = toFunc(copy, (input) => arrayPool.provide(input.length));

      const passThroughAndFlag = procNI1(
        function outOfPlace(
          [_output0, _flag]: [number[], Box<number>],
          _input0: number[],
        ) {
          // not used in this test
        },
        function inPlace(
          _inout0: number[],
          [_flag]: [Box<number>],
        ) {
          // not used in this test
        },
      );

      let provideCalled = false;

      await assertRejects(
        () =>
          run(new Context(contextOptions), ({ $s, $i }) => {
            const input0 = pureCopy($s([1, 2]));

            const output0 = $i(() => arrayPool.provide(2));
            const flag = $i<Box<number>>(() => {
              provideCalled = true;
              throw new Error("test");
            });

            passThroughAndFlag([output0, flag], input0);
          }),
        Error,
        "test",
      );

      assertEquals(provideCalled, true);
      arrayPool.assertNoError();
    },
  );

  await t.step("procNIAll: in-place body throws", async () => {
    const arrayPool = createNumberArrayTestPool();

    const copy = proc(function copyBody(result: number[], input: number[]) {
      for (let i = 0; i < result.length; i++) {
        result[i] = input[i];
      }
    });
    const pureCopy = toFunc(copy, (input) => arrayPool.provide(input.length));

    const explode = procNIAll(
      function outOfPlace(
        _outputs: [number[], number[]],
        _a: number[],
        _b: number[],
      ) {
        // not used in this test
      },
      function inPlace(_inout: [number[], number[]]) {
        throw new Error("test");
      },
    );

    await assertRejects(
      () =>
        run(new Context(contextOptions), ({ $s, $i }) => {
          const a = pureCopy($s([1, 2]));
          const b = pureCopy($s([3, 4]));
          const outA = $i(() => arrayPool.provide(2));
          const outB = $i(() => arrayPool.provide(2));
          explode([outA, outB], a, b);
        }),
      Error,
      "test",
    );

    arrayPool.assertNoError();
  });
});

Deno.test(function types() {
  const so = proc(
    function soBody(_x: Box<number>, _a: Box<string>, _b: Box<boolean>) {},
  );
  const mo = procN(
    function moBody(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<boolean>,
      _b: Box<bigint>,
    ) {},
  );
  // procNI1
  const moi = procNI1(
    function moiOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
      _b: Box<boolean>,
      _c: Box<bigint>,
    ) {},
    function moiInPlace(
      _x: Box<number>,
      [_y]: [Box<string>],
      _b: Box<boolean>,
      _c: Box<bigint>,
    ) {},
  );
  procNI1(
    function moiOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
    ) {},
    // @ts-expect-error: inout type conflicts with output0/input0 type
    function moiInPlace(_x: Box<string>, [_y]: [Box<string>]) {},
  );
  procNI1(
    function moiOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
      _b: Box<boolean>,
      _c: Box<bigint>,
    ) {},
    // @ts-expect-error: rest input types conflict with fOutOfPlace
    function moiInPlace(
      _x: Box<number>,
      [_y]: [Box<string>],
      _b: Box<bigint>,
      _c: Box<boolean>,
    ) {},
  );
  // procI
  const soi = procI(
    function soiOutOfPlace(
      _x: Box<number>,
      _a: Box<number>,
      _b: Box<string>,
      _c: Box<boolean>,
    ) {},
    function soiInPlace(_x: Box<number>, _b: Box<string>, _c: Box<boolean>) {},
  );
  procI(
    function soiOutOfPlace(_x: Box<number>, _a: Box<number>) {},
    // @ts-expect-error: inout type conflicts with output/input0 type
    function soiInPlace(_x: Box<string>) {},
  );
  procI(
    function soiOutOfPlace(
      _x: Box<number>,
      _a: Box<number>,
      _b: Box<string>,
      _c: Box<boolean>,
    ) {},
    // @ts-expect-error: rest input types conflict with fOutOfPlace
    function soiInPlace(
      _x: Box<number>,
      _b: Box<boolean>,
      _c: Box<string>,
    ) {},
  );
  // procNIAll
  const moiAll = procNIAll(
    function moiAllOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
      _b: Box<string>,
      _c: Box<boolean>,
      _d: Box<bigint>,
    ) {},
    function moiAllInPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _c: Box<boolean>,
      _d: Box<bigint>,
    ) {},
  );
  procNIAll(
    function moiAllOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
      _b: Box<string>,
    ) {},
    // @ts-expect-error: inout type conflicts with outputs/IO inputs type
    function moiAllInPlace([_x, _y]: [Box<string>, Box<string>]) {},
  );
  procNIAll(
    // @ts-expect-error: rest input types conflict with fInPlace
    function moiAllOutOfPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _a: Box<number>,
      _b: Box<string>,
      _c: Box<boolean>,
      _d: Box<bigint>,
    ) {},
    function moiAllInPlace(
      [_x, _y]: [Box<number>, Box<string>],
      _c: Box<bigint>,
      _d: Box<boolean>,
    ) {},
  );
  const sop = toFunc(
    so,
    (_a: Box<string>, _b: Box<boolean>) =>
      testValue<DisposableWrap<Box<number>>>(),
  );
  const mop = toFuncN(mo, [
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

    // moi: OK
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );

    // soi: OK
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );

    // moiAll: OK
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
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

    // moi: NG
    moi(
      // @ts-expect-error: 1st output type is wrong
      [testValue<Handle<Box<symbol>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moi(
      // @ts-expect-error: 2nd output type is wrong
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<symbol>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      // @ts-expect-error: input0 type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: 1st rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: 2nd rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient args
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
      // @ts-expect-error: excessive args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
    moi(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: rest args swapped
      testValue<Handle<Box<bigint>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    moi(
      // @ts-expect-error: output0 and input0 type mismatch
      [testValue<Handle<Box<string>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );

    // soi: NG
    soi(
      // @ts-expect-error: output type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    soi(
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: input0 type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: 1st rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      // @ts-expect-error: 2nd rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient args
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
    );
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: excessive args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
    soi(
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: rest args swapped
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<string>>>(),
    );
    soi(
      // @ts-expect-error: output and input0 type mismatch
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );

    // moiAll: NG
    moiAll(
      // @ts-expect-error: 1st output type is wrong
      [testValue<Handle<Box<symbol>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moiAll(
      // @ts-expect-error: 2nd output type is wrong
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<symbol>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      // @ts-expect-error: 1st IO input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      // @ts-expect-error: 2nd IO input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      // @ts-expect-error: 1st rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      // @ts-expect-error: 2nd rest input type is wrong
      testValue<Handle<Box<symbol>>>(),
    );
    // @ts-expect-error: insufficient args
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
      // @ts-expect-error: excessive args
      // deno-lint-ignore no-explicit-any
      testValue<Handle<Box<any>>>(),
    );
    moiAll(
      [testValue<Handle<Box<number>>>(), testValue<Handle<Box<string>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      // @ts-expect-error: rest args swapped
      testValue<Handle<Box<bigint>>>(),
      testValue<Handle<Box<boolean>>>(),
    );
    moiAll(
      // @ts-expect-error: outputs and IO inputs type mismatch
      [testValue<Handle<Box<string>>>(), testValue<Handle<Box<number>>>()],
      testValue<Handle<Box<number>>>(),
      testValue<Handle<Box<string>>>(),
      testValue<Handle<Box<boolean>>>(),
      testValue<Handle<Box<bigint>>>(),
    );
  });
});
