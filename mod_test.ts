import {
  assertEquals,
  assertFalse,
  assertGreater,
  assertGreaterOrEqual,
} from "./deps.ts";
import { action, Context, purify, typeSpec } from "./mod.ts";
import { Pool } from "./pool.ts";
import { Box } from "./box.ts";
import {
  IPipeBoxR,
  IPipeBoxRW,
  IPipeBoxW,
  pipeBox,
  pipeBoxR,
  pipeBoxRW,
} from "./pipebox.ts";

Deno.test(function calc() {
  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec(boxedNumberPool);

  const add = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.value = l.value + r.value,
  );
  const pureAdd = purify(add);
  const mul = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.value = l.value * r.value,
  );
  const pureMul = purify(mul);

  const resultBody = new Box<number>();

  new Context().run(({ input, output }) => {
    const input1 = input(Box.withValue(1));
    const input2 = input(Box.withValue(2));
    const input3 = input(Box.withValue(3));
    const input4 = input(Box.withValue(4));
    const input5 = input(Box.withValue(5));

    const result = output(resultBody);

    const { result: result1 } = pureAdd({ l: input1, r: input2 });
    const { result: result2 } = pureAdd({ l: input3, r: input4 });
    const { result: result3 } = pureMul({ l: result1, r: result2 });
    add({ result }, { l: result3, r: input5 });
  }, { assertNoLeak: true });

  assertEquals(resultBody.value, 26);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});

Deno.test(function empty() {
  new Context().run(() => {}, { assertNoLeak: true });
});

Deno.test(async function twoOutputs(t) {
  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec(boxedNumberPool);

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const divmod = action(
    { div: BoxedNumber, mod: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ div, mod }, { l, r }) => {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const pureDivmod = purify(divmod);
  const add = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.value = l.value + r.value,
  );

  await t.step(function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      const div = output(divBody);
      const mod = output(modBody);

      divmod({ div, mod }, { l: input1, r: input2 });
    }, { assertNoLeak: true });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    assertPostCondition();
  });

  await t.step(function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));
      const input3 = input(Box.withValue(100));

      const result = output(resultBody);

      const { mod } = pureDivmod({ l: input1, r: input2 });
      add({ result }, { l: mod, r: input3 });
    }, { assertNoLeak: true });

    assertEquals(resultBody.value, 102);
    assertPostCondition();
  });
});

Deno.test(async function outputUsage(t) {
  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec(boxedNumberPool);

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const add = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.value = l.value + r.value,
  );
  const pureAdd = purify(add);
  const mul = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.value = l.value * r.value,
  );
  const pureMul = purify(mul);

  await t.step(function noOutputsAreUsed() {
    new Context().run(({ input }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      pureAdd({ l: input1, r: input2 });
      pureMul({ l: input1, r: input2 });
    }, { assertNoLeak: true });

    assertPostCondition();
  });

  await t.step(function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    new Context().run(({ input, output }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(2));
      const input3 = input(Box.withValue(3));
      const input4 = input(Box.withValue(4));

      const result1 = output(result1Body);
      const result2 = output(result2Body);

      const { result: sum } = pureAdd({ l: input1, r: input2 });
      mul({ result: result1 }, { l: sum, r: input3 });
      add({ result: result2 }, { l: sum, r: input4 });
    }, { assertNoLeak: true });

    assertPostCondition();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(2));
      const input3 = input(Box.withValue(3));

      const sum = output(sumBody);
      const result = output(resultBody);

      add({ result: sum }, { l: input1, r: input2 });
      mul({ result }, { l: sum, r: input3 });
    }, { assertNoLeak: true });

    assertPostCondition();
    assertEquals(sumBody.value, 44);
    assertEquals(resultBody.value, 132);
  });
});

Deno.test(function calcIO() {
  let errorReported = false;

  const boxedNumberPool = new Pool<IPipeBoxRW<number>>(
    () => pipeBoxRW<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec<
    IPipeBoxRW<number>,
    IPipeBoxR<number>,
    IPipeBoxW<number>
  >(boxedNumberPool);

  const add = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.setValue(l.getValue() + r.getValue()),
  );
  const pureAdd = purify(add);
  const mul = action(
    { result: BoxedNumber },
    { l: BoxedNumber, r: BoxedNumber },
    ({ result }, { l, r }) => result.setValue(l.getValue() * r.getValue()),
  );
  const pureMul = purify(mul);

  const [input1R, input1W] = pipeBox<number>();
  input1W.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [result1R, result1W] = pipeBox<number>();
  const result2RW = pipeBoxRW<number>();
  new Context().run(({ input, output }) => {
    const input1 = input(input1R);
    const input2 = input(input2RW);
    const input3 = input(pipeBoxR(3));
    const input4 = input(pipeBoxR(4));
    const input5 = input(pipeBoxR(5));

    const { result: result1 } = pureAdd({ l: input1, r: input2 });
    const { result: result2 } = pureAdd({ l: input3, r: input4 });
    const { result: result3 } = pureMul({ l: result1, r: result2 });

    const result1Handle = output(result1W);
    const result2Handle = output(result2RW);

    add({ result: result1Handle }, { l: result3, r: input5 });
    mul({ result: result2Handle }, { l: result3, r: input5 });
  }, { assertNoLeak: true });

  assertEquals(result1R.getValue(), 26);
  assertEquals(result2RW.getValue(), 105);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});
