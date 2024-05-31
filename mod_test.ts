import {
  assertEquals,
  assertFalse,
  assertGreater,
  assertGreaterOrEqual,
} from "./deps.ts";
import {
  Context,
  multipleOutputAction,
  multipleOutputPurify,
  singleOutputAction,
  singleOutputPurify,
  typeSpec,
} from "./mod.ts";
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

  const boxedNumberPool = new Pool<Box<number>, []>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec(boxedNumberPool);

  const add = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );
  const pureAdd = singleOutputPurify(add);
  const mul = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: Box<number>, r: Box<number>) =>
      result.value = l.value * r.value,
  );
  const pureMul = singleOutputPurify(mul);

  const resultBody = new Box<number>();

  new Context().run(({ input, output }) => {
    const result = output(resultBody);
    const result1 = pureAdd(
      input(Box.withValue(1)),
      input(Box.withValue(2)),
    );
    const result2 = pureAdd(
      input(Box.withValue(3)),
      input(Box.withValue(4)),
    );
    const result3 = pureMul(result1, result2);
    add(result, result3, input(Box.withValue(5)));
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

  const boxedNumberPool = new Pool<Box<number>, []>(
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

  const divmod = multipleOutputAction(
    [BoxedNumber, BoxedNumber],
    [
      (provider, _l, _r) => provider.acquire(),
      (provider, _l, _r) => provider.acquire(),
    ],
    ([div, mod], l: Box<number>, r: Box<number>) => {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const pureDivmod = multipleOutputPurify(divmod);
  const add = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );

  await t.step(function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const div = output(divBody);
      const mod = output(modBody);

      divmod([div, mod], input(Box.withValue(42)), input(Box.withValue(5)));
    }, { assertNoLeak: true });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    assertPostCondition();
  });

  await t.step(function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const result = output(resultBody);
      const [, mod] = pureDivmod(
        input(Box.withValue(42)),
        input(Box.withValue(5)),
      );
      add(result, mod, input(Box.withValue(100)));
    }, { assertNoLeak: true });

    assertEquals(resultBody.value, 102);
    assertPostCondition();
  });
});

Deno.test(async function outputUsage(t) {
  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>, []>(
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

  const add = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );
  const pureAdd = singleOutputPurify(add);
  const mul = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: Box<number>, r: Box<number>) =>
      result.value = l.value * r.value,
  );
  const pureMul = singleOutputPurify(mul);

  await t.step(function noOutputsAreUsed() {
    new Context().run(({ input }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      pureAdd(input1, input2);
      pureMul(input1, input2);
    }, { assertNoLeak: true });

    assertPostCondition();
  });

  await t.step(function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    new Context().run(({ input, output }) => {
      const result1 = output(result1Body);
      const result2 = output(result2Body);

      const sum = pureAdd(
        input(Box.withValue(42)),
        input(Box.withValue(2)),
      );
      mul(result1, sum, input(Box.withValue(3)));
      add(result2, sum, input(Box.withValue(4)));
    }, { assertNoLeak: true });

    assertPostCondition();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    new Context().run(({ input, output }) => {
      const sum = output(sumBody);
      const result = output(resultBody);

      add(sum, input(Box.withValue(42)), input(Box.withValue(2)));
      mul(result, sum, input(Box.withValue(3)));
    }, { assertNoLeak: true });

    assertPostCondition();
    assertEquals(sumBody.value, 44);
    assertEquals(resultBody.value, 132);
  });
});

Deno.test(function calcIO() {
  let errorReported = false;

  const boxedNumberPool = new Pool<IPipeBoxRW<number>, []>(
    () => pipeBoxRW<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const BoxedNumber = typeSpec<
    IPipeBoxRW<number>,
    [],
    IPipeBoxR<number>,
    IPipeBoxW<number>
  >(boxedNumberPool);

  const add = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: IPipeBoxR<number>, r: IPipeBoxR<number>) =>
      result.setValue(l.getValue() + r.getValue()),
  );
  const pureAdd = singleOutputPurify(add);
  const mul = singleOutputAction(
    BoxedNumber,
    (provider, _l, _r) => provider.acquire(),
    (result, l: IPipeBoxR<number>, r: IPipeBoxR<number>) =>
      result.setValue(l.getValue() * r.getValue()),
  );
  const pureMul = singleOutputPurify(mul);

  const [input1R, input1W] = pipeBox<number>();
  input1W.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [result1R, result1W] = pipeBox<number>();
  const result2RW = pipeBoxRW<number>();
  new Context().run(({ input, output }) => {
    const result1Handle = output(result1W);
    const result2Handle = output(result2RW);

    const result1 = pureAdd(
      input(input1R),
      input(input2RW),
    );
    const result2 = pureAdd(
      input(pipeBoxR(3)),
      input(pipeBoxR(4)),
    );
    const result3 = pureMul(result1, result2);

    const input5 = input(pipeBoxR(5));
    add(result1Handle, result3, input5);
    mul(result2Handle, result3, input5);
  }, { assertNoLeak: true });

  assertEquals(result1R.getValue(), 26);
  assertEquals(result2RW.getValue(), 105);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});
