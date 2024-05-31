import {
  assertEquals,
  assertFalse,
  assertGreater,
  assertGreaterOrEqual,
} from "./deps.ts";
import {
  Context,
  ContextOptions,
  multipleOutputAction,
  multipleOutputPurify,
  ProviderWrap,
  singleOutputAction,
  singleOutputPurify,
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

const contextOptions: Partial<ContextOptions> = {
  reportError: console.error,
  assertNoLeak: true,
};

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
  const boxedNumberProvider = new ProviderWrap(boxedNumberPool);

  const add = singleOutputAction(
    (result: Box<number>, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );
  const pureAdd = singleOutputPurify(add, () => boxedNumberProvider.acquire());
  const mul = singleOutputAction(
    (result: Box<number>, l: Box<number>, r: Box<number>) =>
      result.value = l.value * r.value,
  );
  const pureMul = singleOutputPurify(mul, () => boxedNumberProvider.acquire());

  const resultBody = new Box<number>();

  new Context(contextOptions).run(({ source, sink }) => {
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
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});

Deno.test(function empty() {
  new Context(contextOptions).run(() => {});
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
  const boxedNumberProvider = new ProviderWrap(boxedNumberPool);

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const divmod = multipleOutputAction(
    (
      [div, mod]: [Box<number>, Box<number>],
      l: Box<number>,
      r: Box<number>,
    ) => {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const pureDivmod = multipleOutputPurify(divmod, [
    () => boxedNumberProvider.acquire(),
    () => boxedNumberProvider.acquire(),
  ]);
  const add = singleOutputAction(
    (result: Box<number>, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );

  await t.step(function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    new Context(contextOptions).run(({ source, sink }) => {
      const div = sink(divBody);
      const mod = sink(modBody);

      divmod([div, mod], source(Box.withValue(42)), source(Box.withValue(5)));
    });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    assertPostCondition();
  });

  await t.step(function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    new Context(contextOptions).run(({ source, sink }) => {
      const result = sink(resultBody);
      const [, mod] = pureDivmod(
        source(Box.withValue(42)),
        source(Box.withValue(5)),
      );
      add(result, mod, source(Box.withValue(100)));
    });

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
  const boxedNumberProvider = new ProviderWrap(boxedNumberPool);

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const add = singleOutputAction(
    (result: Box<number>, l: Box<number>, r: Box<number>) =>
      result.value = l.value + r.value,
  );
  const pureAdd = singleOutputPurify(add, () => boxedNumberProvider.acquire());
  const mul = singleOutputAction(
    (result: Box<number>, l: Box<number>, r: Box<number>) =>
      result.value = l.value * r.value,
  );
  const pureMul = singleOutputPurify(mul, () => boxedNumberProvider.acquire());

  await t.step(function noOutputsAreUsed() {
    new Context(contextOptions).run(({ source }) => {
      const input1 = source(Box.withValue(42));
      const input2 = source(Box.withValue(5));

      pureAdd(input1, input2);
      pureMul(input1, input2);
    });

    assertPostCondition();
  });

  await t.step(function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    new Context(contextOptions).run(({ source, sink }) => {
      const result1 = sink(result1Body);
      const result2 = sink(result2Body);

      const sum = pureAdd(
        source(Box.withValue(42)),
        source(Box.withValue(2)),
      );
      mul(result1, sum, source(Box.withValue(3)));
      add(result2, sum, source(Box.withValue(4)));
    });

    assertPostCondition();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    new Context(contextOptions).run(({ source, sink }) => {
      const sum = sink(sumBody);
      const result = sink(resultBody);

      add(sum, source(Box.withValue(42)), source(Box.withValue(2)));
      mul(result, sum, source(Box.withValue(3)));
    });

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
  const boxedNumberProvider = new ProviderWrap(boxedNumberPool);

  const add = singleOutputAction(
    (result: IPipeBoxW<number>, l: IPipeBoxR<number>, r: IPipeBoxR<number>) =>
      result.setValue(l.getValue() + r.getValue()),
  );
  const pureAdd = singleOutputPurify(add, () => boxedNumberProvider.acquire());
  const mul = singleOutputAction(
    (result: IPipeBoxW<number>, l: IPipeBoxR<number>, r: IPipeBoxR<number>) =>
      result.setValue(l.getValue() * r.getValue()),
  );
  const pureMul = singleOutputPurify(mul, () => boxedNumberProvider.acquire());

  const [input1R, input1W] = pipeBox<number>();
  input1W.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [result1R, result1W] = pipeBox<number>();
  const result2RW = pipeBoxRW<number>();
  new Context(contextOptions).run(({ source, sink }) => {
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
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});
