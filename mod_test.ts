import {
  assertEquals,
  assertFalse,
  assertGreater,
  assertGreaterOrEqual,
} from "./deps.ts";
import { ContextBuilder } from "./mod.ts";
import { Pool } from "./pool.ts";
import { Box } from "./box.ts";
import { typeSpec } from "./mod.ts";
import { IPipeBoxR, IPipeBoxW } from "./pipebox.ts";
import { pipeBox } from "./pipebox.ts";
import { IPipeBoxRW } from "./pipebox.ts";
import { pipeBoxRW } from "./pipebox.ts";
import { pipeBoxR } from "./pipebox.ts";

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

  const ctx = ContextBuilder.empty()
    .addAction(
      "add",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.value = l.value + r.value,
    ).addAction(
      "mul",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.value = l.value * r.value,
    ).build();

  const resultBody = new Box<number>();
  ctx.run(({ input, output, actions: { add, mul } }) => {
    const input1 = input(Box.withValue(1));
    const input2 = input(Box.withValue(2));
    const input3 = input(Box.withValue(3));
    const input4 = input(Box.withValue(4));
    const input5 = input(Box.withValue(5));

    const result = output(resultBody);

    const { result: result1 } = add({ l: input1, r: input2 }, {});
    const { result: result2 } = add({ l: input3, r: input4 }, {});
    const { result: result3 } = mul({ l: result1, r: result2 }, {});
    add({ l: result3, r: input5 }, { result });
  }, { assertNoLeak: true });

  assertEquals(resultBody.value, 26);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});

Deno.test(function empty() {
  const ctx = ContextBuilder.empty().build();
  ctx.run(() => {}, { assertNoLeak: true });
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

  const ctx = ContextBuilder.empty()
    .addAction(
      "divmod",
      { l: BoxedNumber, r: BoxedNumber },
      { div: BoxedNumber, mod: BoxedNumber },
      ({ l, r }, { div, mod }) => {
        div.value = Math.floor(l.value / r.value);
        mod.value = l.value % r.value;
      },
    ).addAction(
      "add",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.value = l.value + r.value,
    ).build();

  await t.step(function bothOutputsAreGlobal() {
    const divBody = new Box<number>();
    const modBody = new Box<number>();

    ctx.run(({ input, output, actions: { divmod } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      const div = output(divBody);
      const mod = output(modBody);

      divmod({ l: input1, r: input2 }, { div, mod });
    }, { assertNoLeak: true });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    assertPostCondition();
  });

  await t.step(function divOutputIsGlobal() {
    const divBody = new Box<number>();

    ctx.run(({ input, output, actions: { divmod } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      const div = output(divBody);

      divmod({ l: input1, r: input2 }, { div });
    }, { assertNoLeak: true });

    assertEquals(divBody.value, 8);
    assertPostCondition();
  });

  await t.step(function modOutputIsIntermediate() {
    const resultBody = new Box<number>();

    ctx.run(({ input, output, actions: { divmod, add } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));
      const input3 = input(Box.withValue(100));

      const result = output(resultBody);

      const { mod } = divmod({ l: input1, r: input2 }, {});
      add({ l: mod, r: input3 }, { result });
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

  const ctx = ContextBuilder.empty()
    .addAction(
      "add",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.value = l.value + r.value,
    ).addAction(
      "mul",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.value = l.value * r.value,
    ).build();

  await t.step(function noOutputsAreUsed() {
    ctx.run(({ input, actions: { add, mul } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(5));

      add({ l: input1, r: input2 }, {});
      mul({ l: input1, r: input2 }, {});
    }, { assertNoLeak: true });

    assertPostCondition();
  });

  await t.step(function outputIsUsedTwice() {
    const result1Body = new Box<number>();
    const result2Body = new Box<number>();

    ctx.run(({ input, output, actions: { add, mul } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(2));
      const input3 = input(Box.withValue(3));
      const input4 = input(Box.withValue(4));

      const result1 = output(result1Body);
      const result2 = output(result2Body);

      const { result: sum } = add({ l: input1, r: input2 }, {});
      mul({ l: sum, r: input3 }, { result: result1 });
      add({ l: sum, r: input4 }, { result: result2 });
    }, { assertNoLeak: true });

    assertPostCondition();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(function globalOutputIsUsedAsInput() {
    const sumBody = new Box<number>();
    const resultBody = new Box<number>();

    ctx.run(({ input, output, actions: { add, mul } }) => {
      const input1 = input(Box.withValue(42));
      const input2 = input(Box.withValue(2));
      const input3 = input(Box.withValue(3));

      const sum = output(sumBody);
      const result = output(resultBody);

      add({ l: input1, r: input2 }, { result: sum });
      mul({ l: sum, r: input3 }, { result });
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

  const ctx = ContextBuilder.empty()
    .addAction(
      "add",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.setValue(l.getValue() + r.getValue()),
    ).addAction(
      "mul",
      { l: BoxedNumber, r: BoxedNumber },
      { result: BoxedNumber },
      ({ l, r }, { result }) => result.setValue(l.getValue() * r.getValue()),
    ).build();

  const [input1Reader, input1Writer] = pipeBox<number>();
  input1Writer.setValue(1);
  const input2RW = pipeBoxRW<number>();
  input2RW.setValue(2);
  const [resultReader, resultWriter] = pipeBox<number>();
  ctx.run(({ input, output, actions: { add, mul } }) => {
    const input1 = input(input1Reader);
    const input2 = input(input2RW);
    const input3 = input(pipeBoxR(3));
    const input4 = input(pipeBoxR(4));
    const input5 = input(pipeBoxR(5));

    const { result: result1 } = add({ l: input1, r: input2 }, {});
    const { result: result2 } = add({ l: input3, r: input4 }, {});
    const { result: result3 } = mul({ l: result1, r: result2 }, {});

    const result = output(resultWriter);

    add({ l: result3, r: input5 }, { result });
  }, { assertNoLeak: true });

  assertEquals(resultReader.getValue(), 26);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});
