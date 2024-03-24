import {
  assertEquals,
  assertFalse,
  assertGreater,
  assertGreaterOrEqual,
} from "./deps.ts";
import { action, Context, input, Plan, run } from "./mod.ts";
import { Pool } from "./pool.ts";
import { Box } from "./box.ts";

Deno.test(function calc() {
  const ctx = new Context();

  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const boxedNumberSpec = {
    provider: boxedNumberPool,
  };

  const add = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ l, r }, { result }) => result.value = l.value + r.value,
  );
  const mul = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ l, r }, { result }) => result.value = l.value * r.value,
  );

  const plan = new Plan(ctx);
  const input1 = input(plan, Box.withValue(1));
  const input2 = input(plan, Box.withValue(2));
  const input3 = input(plan, Box.withValue(3));
  const input4 = input(plan, Box.withValue(4));
  const input5 = input(plan, Box.withValue(5));
  const { result: result1 } = add(plan, { l: input1, r: input2 });
  const { result: result2 } = add(plan, { l: input3, r: input4 });
  const { result: result3 } = mul(plan, { l: result1, r: result2 });
  const { result } = add(plan, { l: result3, r: input5 });

  const resultBody = new Box<number>();
  run(plan, { result: { handle: result, body: resultBody } }, {
    assertNoLeak: true,
  });

  assertEquals(resultBody.value, 26);
  assertEquals(boxedNumberPool.acquiredCount, 0);
  assertGreater(boxedNumberPool.pooledCount, 0);
  assertEquals(boxedNumberPool.taintedCount, 0);
  assertFalse(errorReported);
});

Deno.test(function empty() {
  const ctx = new Context();

  const plan = new Plan(ctx);
  run(plan, {}, { assertNoLeak: true });
});

Deno.test(async function twoOutputs(t) {
  const ctx = new Context();

  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const boxedNumberSpec = {
    provider: boxedNumberPool,
  };

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const divmod = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { div: { type: boxedNumberSpec }, mod: { type: boxedNumberSpec } },
    ({ l, r }, { div, mod }) => {
      div.value = Math.floor(l.value / r.value);
      mod.value = l.value % r.value;
    },
  );
  const add = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ l, r }, { result }) => result.value = l.value + r.value,
  );

  await t.step(function bothOutputsAreGlobal() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(5));
    const { div, mod } = divmod(plan, { l: input1, r: input2 });

    const divBody = new Box<number>();
    const modBody = new Box<number>();
    run(plan, {
      div: { handle: div, body: divBody },
      mod: { handle: mod, body: modBody },
    }, {
      assertNoLeak: true,
    });

    assertEquals(divBody.value, 8);
    assertEquals(modBody.value, 2);
    assertPostCondition();
  });

  await t.step(function divOutputIsGlobal() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(5));
    const { div } = divmod(plan, { l: input1, r: input2 });

    const divBody = new Box<number>();
    run(plan, {
      div: { handle: div, body: divBody },
    }, {
      assertNoLeak: true,
    });

    assertEquals(divBody.value, 8);
    assertPostCondition();
  });

  await t.step(function modOutputIsIntermediate() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(5));
    const input3 = input(plan, Box.withValue(100));
    const { mod } = divmod(plan, { l: input1, r: input2 });
    const { result } = add(plan, { l: mod, r: input3 });

    const resultBody = new Box<number>();
    run(plan, {
      result: { handle: result, body: resultBody },
    }, {
      assertNoLeak: true,
    });

    assertEquals(resultBody.value, 102);
    assertPostCondition();
  });
});

Deno.test(async function outputUsage(t) {
  const ctx = new Context();

  let errorReported = false;

  const boxedNumberPool = new Pool<Box<number>>(
    () => new Box<number>(),
    (x) => x.clear(),
    (e) => {
      errorReported = true;
      console.error(e);
    },
  );
  const boxedNumberSpec = {
    provider: boxedNumberPool,
  };

  function assertPostCondition() {
    assertEquals(boxedNumberPool.acquiredCount, 0);
    assertGreaterOrEqual(boxedNumberPool.pooledCount, 0);
    assertEquals(boxedNumberPool.taintedCount, 0);
    assertFalse(errorReported);
  }

  const add = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ l, r }, { result }) => result.value = l.value + r.value,
  );
  const mul = action(
    ctx,
    { l: { type: boxedNumberSpec }, r: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ l, r }, { result }) => result.value = l.value * r.value,
  );

  await t.step(function noOutputsAreUsed() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(5));
    add(plan, { l: input1, r: input2 });
    mul(plan, { l: input1, r: input2 });

    run(plan, {}, {
      assertNoLeak: true,
    });

    assertPostCondition();
  });

  await t.step(function outputIsUsedTwice() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(2));
    const input3 = input(plan, Box.withValue(3));
    const input4 = input(plan, Box.withValue(4));
    const { result: sum } = add(plan, { l: input1, r: input2 });
    const { result: result1 } = mul(plan, { l: sum, r: input3 });
    const { result: result2 } = add(plan, { l: sum, r: input4 });

    const result1Body = new Box<number>();
    const result2Body = new Box<number>();
    run(plan, {
      result1: { handle: result1, body: result1Body },
      result2: { handle: result2, body: result2Body },
    }, {
      assertNoLeak: true,
    });

    assertPostCondition();
    assertEquals(result1Body.value, 132);
    assertEquals(result2Body.value, 48);
  });

  await t.step(function globalOutputIsUsedAsInput() {
    const plan = new Plan(ctx);
    const input1 = input(plan, Box.withValue(42));
    const input2 = input(plan, Box.withValue(2));
    const input3 = input(plan, Box.withValue(3));
    const { result: sum } = add(plan, { l: input1, r: input2 });
    const { result } = mul(plan, { l: sum, r: input3 });

    const sumBody = new Box<number>();
    const resultBody = new Box<number>();
    run(plan, {
      peek: { handle: sum, body: sumBody },
      result1: { handle: result, body: resultBody },
    }, {
      assertNoLeak: true,
    });

    assertPostCondition();
    assertEquals(sumBody.value, 44);
    assertEquals(resultBody.value, 132);
  });
});
