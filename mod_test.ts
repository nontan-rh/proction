import { assertEquals } from "https://deno.land/std@0.217.0/assert/mod.ts";
import { action, Context, input, Plan, run } from "./mod.ts";
import { Pool } from "./pool.ts";
import { Box } from "./box.ts";

Deno.test(function calcTest() {
  const ctx = new Context();

  const boxedNumberSpec = {
    provider: new Pool<Box<number>>(
      () => new Box<number>(),
      (x) => x.clear(),
      console.error,
    ),
  };

  const add = action(
    ctx,
    { x: { type: boxedNumberSpec }, y: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ x, y }, { result }) => result.value = x.value + y.value,
  );
  const mul = action(
    ctx,
    { x: { type: boxedNumberSpec }, y: { type: boxedNumberSpec } },
    { result: { type: boxedNumberSpec } },
    ({ x, y }, { result }) => result.value = x.value * y.value,
  );

  const plan = new Plan(ctx);
  const input1 = input(plan, Box.withValue(1));
  const input2 = input(plan, Box.withValue(2));
  const input3 = input(plan, Box.withValue(3));
  const input4 = input(plan, Box.withValue(4));
  const input5 = input(plan, Box.withValue(5));
  const { result: result1 } = add(plan, { x: input1, y: input2 });
  const { result: result2 } = add(plan, { x: input3, y: input4 });
  const { result: result3 } = mul(plan, { x: result1, y: result2 });
  const { result } = add(plan, { x: result3, y: input5 });

  const resultBody = new Box<number>();
  run(plan, { result: { handle: result, body: resultBody } });

  assertEquals(resultBody.value, 26);
});
