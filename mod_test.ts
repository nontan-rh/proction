import { assertEquals } from "https://deno.land/std@0.217.0/assert/mod.ts";
import { Context, deferred, input, Plan, run } from "./mod.ts";

Deno.test(function calcTest() {
  const ctx = new Context();

  const deferredAdd = deferred(
    ctx,
    (
      { x, y }: { x: number; y: number },
    ): { result: number } => ({
      result: x + y,
    }),
    { result: 1 },
  );
  const deferredMul = deferred(
    ctx,
    (
      { x, y }: { x: number; y: number },
    ): { result: number } => ({
      result: x * y,
    }),
    { result: 1 },
  );

  const plan = new Plan(ctx);
  const input1 = input(plan, 1);
  const input2 = input(plan, 2);
  const input3 = input(plan, 3);
  const input4 = input(plan, 4);
  const input5 = input(plan, 5);
  const { result: result1 } = deferredAdd(plan, { x: input1, y: input2 });
  const { result: result2 } = deferredAdd(plan, { x: input3, y: input4 });
  const { result: result3 } = deferredMul(plan, { x: result1, y: result2 });
  const { result } = deferredAdd(plan, { x: result3, y: input5 });

  const { result: resultValue } = run(plan, { result });

  assertEquals(resultValue, 26);
});
