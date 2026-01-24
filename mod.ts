/**
 * An ergonomic, resource-aware, dataflow processing library for general-purpose
 *
 * Proction is a utility library for versatile dataflow-based tasks that provides:
 *
 * - Fine-grained resource management
 * - Intuitive interface similar to regular programming
 * - Good integration with externally managed resources
 * - Type-agnostic data handling beyond numeric vectors/tensors
 * - Highly customizable scheduling and parallelism
 *
 * Each feature is provided in a modular, customizable way, and you can combine
 * them as you like.
 *
 * @example
 *
 * ```ts
 * interface ArrayPool {
 *   acquire(length: number): number[];
 *   release(obj: number[]): void;
 * }
 * const pool: ArrayPool = {}!; // some implementation
 * const provide = provider((x) => pool.acquire(x), (x) => pool.release(x));
 *
 * const addProc = proc(function add(output: number[], lht: number[], rht: number[]) {
 *   for (let i = 0; i < output.length; i++) {
 *     output[i] = lht[i] + rht[i];
 *   }
 * });
 *
 * const addFunc = toFunc(addProc, (lht, _rht) => provide(lht.length));
 *
 * const ctx = new Context();
 * async function sum(output: number[], a: number[], b: number[], c: number[]) {
 *   await run(ctx, ({ $s, $d }) => {
 *     const s = addFunc($s(a), $s(b));
 *     addProc($d(output), s, $s(c));
 *   });
 *   // Now `output` stores the result!
 * }
 * ```
 *
 * @module
 */

import {
  AssertionError,
  LogicError,
  PreconditionError,
  unreachable,
} from "./_error.ts";
import type { Brand } from "./_brand.ts";
import type { DisposableWrap } from "./_provider.ts";
import { DelayedRc } from "./_delayedrc.ts";
import { idGenerator } from "./_idgenerator.ts";
import { defaultScheduler, type Scheduler } from "./_scheduler.ts";
export type {
  AcquireFn,
  DisposableWrap,
  ProvideFn,
  ReleaseFn,
} from "./_provider.ts";
export { provider } from "./_provider.ts";
export type { Scheduler } from "./_scheduler.ts";
export { defaultScheduler } from "./_scheduler.ts";

/**
 * An internal symbol used for the key of the parent plan in a handle.
 */
const parentPlanKey = Symbol("parentPlan");
/**
 * An internal symbol used for the key of the handle ID in a handle.
 */
const handleIdKey = Symbol("handleId");
/**
 * An internal symbol used for the key of the phantom data which is used to store the type of the held data in a handle.
 */
const phantomDataKey = Symbol("phantomData");
/**
 * An internal type to identify a handle.
 */
export type HandleId = Brand<number, "handleID">;
/**
 * An indirect handle to a resource.
 * @typeparam T The type of the held data.
 */
export type Handle<T> = {
  /**
   * An underlying plan that the handle belongs to.
   */
  [parentPlanKey]: Plan;
  /**
   * The handle ID.
   */
  [handleIdKey]: HandleId;
  /**
   * Phantom field to retain the generic type. Not used at runtime; do not call.
   */
  [phantomDataKey]: () => T;
};
/**
 * An indirect handle that holds unknown data.
 */
export type UntypedHandle = Handle<unknown>;

/**
 * A utility type to map each element of an object to a handle type.
 * @typeparam T The object type to convert.
 * @returns The converted object type.
 */
type MappedHandleType<T> = {
  [key in keyof T]: Handle<T[key]>;
};
/**
 * A utility type to map each element of an object to a body type.
 * @typeparam T The object type to convert.
 * @returns The converted object type.
 */
type MappedBodyType<T> = {
  [key in keyof T]: BodyType<T[key]>;
};
/**
 * A utility type to get a body type of a handle.
 * @typeparam T The handle type to get the body type of.
 * @returns The body type of the handle.
 */
type BodyType<T> = T extends Handle<infer X> ? X : never;

/**
 * An internal function to check if an object is a handle.
 * @param x The object to check.
 * @returns True if the object is a handle, false otherwise.
 */
function isHandle(x: object): x is UntypedHandle {
  return parentPlanKey in x;
}

/**
 * Gets the plan from one or more handles.
 * It is often used to validate that all handles share the same plan (e.g., when wiring indirect routines).
 * @param handles One or more handles (or arrays of handles).
 * @returns The plan that the provided handles belong to.
 * @throws PreconditionError If handles belong to different plans or no handles are provided.
 */
export function getPlan(
  ...handles: (
    | UntypedHandle
    | readonly UntypedHandle[]
  )[]
): Plan {
  let plan: Plan | undefined;
  for (const t of handles) {
    if (isHandle(t)) {
      const p = t[parentPlanKey];
      if (plan == null) {
        plan = p;
      } else if (p !== plan) {
        throw new PreconditionError("Plan inconsistent");
      }
    } else {
      for (const h of t) {
        const p = h[parentPlanKey];
        if (plan == null) {
          plan = p;
        } else if (p !== plan) {
          throw new PreconditionError("Plan inconsistent");
        }
      }
    }
  }

  if (plan == null) {
    throw new PreconditionError("Failed to detect plan");
  }

  return plan;
}

/**
 * An internal type to represent an invocation.
 */
type InvocationBodyFn = () => Promise<void>;
/**
 * A type to represent a middleware function.
 * @param next Invoke to run the next middleware or the underlying body.
 */
export type MiddlewareFn = (next: () => Promise<void>) => Promise<void>;

/**
 * A type to represent the options of a proc.
 */
export type ProcOptions = {
  /**
   * The middlewares to apply to the indirect procedure.
   */
  middlewares?: MiddlewareFn[];
};

// NOTE: The order of preparing outputs and restoring inputs is important,
// especially for in-place routines.
//
// `transferInOut()` extracts the managed object from `inHandle`'s DelayedRc,
// invalidating the input handle (it becomes "freed"). After this point,
// any later attempt to `restore(plan, inHandle)` will throw.
//
// This matters because "output preparation" may indirectly restore inputs.
// For example, outputs created by `toFunc()`/`toFuncN()` have provide closures
// that call `restoreInputs(plan, inputs)` to allocate the output objects.
// That restoration can include the to-be-transferred input handle, even
// if the provide function ignores that parameter.
//
// Therefore, for in-place bodies that use `transferInOut()`:
// - prepare any other outputs that might restore `inHandle` first
// - restore other inputs next
// - call `transferInOut` last, right before invoking user code

/**
 * Creates an indirect procedure which has a single output.
 * @typeparam O The output type of the indirect procedure.
 * @typeparam I The list of input types of the indirect procedure.
 * @param f The body function of the indirect procedure.
 * @param procOptions The options of the proc.
 * @returns An indirect procedure.
 */
export function proc<O, I extends readonly unknown[]>(
  f: (output: O, ...inputs: I) => void | Promise<void>,
  procOptions?: ProcOptions,
): (
  output: Handle<O>,
  ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  const g = (
    output: Handle<O>,
    ...inputs: MappedHandleType<I>
  ) => {
    const plan = getPlan(output, ...inputs);

    const id = plan[internalPlanKey].generateInvocationID();
    const body = async () => {
      try {
        const restoredInputs = restoreInputs(plan, inputs);
        const preparedOutputs = prepareOutput(plan, output);
        await f(preparedOutputs, ...restoredInputs);
      } finally {
        decRefArray(plan, inputs);
        decRef(plan, output);
      }
    };
    const invocation: Invocation = {
      id,
      inputs,
      outputs: [output],
      resolveBody: () => applyMiddlewares(body, middlewares),
      // calculated on run preparation
      next: [],
      numBlockers: 0,
      numResolvedBlockers: 0,
      body: null,
    };
    plan[internalPlanKey].invocations.set(invocation.id, invocation);
  };

  return g;
}

/**
 * Creates an indirect procedure which has a single output combining an in-place
 * implementation and an out-of-place implementation.
 * @typeparam IO The first input type and output type of the indirect procedure.
 * @typeparam I The list of rest input types of the indirect procedure.
 * @param fOutOfPlace The body function of the indirect out-of-place procedure.
 * @param fInPlace The body function of the indirect in-place procedure.
 * @param procOptions The options of the proc.
 * @returns An indirect procedure.
 */
export function procI<IO, I extends readonly unknown[]>(
  fOutOfPlace: (
    output: IO,
    input0: IO,
    ...restInputs: I
  ) => void | Promise<void>,
  fInPlace: (inout: IO, ...restInputs: I) => void | Promise<void>,
  procOptions?: ProcOptions,
): (
  output: Handle<IO>,
  input0: Handle<IO>,
  ...restInputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  const g = (
    output: Handle<IO>,
    input0: Handle<IO>,
    ...restInputs: MappedHandleType<I>
  ) => {
    const plan = getPlan(output, input0, ...restInputs);

    const id = plan[internalPlanKey].generateInvocationID();
    const bodyOutOfPlace = async () => {
      try {
        const restoredInput0 = restore(plan, input0);
        const restoredRestInputs = restoreInputs(plan, restInputs);
        const preparedOutput = prepareOutput(plan, output);
        await fOutOfPlace(
          preparedOutput,
          restoredInput0,
          ...restoredRestInputs,
        );
      } finally {
        decRef(plan, input0);
        decRefArray(plan, restInputs);
        decRef(plan, output);
      }
    };

    const bodyInPlace = async () => {
      let transferring = false;
      try {
        const restoredRestInputs = restoreInputs(plan, restInputs);
        transferring = true;
        const restoredInOut0 = transferInOut(plan, input0, output);
        await fInPlace(restoredInOut0, ...restoredRestInputs);
      } finally {
        // If transfer did not happen, we still need to release input0.
        if (!transferring) {
          decRef(plan, input0);
        }
        decRefArray(plan, restInputs);
        decRef(plan, output);
      }
    };

    const resolveBody = (ctx: ResolveContext): () => Promise<void> => {
      const input0Count = ctx.inputConsumerCounts.get(input0[handleIdKey]);
      if (input0Count == null) {
        throw new LogicError(
          `the input consumer count for input handle is not calculated: ${input0}`,
        );
      }

      if (input0Count === 1) {
        const input0Slot = ctx.plan[internalPlanKey].dataSlots.get(
          input0[handleIdKey],
        );
        if (input0Slot == null) {
          throw new LogicError(`dataSlot not found for handle: ${input0}`);
        }
        const outputSlot = ctx.plan[internalPlanKey].dataSlots.get(
          output[handleIdKey],
        );
        if (outputSlot == null) {
          throw new LogicError(`dataSlot not found for handle: ${output}`);
        }

        if (
          input0Slot.type === "intermediate" &&
          outputSlot.type === "intermediate"
        ) {
          return applyMiddlewares(bodyInPlace, middlewares);
        }
      }
      return applyMiddlewares(bodyOutOfPlace, middlewares);
    };

    const invocation: Invocation = {
      id,
      inputs: [input0, ...restInputs],
      outputs: [output],
      resolveBody,
      // calculated on run preparation
      next: [],
      numBlockers: 0,
      numResolvedBlockers: 0,
      body: null,
    };
    plan[internalPlanKey].invocations.set(invocation.id, invocation);
  };

  return g;
}

/**
 * Creates an indirect procedure which has multiple outputs.
 * @typeparam O The list of output types of the indirect procedure.
 * @typeparam I The list of input types of the indirect procedure.
 * @param f The body function of the indirect procedure.
 * @param procOptions The options of the proc.
 * @returns An indirect procedure.
 */
export function procN<
  O extends readonly unknown[],
  I extends readonly unknown[],
>(
  f: (outputs: O, ...inputs: I) => void | Promise<void>,
  procOptions?: ProcOptions,
): (
  outputs: { [key in keyof O]: Handle<O[key]> }, // expanded for readability of inferred type
  ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  const g = (
    outputs: MappedHandleType<O>,
    ...inputs: MappedHandleType<I>
  ) => {
    const plan = getPlan(outputs, ...inputs);

    const id = plan[internalPlanKey].generateInvocationID();
    const body = async () => {
      try {
        const restoredInputs = restoreInputs(plan, inputs);
        const preparedOutputs = prepareMultipleOutput(plan, outputs);
        await f(preparedOutputs, ...restoredInputs);
      } finally {
        decRefArray(plan, inputs);
        decRefArray(plan, outputs);
      }
    };
    const invocation: Invocation = {
      id,
      inputs,
      outputs,
      resolveBody: () => applyMiddlewares(body, middlewares),
      // calculated on run preparation
      next: [],
      numBlockers: 0,
      numResolvedBlockers: 0,
      body: null,
    };
    plan[internalPlanKey].invocations.set(invocation.id, invocation);
  };

  return g;
}

/**
 * Creates an indirect procedure which has multiple outputs combining an in-place
 * implementation and an out-of-place implementation.
 * The first input and first output can be processed in-place when conditions are met.
 * @typeparam IO The first input type and first output type of the indirect procedure.
 * @typeparam O The list of rest output types of the indirect procedure.
 * @typeparam I The list of rest input types of the indirect procedure.
 * @param fOutOfPlace The body function of the indirect out-of-place procedure.
 * @param fInPlace The body function of the indirect in-place procedure.
 * @param procOptions The options of the proc.
 * @returns An indirect procedure.
 */
export function procNI1<
  IO,
  O extends readonly unknown[],
  I extends readonly unknown[],
>(
  fOutOfPlace: (
    outputs: [IO, ...O],
    input0: IO,
    ...restInputs: I
  ) => void | Promise<void>,
  fInPlace: (
    inout: IO,
    restOutputs: O,
    ...restInputs: I
  ) => void | Promise<void>,
  procOptions?: ProcOptions,
): (
  outputs: [Handle<IO>, ...{ [key in keyof O]: Handle<O[key]> }], // expanded for readability of inferred type
  input0: Handle<IO>,
  ...restInputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  const g = (
    outputs: [Handle<IO>, ...MappedHandleType<O>],
    input0: Handle<IO>,
    ...restInputs: MappedHandleType<I>
  ) => {
    const plan = getPlan(outputs, input0, ...restInputs);

    const output0 = outputs[0];
    const restOutputs = outputs.slice(1) as MappedHandleType<O>;

    const id = plan[internalPlanKey].generateInvocationID();
    const bodyOutOfPlace = async () => {
      try {
        const restoredInput0 = restore(plan, input0);
        const restoredRestInputs = restoreInputs(plan, restInputs);
        const preparedOutputs = prepareMultipleOutput(plan, outputs) as [
          IO,
          ...O,
        ];
        await fOutOfPlace(
          preparedOutputs,
          restoredInput0,
          ...restoredRestInputs,
        );
      } finally {
        decRef(plan, input0);
        decRefArray(plan, restInputs);
        decRefArray(plan, outputs);
      }
    };

    const bodyInPlace = async () => {
      let transferring = false;
      try {
        const preparedRestOutputs = prepareMultipleOutput(
          plan,
          restOutputs,
        ) as O;
        const restoredRestInputs = restoreInputs(plan, restInputs);
        transferring = true;
        const restoredInOut0 = transferInOut(plan, input0, output0);
        await fInPlace(
          restoredInOut0,
          preparedRestOutputs,
          ...restoredRestInputs,
        );
      } finally {
        // If transfer did not happen, we still need to release input0.
        if (!transferring) {
          decRef(plan, input0);
        }
        decRefArray(plan, restInputs);
        decRefArray(plan, outputs);
      }
    };

    const resolveBody = (ctx: ResolveContext): () => Promise<void> => {
      const input0Count = ctx.inputConsumerCounts.get(input0[handleIdKey]);
      if (input0Count == null) {
        throw new LogicError(
          `the input consumer count for input handle is not calculated: ${input0}`,
        );
      }

      if (input0Count === 1) {
        const input0Slot = ctx.plan[internalPlanKey].dataSlots.get(
          input0[handleIdKey],
        );
        if (input0Slot == null) {
          throw new LogicError(`dataSlot not found for handle: ${input0}`);
        }
        const output0Slot = ctx.plan[internalPlanKey].dataSlots.get(
          output0[handleIdKey],
        );
        if (output0Slot == null) {
          throw new LogicError(`dataSlot not found for handle: ${output0}`);
        }

        if (
          input0Slot.type === "intermediate" &&
          output0Slot.type === "intermediate"
        ) {
          return applyMiddlewares(bodyInPlace, middlewares);
        }
      }
      return applyMiddlewares(bodyOutOfPlace, middlewares);
    };

    const invocation: Invocation = {
      id,
      inputs: [input0, ...restInputs],
      outputs,
      resolveBody,
      // calculated on run preparation
      next: [],
      numBlockers: 0,
      numResolvedBlockers: 0,
      body: null,
    };
    plan[internalPlanKey].invocations.set(invocation.id, invocation);
  };

  return g;
}

/**
 * Creates an indirect procedure which has multiple outputs combining an in-place
 * implementation and an out-of-place implementation.
 * All input-output pairs specified by `IO` can be processed in-place when all inputs are
 * not referenced from other places.
 * @typeparam IO A tuple type representing input-output pairs that can be processed in-place.
 * @typeparam I The tuple type of additional input types that are not processed in-place.
 * @param fOutOfPlace The body function of the out-of-place procedure.
 * @param fInPlace The body function of the in-place procedure.
 * @param procOptions The options of the proc.
 * @returns An indirect procedure.
 */
export function procNIAll<
  IO extends readonly unknown[],
  I extends readonly unknown[],
>(
  fOutOfPlace: (
    outputs: IO,
    ...inputs: [...IO, ...I]
  ) => void | Promise<void>,
  fInPlace: (
    inout: IO,
    ...restInputs: I
  ) => void | Promise<void>,
  procOptions?: ProcOptions,
): (
  outputs: { [key in keyof IO]: Handle<IO[key]> }, // expanded for readability of inferred type
  ...restInputs: [
    ...{ [key in keyof IO]: Handle<IO[key]> },
    ...{ [key in keyof I]: Handle<I[key]> },
  ] // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  const g = (
    outputs: MappedHandleType<IO>,
    ...restInputs: [...MappedHandleType<IO>, ...MappedHandleType<I>]
  ) => {
    const plan = getPlan(outputs, ...restInputs);

    const ioLength = outputs.length;
    const ioInputs = restInputs.slice(0, ioLength) as MappedHandleType<IO>;
    const additionalInputs = restInputs.slice(ioLength) as MappedHandleType<I>;

    const id = plan[internalPlanKey].generateInvocationID();

    const bodyOutOfPlace = async () => {
      try {
        const restoredIoInputs = restoreInputs(plan, ioInputs);
        const restoredAdditionalInputs = restoreInputs(plan, additionalInputs);
        const preparedOutputs = prepareMultipleOutput(plan, outputs) as IO;
        await fOutOfPlace(
          preparedOutputs,
          ...(restoredIoInputs as [...IO]),
          ...(restoredAdditionalInputs as [...I]),
        );
      } finally {
        decRefArray(plan, ioInputs);
        decRefArray(plan, additionalInputs);
        decRefArray(plan, outputs);
      }
    };

    const bodyInPlace = async () => {
      let transferringCount = 0;
      try {
        const restoredAdditionalInputs = restoreInputs(plan, additionalInputs);
        const restoredInOuts = [];
        for (let i = 0; i < ioLength; i++) {
          transferringCount++;
          restoredInOuts.push(transferInOut(plan, ioInputs[i], outputs[i]));
        }
        await fInPlace(
          restoredInOuts as unknown as IO,
          ...(restoredAdditionalInputs as [...I]),
        );
      } finally {
        // If some transfers did not happen, we still need to release those inputs.
        for (let i = transferringCount; i < ioLength; i++) {
          decRef(plan, ioInputs[i]);
        }
        decRefArray(plan, additionalInputs);
        decRefArray(plan, outputs);
      }
    };

    const resolveBody = (ctx: ResolveContext): () => Promise<void> => {
      let canInPlace = true;

      for (let i = 0; i < ioLength; i++) {
        const ioInput = ioInputs[i];
        const ioOutput = outputs[i];

        const inputCount = ctx.inputConsumerCounts.get(ioInput[handleIdKey]);
        if (inputCount == null) {
          throw new LogicError(
            `the input consumer count for input handle is not calculated: ${ioInput}`,
          );
        }

        if (inputCount !== 1) {
          canInPlace = false;
          break;
        }

        const inputSlot = ctx.plan[internalPlanKey].dataSlots.get(
          ioInput[handleIdKey],
        );
        if (inputSlot == null) {
          throw new LogicError(`dataSlot not found for handle: ${ioInput}`);
        }

        const outputSlot = ctx.plan[internalPlanKey].dataSlots.get(
          ioOutput[handleIdKey],
        );
        if (outputSlot == null) {
          throw new LogicError(`dataSlot not found for handle: ${ioOutput}`);
        }

        if (
          inputSlot.type !== "intermediate" ||
          outputSlot.type !== "intermediate"
        ) {
          canInPlace = false;
          break;
        }
      }

      if (canInPlace) {
        return applyMiddlewares(bodyInPlace, middlewares);
      }
      return applyMiddlewares(bodyOutOfPlace, middlewares);
    };

    const invocation: Invocation = {
      id,
      inputs: [...ioInputs, ...additionalInputs],
      outputs: outputs as unknown as UntypedHandle[],
      resolveBody,
      next: [],
      numBlockers: 0,
      numResolvedBlockers: 0,
      body: null,
    };
    plan[internalPlanKey].invocations.set(invocation.id, invocation);
  };

  return g;
}

/**
 * Converts an indirect procedure which has a single output to an indirect function.
 * @typeparam O The output type of the indirect procedure and the return type of the indirect function.
 * @typeparam I The list of input types of the indirect procedure and the indirect function.
 * @typeparam A The list of argument types of the provide function. They are also the type of objects created by the provide functions.
 * @param indirectProcedure The indirect procedure to convert.
 * @param provide The provide function attached to the indirect function.
 * @returns The converted indirect function.
 */
export function toFunc<
  O,
  I extends readonly UntypedHandle[],
  A extends O,
>(
  indirectProcedure: (
    output: Handle<O>,
    ...inputs: I
  ) => void,
  provide: (
    ...inputs: { [key in keyof I]: I[key] extends Handle<infer X> ? X : never }
  ) => DisposableWrap<A>,
): (
  ...inputs: I
) => Handle<A> {
  return (...inputs: I): Handle<A> => {
    const plan = getPlan(...inputs);
    const output = intermediate(
      plan,
      () => provide(...restoreInputs(plan, inputs)),
    );
    indirectProcedure(output, ...inputs);
    return output;
  };
}

/**
 * Converts an indirect procedure which has multiple outputs to an indirect function.
 * @typeparam O The list of output types of the indirect procedure and the return type of the indirect function.
 * @typeparam I The list of input types of the indirect procedure and the indirect function.
 * @typeparam A The list of argument types of the provide function. They are also the type of objects created by the provide functions.
 * @param indirectProcedure The indirect procedure to convert.
 * @param provideFns The provide functions attached to the indirect function.
 * @returns The converted indirect function.
 */
export function toFuncN<
  O extends readonly unknown[],
  I extends readonly UntypedHandle[],
  A extends O,
>(
  indirectProcedure: (
    outputs: { [key in keyof O]: Handle<O[key]> },
    ...inputs: I
  ) => void,
  provideFns: {
    [key in keyof O]: (
      ...inputs: {
        [key in keyof I]: I[key] extends Handle<infer X> ? X : never;
      }
    ) => DisposableWrap<A[key]>;
  },
): (
  ...inputs: I
) => { [key in keyof O]: Handle<A[key]> } // expanded for readability of inferred type
{
  return (...inputs: I): MappedHandleType<O> => {
    const plan = getPlan(...inputs);

    const partialOutputs = [];
    for (let i = 0; i < provideFns.length; i++) {
      const provide = provideFns[i];
      const handle = intermediate(
        plan,
        () => provide(...restoreInputs(plan, inputs)),
      );
      partialOutputs.push(handle);
    }
    const outputs = partialOutputs as MappedHandleType<O>;

    indirectProcedure(outputs, ...inputs);

    return outputs;
  };
}

/**
 * An internal type to represent an invocation ID.
 */
type InvocationID = Brand<number, "invocationID">;
/**
 * An internal type used for body resolution.
 */
interface ResolveContext {
  readonly plan: Plan;
  readonly inputConsumerCounts: Map<HandleId, number>;
}
/**
 * An internal type to represent an invocation. Invocation represents a running procedure or function.
 * It is the unit of execution of a Proction program.
 */
interface Invocation {
  readonly id: InvocationID;
  readonly inputs: readonly UntypedHandle[];
  readonly outputs: readonly UntypedHandle[];
  readonly resolveBody: (context: ResolveContext) => () => Promise<void>;
  readonly next: Invocation[];
  numBlockers: number;
  numResolvedBlockers: number;
  body: (() => Promise<void>) | null;
}

/**
 * An internal symbol used for the key of the context options in a context.
 */
const contextOptionsKey = Symbol("contextOptions");

/**
 * A context for a Proction program. It is expected to live some long span in an application.
 */
export class Context {
  /**
   * The options of the context.
   */
  [contextOptionsKey]: ContextOptions;

  /**
   * Creates a new context.
   * @param options The options of the context.
   */
  constructor(options?: Partial<ContextOptions>) {
    const mergedOptions = { ...defaultContextOptions, ...options };

    const reportError = mergedOptions.reportError;
    mergedOptions.reportError = (e) => {
      try {
        reportError(e);
      } catch {
        // no recovery
      }
    };

    this[contextOptionsKey] = mergedOptions;
  }
}

/**
 * Runs a Proction program. Indirect routines are expected to be called within the body function.
 * When the promise is resolved, the program is guaranteed to be finished.
 * @param context The Proction context.
 * @param bodyFn The body function of the Proction program.
 * @returns A promise that resolves when all scheduled invocations are finished.
 */
export async function run(
  context: Context,
  bodyFn: (runContext: RunContext) => void,
) {
  const plan: Plan = {
    context,
    [internalPlanKey]: new InternalPlan(context),
  };
  plan[internalPlanKey].plan = plan;
  const runContext: RunContext = {
    $s: (value) => source(plan, value),
    $d: (value) => destination(plan, value),
    $i: (provide) => intermediate(plan, provide),
  };
  bodyFn(runContext);
  await runPlan(plan);
}

/**
 * A type to represent the options of a context.
 */
export type ContextOptions = {
  /**
   * The function to report an error. In Proction, exceptions are caught and reported by this function.
   */
  reportError: (e: unknown) => void;
  /**
   * Whether to assert no leak. If true, additional assertions are added to the program to ensure that all data slots are freed.
   */
  assertNoLeak: boolean;
  /**
   * The task scheduler
   */
  scheduler: Scheduler;
};

/**
 * The default options of a context.
 */
const defaultContextOptions: ContextOptions = {
  reportError: () => {},
  assertNoLeak: false,
  scheduler: defaultScheduler,
};

/**
 * A type passed to the body function in the run function.
 */
export type RunContext = {
  /**
   * Creates a read-only source handle from an external resource.
   * @typeparam T The type of the external resource.
   * @param value The external resource.
   * @returns The read-only source handle.
   */
  $s<T extends object>(value: T): Handle<T>;
  /**
   * Creates a write-only destination handle from an external resource.
   * @typeparam T The type of the external resource.
   * @param value The external resource.
   * @returns The write-only destination handle.
   */
  $d<T extends object>(value: T): Handle<T>;
  /**
   * Creates an intermediate handle.
   * @typeparam T The type of the provided object.
   * @param provide The provide function.
   * @returns The intermediate handle.
   */
  $i<T>(provide: () => DisposableWrap<T>): Handle<T>;
};

/**
 * An internal function to return undefined.
 */
const undefinedFn = () => {};

/**
 * An internal symbol used for the key of the internal plan in a plan.
 */
const internalPlanKey = Symbol("internalPlan");
/**
 * A type to represent a plan.
 */
export type Plan = {
  /**
   * The context the plan belongs to.
   */
  readonly context: Context;
  /**
   * The private members of the plan.
   */
  [internalPlanKey]: InternalPlan;
};

/**
 * An internal class to hold the private members of a plan.
 */
class InternalPlan {
  context: Context;
  plan: Plan;
  state: PlanState;
  inputCache: WeakMap<object, UntypedHandle>;
  outputCache: WeakMap<object, UntypedHandle>;

  generateHandle: () => UntypedHandle = idGenerator((
    value,
  ) => ({
    [parentPlanKey]: this.plan,
    [handleIdKey]: value as HandleId,
    [phantomDataKey]: undefinedFn,
  }));
  dataSlots: Map<HandleId, DataSlot> = new Map<HandleId, DataSlot>();

  generateInvocationID: () => InvocationID = idGenerator((value) =>
    value as InvocationID
  );
  invocations: Map<InvocationID, Invocation> = new Map<
    InvocationID,
    Invocation
  >();

  constructor(context: Context) {
    this.context = context;
    this.plan = undefined!;
    this.state = "initial";
    this.inputCache = new WeakMap();
    this.outputCache = new WeakMap();
  }
}

/**
 * An internal type to represent the state of a plan.
 */
type PlanState = "initial" | "planning" | "running" | "done" | "error";
/**
 * An internal union type of data slots.
 */
type DataSlot =
  | SourceSlot
  | IntermediateSlot
  | DestinationSlot;
/**
 * An internal type to represent a source slot.
 */
type SourceSlot = { type: "source"; body: unknown };
/**
 * An internal type to represent an intermediate slot.
 */
type IntermediateSlot = {
  type: "intermediate";
  provide: () => DisposableWrap<unknown>;
  disposableWrapContainer: DelayedRc<DisposableWrap<unknown>>;
};
/**
 * An internal type to represent a destination slot.
 */
type DestinationSlot = { type: "destination"; body: unknown };

/**
 * An internal function to create a source handle and a source slot. It is the implementation of $s function.
 * @typeparam T The type of the external resource.
 * @param plan The plan to create the source handle in.
 * @param value The external resource.
 * @returns The source handle.
 */
function source<T extends object>(plan: Plan, value: T): Handle<T> {
  const cached = plan[internalPlanKey].inputCache.get(value);
  if (cached) {
    return cached as Handle<T>;
  }

  // validation
  if (plan[internalPlanKey].outputCache.has(value)) {
    throw new PreconditionError("the value is already specified as output");
  }

  const handle = plan[internalPlanKey].generateHandle();

  plan[internalPlanKey].dataSlots.set(handle[handleIdKey], {
    type: "source",
    body: value,
  });
  plan[internalPlanKey].inputCache.set(value, handle);

  return handle as Handle<T>;
}

/**
 * An internal function to create a destination handle and a destination slot. It is the implementation of $d function.
 * @typeparam T The type of the external resource.
 * @param plan The plan to create the destination handle in.
 * @param value The external resource.
 * @returns The destination handle.
 */
function destination<T extends object>(plan: Plan, value: T): Handle<T> {
  const cached = plan[internalPlanKey].outputCache.get(value);
  if (cached) {
    return cached as Handle<T>;
  }

  // validation
  if (plan[internalPlanKey].inputCache.has(value)) {
    throw new PreconditionError("the value is already specified as input");
  }

  const handle = plan[internalPlanKey].generateHandle();

  plan[internalPlanKey].dataSlots.set(handle[handleIdKey], {
    type: "destination",
    body: value,
  });
  plan[internalPlanKey].outputCache.set(value, handle);

  return handle as Handle<T>;
}

/**
 * An internal function to create an intermediate handle and an intermediate slot. It is the implementation of $i function.
 * @typeparam T The type of the provided object.
 * @param plan The plan to create the intermediate handle in.
 * @param provide The provide function.
 * @returns The intermediate handle.
 */
function intermediate<T>(
  plan: Plan,
  provide: () => DisposableWrap<T>,
): Handle<T> {
  const handle = plan[internalPlanKey].generateHandle();

  plan[internalPlanKey].dataSlots.set(handle[handleIdKey], {
    type: "intermediate",
    disposableWrapContainer: new DelayedRc((x) => {
      x[Symbol.dispose]();
    }, plan.context[contextOptionsKey].reportError),
    provide,
  });

  return handle as Handle<T>;
}

/**
 * An internal function to run a plan.
 * @param plan The plan to run.
 * @returns The promise to run the plan.
 */
async function runPlan(
  plan: Plan,
): Promise<void> {
  if (plan[internalPlanKey].state !== "initial") {
    throw new PreconditionError(
      `invalid state precondition for run(): ${plan[internalPlanKey].state}`,
    );
  }

  try {
    plan[internalPlanKey].state = "planning";

    const runningInvocations = new Set<InvocationID>();
    const freeInvocations = prepareInvocations(plan);
    prepareDataSlots(plan);

    plan[internalPlanKey].state = "running";

    // condvar is for runningInvocations and freeInvocations
    let { promise: condvar, resolve: notify } = Promise.withResolvers<void>();

    while (true) {
      const invocation = freeInvocations.shift();
      if (invocation == null) {
        if (runningInvocations.size === 0) {
          break;
        }

        await condvar;
        ({ promise: condvar, resolve: notify } = Promise.withResolvers<void>());

        continue;
      }

      const scheduler = plan.context[contextOptionsKey].scheduler;
      runningInvocations.add(invocation.id);
      scheduler.spawn(invocation.body!).then(() => {
        for (const next of invocation.next) {
          if (next.numResolvedBlockers >= next.numBlockers) {
            throw new LogicError("the invocation is resolved twice");
          }
          next.numResolvedBlockers++;
          if (next.numResolvedBlockers >= next.numBlockers) {
            freeInvocations.push(next);
          }
        }
        runningInvocations.delete(invocation.id);
        notify();
      }, (_: unknown) => {
        // TODO: Internal errors come here. Notify the error to the user.
        runningInvocations.delete(invocation.id);
        notify();
      });
    }

    ensureAllIntermediateSlotsFreed(plan);

    plan[internalPlanKey].state = "done";
  } finally {
    if (plan[internalPlanKey].state !== "done") {
      plan[internalPlanKey].state = "error";
    }
  }
}

/**
 * An internal function to preprocess invocations before execution.
 * @param plan The plan to prepare invocations for.
 * @returns The prepared invocations.
 */
function prepareInvocations(
  plan: Plan,
): Invocation[] {
  const freeInvocations: Invocation[] = [];

  const outputToInvocation = new Map<HandleId, Invocation>();
  for (const invocation of plan[internalPlanKey].invocations.values()) {
    for (const output of invocation.outputs) {
      if (outputToInvocation.has(output[handleIdKey])) {
        throw new LogicError(
          "the output have two parent invocations",
        );
      }
      outputToInvocation.set(output[handleIdKey], invocation);
    }
  }

  for (const invocation of plan[internalPlanKey].invocations.values()) {
    for (const input of invocation.inputs) {
      const parentInvocation = outputToInvocation.get(input[handleIdKey]);
      if (parentInvocation == null) {
        continue;
      }
      // Input of the invocation depends on its parent invocation

      parentInvocation.next.push(invocation); // allow duplication for proper counting
      invocation.numBlockers++;
    }
  }

  const inputConsumerCounts = new Map<HandleId, number>();
  const resolveContext: ResolveContext = { plan, inputConsumerCounts };
  for (const invocation of plan[internalPlanKey].invocations.values()) {
    for (const input of invocation.inputs) {
      const id = input[handleIdKey];
      inputConsumerCounts.set(id, (inputConsumerCounts.get(id) ?? 0) + 1);
    }
  }
  for (const invocation of plan[internalPlanKey].invocations.values()) {
    invocation.body = invocation.resolveBody(resolveContext);
  }

  for (const invocation of plan[internalPlanKey].invocations.values()) {
    if (invocation.numBlockers === 0) {
      freeInvocations.push(invocation);
    }
  }

  return freeInvocations;
}

/**
 * An internal function to allocate data slots before execution.
 * @param plan The plan to prepare data slots for.
 */
function prepareDataSlots(
  plan: Plan,
): void {
  for (const invocation of plan[internalPlanKey].invocations.values()) {
    // reserve intermediate inputs
    for (const input of invocation.inputs) {
      const dataSlot = plan[internalPlanKey].dataSlots.get(
        input[handleIdKey],
      );
      if (dataSlot == null) {
        throw new LogicError(
          `dataSlot not found for handle: ${input}`,
        );
      }

      const type = dataSlot.type;
      switch (type) {
        case "source":
          break;
        case "intermediate":
          dataSlot.disposableWrapContainer.incRef();
          break;
        case "destination":
          break;
        default:
          return unreachable(type);
      }
    }
  }
}

/**
 * An internal function to create actual argument list from handles before the execution of an invocation.
 * @typeparam T The type of the argument list.
 * @param plan The plan the handles belong to.
 * @param argHandles The handles of inputs.
 * @returns The restored inputs.
 */
function restoreInputs<T extends readonly UntypedHandle[]>(
  plan: Plan,
  argHandles: T,
): MappedBodyType<T> {
  const restored = [];
  for (const argHandle of argHandles) {
    restored.push(restore(plan, argHandle));
  }
  return restored as MappedBodyType<T>;
}

/**
 * An internal function to restore an actual argument from a handle.
 * @typeparam T The type of the argument.
 * @param plan The plan the handle belongs to.
 * @param handle The handle of an input.
 * @returns The restored inputs.
 */
function restore<T>(plan: Plan, handle: Handle<T>): T {
  const dataSlot = plan[internalPlanKey].dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new LogicError(
      `datum not saved for handle: ${handle}`,
    );
  }

  const type = dataSlot.type;
  switch (type) {
    case "source": {
      const body = dataSlot.body;
      return body as T;
    }
    case "intermediate":
      return dataSlot.disposableWrapContainer.managedObject.body as T;
    case "destination": {
      const body = dataSlot.body;
      return body as T;
    }
    default:
      return unreachable(type);
  }
}

/**
 * An internal function to restore an actual argument from a handle and transfer the content.
 * @typeparam T The type of the argument.
 * @param plan The plan the handle belongs to.
 * @param inHandle The handle of an input.
 * @param outHandle The handle of an output.
 * @returns The restored input and prepared output.
 */
function transferInOut<T>(
  plan: Plan,
  inHandle: Handle<T>,
  outHandle: Handle<T>,
): T {
  const inDataSlot = plan[internalPlanKey].dataSlots.get(inHandle[handleIdKey]);
  if (inDataSlot == null) {
    throw new LogicError(`datum not saved for handle: ${inHandle}`);
  }
  const inType = inDataSlot.type;
  if (inType !== "intermediate") {
    throw new LogicError(`unexpected data slot type: ${inType}`);
  }
  const outDataSlot = plan[internalPlanKey].dataSlots.get(
    outHandle[handleIdKey],
  );
  if (outDataSlot == null) {
    throw new LogicError(`datum not saved for handle: ${outHandle}`);
  }
  const outType = outDataSlot.type;
  if (outType !== "intermediate") {
    throw new LogicError(`unexpected data slot type: ${outType}`);
  }

  const disposableWrap = inDataSlot.disposableWrapContainer.extract();
  // NOTE: `transferInOut()` is not expected to fail in normal operation:
  // any failure here indicates an internal invariant violation (LogicError),
  // and we intentionally bail out rather than attempting recovery.
  //
  // However, after `extract()` succeeds, ownership of the DisposableWrap is no longer
  // recorded in any data slot. If `initialize()` throws (e.g. the output container is already
  // initialized/freed), the final plan sweep cannot dispose this object. So we dispose it here
  // to avoid leaking resources, then rethrow.
  try {
    outDataSlot.disposableWrapContainer.initialize(disposableWrap);
  } catch (e: unknown) {
    try {
      disposableWrap[Symbol.dispose]();
    } catch (disposeError: unknown) {
      try {
        plan.context[contextOptionsKey].reportError(disposeError);
      } catch {
        // cannot recover
      }
    }
    throw e;
  }

  return disposableWrap.body as T;
}

/**
 * An internal function to decrement the reference count of an array of handles used for inputs.
 * @param plan The plan the handles belong to.
 * @param handles The handles to decrement the reference count of.
 */
function decRefArray(plan: Plan, handles: readonly UntypedHandle[]): void {
  for (const handle of handles) {
    decRef(plan, handle);
  }
}

/**
 * An internal function to decrement the reference count of a handle.
 * @param plan The plan the handle belongs to.
 * @param handle The handle to decrement the reference count of.
 */
function decRef(plan: Plan, handle: UntypedHandle): void {
  const dataSlot = plan[internalPlanKey].dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new LogicError(
      `data slot not saved for handle: ${handle}`,
    );
  }

  const type = dataSlot.type;
  switch (type) {
    case "source":
      break;
    case "intermediate":
      dataSlot.disposableWrapContainer.decRef();
      break;
    case "destination":
      break;
    default:
      return unreachable(type);
  }
}

/**
 * An internal function to prepare an output for the execution of an invocation.
 * @typeparam T The type of the output.
 * @param plan The plan the handle belongs to.
 * @param handle The handle of an output.
 * @returns The prepared output.
 */
function prepareOutput<T>(
  plan: Plan,
  handle: Handle<T>,
): T {
  const dataSlot = plan[internalPlanKey].dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new LogicError("data slot not found");
  }

  const type = dataSlot.type;
  switch (type) {
    case "source":
      throw new LogicError(`unexpected data slot type: ${type}`);
    case "intermediate": {
      const disposableWrap = dataSlot.provide();
      dataSlot.disposableWrapContainer.initialize(disposableWrap);
      return disposableWrap.body as T;
    }
    case "destination": {
      const body = dataSlot.body;
      return body as T;
    }
    default:
      return unreachable(type);
  }
}

/**
 * An internal function to prepare multiple outputs for the execution of an invocation.
 * @typeparam T The type of the outputs.
 * @param plan The plan the handles belong to.
 * @param handles The handles of outputs.
 * @returns The prepared outputs.
 */
function prepareMultipleOutput<
  T extends readonly UntypedHandle[],
>(
  plan: Plan,
  handles: T,
): MappedBodyType<T> {
  const partialPrepared = [];
  for (let i = 0; i < handles.length; i++) {
    partialPrepared.push(prepareOutput(
      plan,
      handles[i],
    ));
  }
  return partialPrepared as MappedBodyType<T>;
}

/**
 * An internal function to assert no leak.
 * @param plan The plan to check.
 */
function ensureAllIntermediateSlotsFreed(plan: Plan) {
  let hasLeak = false;
  for (const dataSlot of plan[internalPlanKey].dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        break;
      case "intermediate":
        if (!dataSlot.disposableWrapContainer.isFreed) {
          hasLeak = true;
        }
        dataSlot.disposableWrapContainer.forceCleanUp();
        break;
      case "destination":
        break;
      default:
        return unreachable(type);
    }
  }
  if (plan.context[contextOptionsKey].assertNoLeak && hasLeak) {
    throw new AssertionError(
      "intermediate data slot is not freed",
    );
  }
}

/**
 * An internal function to apply the middlewares to the body function.
 * @param body The body function.
 * @param middlewares The middlewares to apply.
 * @returns The body function with the middlewares applied.
 */
function applyMiddlewares(
  body: InvocationBodyFn,
  middlewares: MiddlewareFn[],
): InvocationBodyFn {
  return middlewares.reduceRight<InvocationBodyFn>((f, m) => () => m(f), body);
}
