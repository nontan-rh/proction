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
 * - Incremental calculation across repeated runs
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
import {
  alwaysChangedDataVersion,
  type DataID,
  type DataVersion,
  generateProcID,
  Graph,
  type GraphRun,
  type InvocationDraft,
  type ProcID,
  undefinedProviderID,
  unknownDataVersion,
  unresolvedIntermediateDataID,
  unresolvedIntermediateDataVersion,
  versionToSourceDataVersion,
} from "./_graph.ts";
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
 * A version of the content of a destination, generated by the library.
 * Versions are opaque values compared only for equality: a changed version
 * means changed content. They are reported via SetVersionFn.
 */
export type Version = DataVersion;

/**
 * A callback to receive the version of the content written to a destination.
 * It is called after a run finishes successfully.
 */
export type SetVersionFn = (version: Version) => void;

/**
 * An internal type to represent an invocation.
 */
type InvocationBodyFn = () => Promise<void>;
/**
 * A type to represent a middleware function. Executions of middlewares
 * may be skipped by the incremental run feature.
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
// For example, outputs created by the `toFunc*()` operators have provide closures
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
  const procID = generateProcID();
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
      procID,
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
  const procID = generateProcID();
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
      procID,
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
  const procID = generateProcID();
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
      procID,
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
  const procID = generateProcID();
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
        const preparedOutputs = prepareMultipleOutput(plan, outputs);
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
        );
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
      procID,
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
  const procID = generateProcID();
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
        const preparedOutputs = prepareMultipleOutput(plan, outputs);
        await fOutOfPlace(
          preparedOutputs,
          ...restoredIoInputs,
          ...restoredAdditionalInputs,
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
          ...restoredAdditionalInputs,
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
      procID,
      id,
      inputs: [...ioInputs, ...additionalInputs],
      outputs: outputs,
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
      provide,
    );
    indirectProcedure(output, ...inputs);
    return output;
  };
}

/**
 * Converts an indirect procedure which has a single output to an indirect
 * function whose output is memoized across runs. The output is created as a
 * memoized intermediate: its buffer is retained inside the {@link Context}
 * after a successful run, and the invocation writing it is skipped on later
 * runs while its inputs are unchanged — consumers read the retained buffer
 * directly. Memoization is keyed by the procedure, its inputs, and the
 * identity of `provide`, so convert once and reuse the converted function
 * across runs; converting anew every run is still correct but recomputes
 * every run. When the invocation does re-execute, a new buffer is allocated
 * and the retained one is released only after the run succeeds, so a failed
 * run leaves the previously retained content valid. Calling the converted
 * function marks the run as versioned, so such runs on one context must not
 * overlap.
 * @typeparam O The output type of the indirect procedure and the return type of the indirect function.
 * @typeparam I The list of input types of the indirect procedure and the indirect function.
 * @typeparam A The list of argument types of the provide function. They are also the type of objects created by the provide functions.
 * @param indirectProcedure The indirect procedure to convert.
 * @param provide The provide function attached to the indirect function.
 * Its identity across runs keys the memoization.
 * @returns The converted indirect function.
 */
export function toFuncM<
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
    const output = memoizedIntermediate(
      plan,
      () => provide(...restoreInputs(plan, inputs)),
      provide,
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
        provide,
      );
      partialOutputs.push(handle);
    }
    const outputs = partialOutputs as MappedHandleType<O>;

    indirectProcedure(outputs, ...inputs);

    return outputs;
  };
}

/**
 * Converts an indirect procedure which has multiple outputs to an indirect
 * function whose outputs are memoized across runs. Each output is created
 * as a memoized intermediate: its buffer is retained inside the
 * {@link Context} after a successful run, and the invocation writing it is
 * skipped on later runs while its inputs are unchanged — consumers read the
 * retained buffers directly. Memoization is keyed by the procedure, its
 * inputs, and the identities of the provide functions, so convert once and
 * reuse the converted function across runs; converting anew every run is
 * still correct but recomputes every run. When the invocation does
 * re-execute, new buffers are allocated and the retained ones are released
 * only after the run succeeds, so a failed run leaves the previously
 * retained content valid. Calling the converted function marks the run as
 * versioned, so such runs on one context must not overlap.
 * @typeparam O The list of output types of the indirect procedure and the return type of the indirect function.
 * @typeparam I The list of input types of the indirect procedure and the indirect function.
 * @typeparam A The list of argument types of the provide function. They are also the type of objects created by the provide functions.
 * @param indirectProcedure The indirect procedure to convert.
 * @param provideFns The provide functions attached to the indirect
 * function. Their identities across runs key the memoization.
 * @returns The converted indirect function.
 */
export function toFuncNM<
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
      const handle = memoizedIntermediate(
        plan,
        () => provide(...restoreInputs(plan, inputs)),
        provide,
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
  readonly procID: ProcID;
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
 * An internal symbol used for the key of the dependency graph in a context.
 */
const graphKey = Symbol("graph");

/**
 * An internal symbol used for the key of the run state of a context.
 */
const stateKey = Symbol("state");

/**
 * An internal type to represent the run state of a context. Runs mutate the
 * shared graph and must not overlap, so the state also acts as the semaphore
 * that rejects overlapping runs.
 */
type ContextState = "idle" | "planning" | "running";

/**
 * An internal symbol used for the key of the buffers of memoized
 * intermediates retained in a context.
 */
const retainedBuffersKey = Symbol("retainedBuffers");

/**
 * A context for a Proction program. It is expected to live some long span in an application.
 */
export class Context {
  /**
   * The options of the context.
   */
  [contextOptionsKey]: ContextOptions;

  /**
   * The persistent dependency graph used for incremental calculation.
   */
  [graphKey]: Graph = new Graph();

  /**
   * The run state. A new run is accepted only in the "idle" state.
   */
  [stateKey]: ContextState = "idle";

  /**
   * The buffers of memoized intermediates retained across runs, keyed by
   * their resolved data IDs. A buffer whose wiring is absent from the
   * latest versioned run is released back to its provider.
   */
  [retainedBuffersKey]: Map<DataID, DisposableWrap<unknown>> = new Map();

  /**
   * Releases all buffers retained for memoized intermediates back to their
   * providers. The context remains usable afterwards; later runs simply
   * recompute and retain again. An exception thrown by a release is routed
   * to the context's `reportError`.
   */
  [Symbol.dispose](): void {
    const reportError = this[contextOptionsKey].reportError;
    for (const wrap of this[retainedBuffersKey].values()) {
      try {
        wrap[Symbol.dispose]();
      } catch (e: unknown) {
        reportError(e);
      }
    }
    this[retainedBuffersKey].clear();
  }

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
 * Runs on a context must not overlap: a run submitted while another run of
 * the same context is in flight is rejected, because runs read and update
 * the context's incremental records.
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
    $s: (value, version) => source(plan, value, version),
    $d: (value, version, setVersion) =>
      destination(plan, value, version, setVersion),
    $e: (value, version, setVersion) =>
      externalIntermediate(plan, value, version, setVersion),
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
   * @param version The version of the content of the resource, a non-negative
   * integer managed by the caller. It enables incremental calculation: change
   * it whenever the content changes. If omitted, the content is treated as
   * changed every run. Repeated `$s` calls on the same object must claim the
   * same version (or all omit it); any disagreement throws.
   * @returns The read-only source handle.
   */
  $s<T extends object>(value: T, version?: number): Handle<T>;
  /**
   * Creates a write-only destination handle from an external resource.
   * @typeparam T The type of the external resource.
   * @param value The external resource.
   * @param version The version of the content the resource currently holds,
   * as previously reported via setVersion. It enables incremental
   * calculation: an invocation writing this destination can be skipped when
   * the version matches the recorded result. If omitted, the content is
   * treated as unknown and the writing invocation always runs. Each object
   * may be passed to `$d` at most once per run; a repeated call throws.
   * @param setVersion A callback that receives the version of the written
   * content after a successful run.
   * @returns The write-only destination handle.
   */
  $d<T extends object>(
    value: T,
    version?: Version,
    setVersion?: SetVersionFn,
  ): Handle<T>;
  /**
   * Creates an intermediate handle backed by an externally managed buffer.
   * The buffer is written by the invocation that outputs it and can be read
   * by other invocations like an intermediate, but its storage and lifetime
   * are managed by the caller like a destination. With versions it memoizes
   * the intermediate result across runs: the writing invocation is skipped
   * while its inputs are unchanged and either the buffer still holds the
   * recorded content (the version matches) or no surviving invocation reads
   * the buffer; a stale buffer is recomputed only when some invocation
   * actually reads it.
   * @typeparam T The type of the external buffer.
   * @param value The external buffer.
   * @param version The version of the content the buffer currently holds,
   * as previously reported via setVersion. If omitted, the content is
   * treated as unknown. `$e` may be called at most once per object in a
   * run; reuse the returned handle instead of calling it again.
   * @param setVersion A callback that receives the version describing the
   * buffer's content after a successful run. When the writing invocation is
   * skipped while the buffer is stale, no version is reported: the content
   * is unchanged and still described by the version passed in, if any.
   * @returns The external intermediate handle.
   */
  $e<T extends object>(
    value: T,
    version?: Version,
    setVersion?: SetVersionFn,
  ): Handle<T>;
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
  inputCache: WeakMap<object, UntypedHandle>;
  outputCache: WeakMap<object, UntypedHandle>;
  externalCache: WeakMap<object, UntypedHandle>;
  // Whether any $s/$d call supplied a version or a setVersion callback.
  // When false, the incremental pass is skipped entirely.
  usesVersions = false;

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
    this.inputCache = new WeakMap();
    this.outputCache = new WeakMap();
    this.externalCache = new WeakMap();
  }
}
/**
 * An internal union type of data slots.
 */
type DataSlot =
  | SourceSlot
  | IntermediateSlot
  | DestinationSlot
  | ExternalIntermediateSlot
  | MemoizedIntermediateSlot;
/**
 * An internal type to represent a source slot.
 */
type SourceSlot = {
  type: "source";
  body: unknown;
  dataID: DataID;
  // A caller-managed version; mapped into the graph's namespace by the
  // incremental pass.
  version: number | undefined;
};
/**
 * An internal type to represent an intermediate slot.
 */
type IntermediateSlot = {
  type: "intermediate";
  provide: () => DisposableWrap<unknown>;
  // The stable identity of the provider across runs. Unlike dataID, this is
  // managed outside of the graph because toFunc* operators would make a new
  // function object for each call and it would break the identity.
  provideKey: object;
  disposableWrapContainer: DelayedRc<DisposableWrap<unknown>>;
};
/**
 * An internal type to represent a destination slot.
 */
type DestinationSlot = {
  type: "destination";
  body: unknown;
  dataID: DataID;
  version: Version | undefined;
  setVersion: SetVersionFn | undefined;
  // The version of the content the destination holds after the run,
  // calculated by the incremental pass.
  resolvedVersion: Version | undefined;
};
/**
 * An internal type to represent an external-intermediate slot: a
 * caller-managed buffer used as an intermediate, memoizing its content
 * across runs when versions are supplied.
 */
type ExternalIntermediateSlot = {
  type: "externalIntermediate";
  body: unknown;
  dataID: DataID;
  version: Version | undefined;
  setVersion: SetVersionFn | undefined;
  // The version describing the buffer's content after the run, calculated
  // by the incremental pass. When the writing invocation is skipped while
  // the buffer is stale, this is reset to the caller's claimed version (or
  // undefined), because the recorded version does not describe the content.
  resolvedVersion: Version | undefined;
};
/**
 * An internal type to represent a memoized-intermediate slot: an
 * intermediate whose buffer is retained inside the context across runs, so
 * that the invocation writing it can be skipped while its inputs are
 * unchanged.
 */
type MemoizedIntermediateSlot = {
  type: "memoizedIntermediate";
  provide: () => DisposableWrap<unknown>;
  // The stable identity of the provider across runs (see IntermediateSlot).
  provideKey: object;
  // The data ID resolved for this output by the incremental pass; it keys
  // the buffer retained in the context.
  resolvedDataID: DataID | undefined;
  // The buffer retained by a previous run, looked up by the incremental
  // pass. It stays owned by the context; consumers read it when the writing
  // invocation is skipped.
  retainedWrap: DisposableWrap<unknown> | undefined;
  // Holds the buffer produced by this run. Consumers do not reference-count
  // it: the wrap must survive the whole run to be retained in the context,
  // so the count stays at its initial 1 until the retention pass extracts it
  // or cleanup frees it. The retained buffer is never written to:
  // re-execution allocates a fresh buffer and the retention pass swaps it
  // in only after the run succeeds.
  disposableWrapContainer: DelayedRc<DisposableWrap<unknown>>;
};

/**
 * An internal function to validate the common shape of caller-supplied
 * versions.
 * @param version The version to validate.
 */
function validateVersionShape(version: number): void {
  if (!Number.isInteger(version) || version < 0) {
    throw new PreconditionError("version must be a non-negative integer");
  }
}

/**
 * An internal function to validate a caller-managed source version. The
 * upper bound keeps versionToSourceDataVersion exact and inside the odd
 * namespace: from 2^52 on, `version * 2 + 1` loses precision, so distinct
 * versions collide and can even fall into the even generated namespace.
 * @param version The version passed to $s, if any.
 */
function validateSourceVersion(version: number | undefined): void {
  if (version == null) {
    return;
  }
  validateVersionShape(version);
  if (version >= 2 ** 52) {
    throw new PreconditionError("version must be less than 2^52");
  }
}

/**
 * An internal function to validate a destination version claim. Versions
 * generated by the library are always even, so an odd claim cannot have
 * been reported via setVersion and could alias a caller-managed source
 * version.
 * @param version The version passed to $d, if any.
 */
function validateDestinationVersion(version: Version | undefined): void {
  if (version == null) {
    return;
  }
  validateVersionShape(version);
  if (version % 2 !== 0) {
    throw new PreconditionError(
      "version must be a version reported via setVersion",
    );
  }
}

/**
 * An internal function to create a source handle and a source slot. It is the implementation of $s function.
 * @typeparam T The type of the external resource.
 * @param plan The plan to create the source handle in.
 * @param value The external resource.
 * @returns The source handle.
 */
function source<T extends object>(
  plan: Plan,
  value: T,
  version: number | undefined,
): Handle<T> {
  const internalPlan = plan[internalPlanKey];

  // validation (also for repeated calls on the same object)
  validateSourceVersion(version);
  if (version != null) {
    internalPlan.usesVersions = true;
  }

  const cached = internalPlan.inputCache.get(value);
  if (cached) {
    const dataSlot = internalPlan.dataSlots.get(cached[handleIdKey]);
    if (dataSlot == null || dataSlot.type !== "source") {
      throw new LogicError(`dataSlot not found for handle: ${cached}`);
    }
    // Two different versions for a single object are contradictory.
    if (dataSlot.version !== version) {
      throw new PreconditionError(
        "the value is already specified as input with a different version",
      );
    }
    return cached as Handle<T>;
  }

  if (internalPlan.outputCache.has(value)) {
    throw new PreconditionError("the value is already specified as output");
  }
  if (internalPlan.externalCache.has(value)) {
    throw new PreconditionError(
      "the value is already specified as external intermediate",
    );
  }

  const handle = internalPlan.generateHandle();

  internalPlan.dataSlots.set(handle[handleIdKey], {
    type: "source",
    body: value,
    dataID: plan.context[graphKey].resolveDataID(value),
    version,
  });
  internalPlan.inputCache.set(value, handle);

  return handle as Handle<T>;
}

/**
 * An internal function to create a destination handle and a destination slot. It is the implementation of $d function.
 * @typeparam T The type of the external resource.
 * @param plan The plan to create the destination handle in.
 * @param value The external resource.
 * @returns The destination handle.
 */
function destination<T extends object>(
  plan: Plan,
  value: T,
  version: Version | undefined,
  setVersion: SetVersionFn | undefined,
): Handle<T> {
  const internalPlan = plan[internalPlanKey];

  // validation (also for repeated calls on the same object)
  validateDestinationVersion(version);
  if (version != null || setVersion != null) {
    internalPlan.usesVersions = true;
  }

  // Aliasing is not allowed for destinations
  if (internalPlan.outputCache.has(value)) {
    throw new PreconditionError(
      "the value is already specified as another output",
    );
  }
  if (internalPlan.inputCache.has(value)) {
    throw new PreconditionError("the value is already specified as input");
  }
  if (internalPlan.externalCache.has(value)) {
    throw new PreconditionError(
      "the value is already specified as external intermediate",
    );
  }

  const handle = internalPlan.generateHandle();

  internalPlan.dataSlots.set(handle[handleIdKey], {
    type: "destination",
    body: value,
    dataID: plan.context[graphKey].resolveDataID(value),
    version,
    setVersion,
    resolvedVersion: undefined,
  });
  internalPlan.outputCache.set(value, handle);

  return handle as Handle<T>;
}

/**
 * An internal function to create an external-intermediate handle and an
 * external-intermediate slot. It is the implementation of $e function.
 * @typeparam T The type of the external buffer.
 * @param plan The plan to create the external-intermediate handle in.
 * @param value The external buffer.
 * @param version The version of the content the buffer currently holds.
 * @param setVersion The callback that receives the version of the content.
 * @returns The external-intermediate handle.
 */
function externalIntermediate<T extends object>(
  plan: Plan,
  value: T,
  version: Version | undefined,
  setVersion: SetVersionFn | undefined,
): Handle<T> {
  const internalPlan = plan[internalPlanKey];

  // validation (also for repeated calls on the same object)
  if (version != null && (!Number.isInteger(version) || version < 0)) {
    throw new PreconditionError("version must be a non-negative integer");
  }
  if (version != null || setVersion != null) {
    internalPlan.usesVersions = true;
  }

  if (internalPlan.externalCache.has(value)) {
    throw new PreconditionError(
      "the value is already specified as external intermediate",
    );
  }
  if (internalPlan.inputCache.has(value)) {
    throw new PreconditionError("the value is already specified as input");
  }
  if (internalPlan.outputCache.has(value)) {
    throw new PreconditionError("the value is already specified as output");
  }

  const handle = internalPlan.generateHandle();

  internalPlan.dataSlots.set(handle[handleIdKey], {
    type: "externalIntermediate",
    body: value,
    dataID: plan.context[graphKey].resolveDataID(value),
    version,
    setVersion,
    resolvedVersion: undefined,
  });
  internalPlan.externalCache.set(value, handle);

  return handle as Handle<T>;
}

/**
 * An internal function to create a memoized-intermediate handle and a
 * memoized-intermediate slot. It backs the outputs of the toFuncM/toFuncNM
 * conversions. Each call creates a distinct intermediate.
 * @typeparam T The type of the provided buffer.
 * @param plan The plan to create the memoized-intermediate handle in.
 * @param provide The provide function that allocates the buffer.
 * @param provideKey The stable identity of the provider across runs. It
 * defaults to `provide` itself; wrappers that recreate the provide closure
 * per wiring (e.g. toFuncM) must pass the caller's underlying function.
 * @returns The memoized-intermediate handle.
 */
function memoizedIntermediate<T>(
  plan: Plan,
  provide: () => DisposableWrap<T>,
  provideKey: object = provide,
): Handle<T> {
  const internalPlan = plan[internalPlanKey];
  // Memoization is inherently versioned: the retained buffers are shared
  // state of the context, protected by the versioned-run overlap rejection.
  internalPlan.usesVersions = true;

  const handle = internalPlan.generateHandle();

  internalPlan.dataSlots.set(handle[handleIdKey], {
    type: "memoizedIntermediate",
    provide,
    provideKey,
    resolvedDataID: undefined,
    retainedWrap: undefined,
    disposableWrapContainer: new DelayedRc((x) => {
      x[Symbol.dispose]();
    }, plan.context[contextOptionsKey].reportError),
  });

  return handle as Handle<T>;
}

/**
 * An internal function to create an intermediate handle and an intermediate slot. It backs the outputs of the toFunc/toFuncN conversions.
 * @typeparam T The type of the provided object.
 * @param plan The plan to create the intermediate handle in.
 * @param provide The provide function.
 * @param provideKey The stable identity of the provider across runs. It
 * defaults to `provide` itself; wrappers that recreate the provide closure
 * per wiring (e.g. toFunc) must pass the caller's underlying function.
 * @returns The intermediate handle.
 */
function intermediate<T>(
  plan: Plan,
  provide: () => DisposableWrap<T>,
  provideKey: object = provide,
): Handle<T> {
  const handle = plan[internalPlanKey].generateHandle();

  plan[internalPlanKey].dataSlots.set(handle[handleIdKey], {
    type: "intermediate",
    disposableWrapContainer: new DelayedRc((x) => {
      x[Symbol.dispose]();
    }, plan.context[contextOptionsKey].reportError),
    provide,
    provideKey,
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
  // A run mutates the context's shared graph while invocation bodies run
  // asynchronously; an overlap would silently corrupt the recorded versions,
  // so it is rejected instead.
  const context = plan.context;
  if (context[stateKey] !== "idle") {
    throw new PreconditionError("runs on a context must not overlap");
  }
  context[stateKey] = "planning";

  const invocationErrors: unknown[] = [];
  const startedInvocations = new Set<InvocationID>();
  let cleanupError: unknown | undefined;
  let pruneResult: PruneResult = null;
  try {
    const internalPlan = plan[internalPlanKey];
    if (internalPlan.usesVersions) {
      pruneResult = pruneUpToDateInvocations(plan);
    } else if (internalPlan.invocations.size > 0) {
      // A fully unversioned plan can never be skipped, so the incremental
      // pass is not worth its cost. Its invocations may still overwrite
      // destinations that committed records describe, so the records are
      // evicted, exactly as an ordinary commit would evict every record the
      // run did not resolve.
      context[graphKey].evictAllRecords();
    }

    const runningInvocations = new Set<InvocationID>();
    const freeInvocations = prepareInvocations(
      plan,
      pruneResult?.dependencyMaps ?? null,
      pruneResult?.submittedConsumerCounts ?? null,
    );
    prepareDataSlots(plan);

    context[stateKey] = "running";

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
      startedInvocations.add(invocation.id);
      scheduler.spawn(invocation.body!)
        .then(() => {
          for (const next of invocation.next) {
            if (next.numResolvedBlockers >= next.numBlockers) {
              throw new LogicError("the invocation is resolved twice");
            }
            next.numResolvedBlockers++;
            if (next.numResolvedBlockers >= next.numBlockers) {
              freeInvocations.push(next);
            }
          }
        })
        .catch((err: unknown) => {
          invocationErrors.push(err);
        })
        .finally(() => {
          runningInvocations.delete(invocation.id);
          notify();
        });
    }

    if (invocationErrors.length > 0) {
      // Invocations that started executing may have written their
      // destinations, even the ones that succeeded, and a failed run never
      // reports versions, so the caller's stored claims can go stale. Drop
      // the committed records of every started invocation that writes
      // caller-visible storage in place so the next submission re-executes
      // them instead of trusting the records. Memoized and plain
      // intermediates are produced into fresh buffers that a failed run
      // releases, so their records still describe the retained content and
      // are kept. Records produced by this run are never committed on
      // failure.
      if (pruneResult != null) {
        for (const id of startedInvocations) {
          const draft = pruneResult.drafts.get(id);
          const invocation = internalPlan.invocations.get(id);
          if (draft == null || invocation == null) {
            continue;
          }
          const writesInPlace = invocation.outputs.some((output) => {
            const dataSlot = internalPlan.dataSlots.get(output[handleIdKey]);
            return dataSlot?.type === "destination" ||
              dataSlot?.type === "externalIntermediate";
          });
          if (writesInPlace) {
            pruneResult.graphRun.invalidate(draft);
          }
        }
      }

      if (invocationErrors.length === 1) {
        throw invocationErrors[0];
      }
      throw new AggregateError(invocationErrors, "invocation failed");
    }

    pruneResult?.graphRun.commit();
    retainMemoizedBuffers(plan, pruneResult != null);
    notifyResolvedVersions(plan);
  } finally {
    try {
      ensureAllIntermediateSlotsFreed(plan);
    } catch (error) {
      cleanupError = error;
    }

    context[stateKey] = "idle";
  }

  if (cleanupError !== undefined && invocationErrors.length === 0) {
    throw cleanupError;
  }
}

/**
 * An internal function to preprocess invocations before execution.
 * @param plan The plan to prepare invocations for.
 * @param dependencyMaps Dependency maps that already describe the plan's
 * invocations, or null to build them here.
 * @param submittedConsumerCounts The consumer counts of the plan as
 * submitted, before pruning, or null when the plan was not pruned.
 * @returns The prepared invocations.
 */
function prepareInvocations(
  plan: Plan,
  dependencyMaps: DependencyMaps | null,
  submittedConsumerCounts: Map<HandleId, number> | null,
): Invocation[] {
  const freeInvocations: Invocation[] = [];

  const invocations = plan[internalPlanKey].invocations;
  const { producerByHandle, consumersByHandle, blockerCounts } =
    dependencyMaps ?? buildDependencyMaps(invocations);

  for (const invocation of invocations.values()) {
    for (const input of invocation.inputs) {
      const parentInvocation = producerByHandle.get(input[handleIdKey]);
      if (parentInvocation == null) {
        continue;
      }
      // Input of the invocation depends on its parent invocation

      parentInvocation.next.push(invocation); // allow duplication for proper counting
    }
    invocation.numBlockers = blockerCounts.get(invocation.id)!;
  }

  // The in-place/out-of-place variant selection must depend only on the
  // wiring as submitted, not on this run's pruning: the graph carries
  // recorded versions over on the assumption that a re-execution reproduces
  // the recorded content, and the variant can change the produced content
  // (e.g. through the shape of the output buffer).
  const inputConsumerCounts = submittedConsumerCounts ??
    consumerCountsOf(consumersByHandle);
  const resolveContext: ResolveContext = { plan, inputConsumerCounts };
  for (const invocation of invocations.values()) {
    invocation.body = invocation.resolveBody(resolveContext);
  }

  for (const invocation of invocations.values()) {
    if (invocation.numBlockers === 0) {
      freeInvocations.push(invocation);
    }
  }

  return freeInvocations;
}

/**
 * An internal type of the dependency structure of a plan's invocations: the
 * producer of each handle, the consumers of each handle (once per input
 * reference), and the number of in-plan blockers per invocation.
 */
type DependencyMaps = {
  producerByHandle: Map<HandleId, Invocation>;
  consumersByHandle: Map<HandleId, Invocation[]>;
  blockerCounts: Map<InvocationID, number>;
};

/**
 * An internal function to derive the dependency structure of a plan's
 * invocations.
 * @param invocations The invocations of the plan.
 * @returns The dependency maps of the invocations.
 */
function buildDependencyMaps(
  invocations: Map<InvocationID, Invocation>,
): DependencyMaps {
  const producerByHandle = new Map<HandleId, Invocation>();
  for (const invocation of invocations.values()) {
    for (const output of invocation.outputs) {
      const id = output[handleIdKey];
      if (producerByHandle.has(id)) {
        throw new LogicError(
          "the output have two parent invocations",
        );
      }
      producerByHandle.set(id, invocation);
    }
  }

  const consumersByHandle = new Map<HandleId, Invocation[]>();
  const blockerCounts = new Map<InvocationID, number>();
  for (const invocation of invocations.values()) {
    let numBlockers = 0;
    for (const input of invocation.inputs) {
      const id = input[handleIdKey];
      if (producerByHandle.has(id)) {
        numBlockers++;
      }
      let consumers = consumersByHandle.get(id);
      if (consumers == null) {
        consumers = [];
        consumersByHandle.set(id, consumers);
      }
      consumers.push(invocation);
    }
    blockerCounts.set(invocation.id, numBlockers);
  }

  return { producerByHandle, consumersByHandle, blockerCounts };
}

/**
 * An internal function to derive per-handle consumer counts from the
 * consumers map.
 * @param consumersByHandle The consumers of each handle.
 * @returns The number of consuming input references per handle.
 */
function consumerCountsOf(
  consumersByHandle: Map<HandleId, Invocation[]>,
): Map<HandleId, number> {
  const counts = new Map<HandleId, number>();
  for (const [id, consumers] of consumersByHandle) {
    counts.set(id, consumers.length);
  }
  return counts;
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
        case "externalIntermediate":
          break;
        case "memoizedIntermediate":
          // Not reference-counted: the wrap survives the whole run to be
          // retained in the context.
          break;
        default:
          return unreachable(type);
      }
    }
  }
}

/**
 * The graph session of a pruned plan, used by runPlan to commit the records
 * of a successful run or to drop the records of started invocations on
 * failure, plus precomputed structures reusable by prepareInvocations.
 */
type PruneResult = {
  graphRun: GraphRun;
  drafts: Map<InvocationID, InvocationDraft>;
  // The consumer counts of the plan as submitted, for prune-independent
  // in-place variant selection.
  submittedConsumerCounts: Map<HandleId, number>;
  // The dependency maps of the plan, still valid when no invocation was
  // pruned; null otherwise.
  dependencyMaps: DependencyMaps | null;
} | null;

/**
 * An internal function to remove invocations whose outputs are already
 * up-to-date from a plan before execution. It resolves data IDs and versions
 * of the plan against the persistent graph of the context, and an invocation
 * is removed when its inputs are unchanged since its last recorded run and
 * every output is either a destination already holding the recorded content
 * or an intermediate consumed only by removed invocations. It must be called
 * before prepareInvocations and prepareDataSlots so that blocker counts and
 * reference counts are calculated only on the surviving invocations, while
 * the consumer counts it returns describe the plan as submitted.
 * @param plan The plan to prune.
 * @returns The graph session, per-invocation drafts, the submitted consumer
 * counts, and dependency maps reusable when nothing was pruned, or null when
 * the plan has no invocations and the graph was not consulted.
 */
function pruneUpToDateInvocations(plan: Plan): PruneResult {
  const internalPlan = plan[internalPlanKey];
  const invocations = internalPlan.invocations;
  if (invocations.size === 0) {
    return null;
  }

  const graph = plan.context[graphKey];
  const graphRun = graph.beginRun();
  const retainedBuffers = plan.context[retainedBuffersKey];

  // invocation.next and invocation.numBlockers must be left untouched for
  // prepareInvocations; the shared builder returns fresh local maps.
  const dependencyMaps = buildDependencyMaps(invocations);
  const { consumersByHandle } = dependencyMaps;
  const submittedConsumerCounts = consumerCountsOf(consumersByHandle);

  // Topological order. The wiring order is not guaranteed to be topological
  // because a destination handle can be consumed by an invocation wired
  // before its producer. Invocations left unordered (cycles) are never
  // considered unchanged. Decrements work on a copy so that the dependency
  // maps stay reusable by prepareInvocations.
  const remainingBlockers = new Map(dependencyMaps.blockerCounts);
  const order: Invocation[] = [];
  for (const invocation of invocations.values()) {
    if (remainingBlockers.get(invocation.id) === 0) {
      order.push(invocation);
    }
  }
  for (let i = 0; i < order.length; i++) {
    for (const output of order[i].outputs) {
      const consumers = consumersByHandle.get(output[handleIdKey]);
      if (consumers == null) {
        continue;
      }
      for (const consumer of consumers) {
        const count = remainingBlockers.get(consumer.id)! - 1;
        remainingBlockers.set(consumer.id, count);
        if (count === 0) {
          order.push(consumer);
        }
      }
    }
  }

  // Forward pass: resolve data IDs and versions in topological order and
  // record which invocations are unchanged.
  const resolvedRefs = new Map<
    HandleId,
    { dataID: DataID; version: DataVersion }
  >();
  const unchangedInvocations = new Set<InvocationID>();
  const drafts = new Map<InvocationID, InvocationDraft>();

  for (const invocation of order) {
    let consultGraph = true;

    const inputIDs: DataID[] = [];
    const inputVersions: DataVersion[] = [];
    for (const input of invocation.inputs) {
      const id = input[handleIdKey];
      const resolved = resolvedRefs.get(id);
      if (resolved != null) {
        inputIDs.push(resolved.dataID);
        inputVersions.push(resolved.version);
        continue;
      }

      const dataSlot = internalPlan.dataSlots.get(id);
      if (dataSlot == null) {
        throw new LogicError(`dataSlot not found for handle: ${input}`);
      }

      const type = dataSlot.type;
      switch (type) {
        case "source":
          // Caller-managed source versions live in a namespace disjoint from
          // the generated versions.
          inputIDs.push(dataSlot.dataID);
          inputVersions.push(
            dataSlot.version != null
              ? versionToSourceDataVersion(dataSlot.version)
              : alwaysChangedDataVersion,
          );
          break;
        case "destination":
        case "externalIntermediate":
          // A destination or external-intermediate version is a generated
          // version round-tripped through the caller, used verbatim.
          inputIDs.push(dataSlot.dataID);
          inputVersions.push(dataSlot.version ?? alwaysChangedDataVersion);
          break;
        case "intermediate":
        case "memoizedIntermediate":
          // No producer in this plan; the invocation cannot be resolved and
          // the run will fail at execution time as usual.
          consultGraph = false;
          break;
        default:
          return unreachable(type);
      }
      if (!consultGraph) {
        break;
      }
    }

    const outputIDs: DataID[] = [];
    const outputVersions: DataVersion[] = [];
    const providerIDs: DataID[] = [];
    const outputSlots: (
      | DestinationSlot
      | ExternalIntermediateSlot
      | MemoizedIntermediateSlot
      | null
    )[] = [];
    if (consultGraph) {
      for (const output of invocation.outputs) {
        const id = output[handleIdKey];
        const dataSlot = internalPlan.dataSlots.get(id);
        if (dataSlot == null) {
          throw new LogicError(`dataSlot not found for handle: ${output}`);
        }

        const type = dataSlot.type;
        switch (type) {
          case "source":
            // Invalid plan; the run will fail at execution time as usual.
            consultGraph = false;
            break;
          case "destination":
            outputIDs.push(dataSlot.dataID);
            outputVersions.push(dataSlot.version ?? unknownDataVersion);
            providerIDs.push(undefinedProviderID);
            outputSlots.push(dataSlot);
            break;
          case "intermediate":
            outputIDs.push(unresolvedIntermediateDataID);
            outputVersions.push(unresolvedIntermediateDataVersion);
            providerIDs.push(graph.resolveDataID(dataSlot.provideKey));
            outputSlots.push(null);
            break;
          case "externalIntermediate":
            // The buffer has a stable external identity, but its version
            // acts as a wildcard for the unchanged check: whether the
            // invocation can be skipped is decided in the backward pass,
            // where a stale buffer is tolerated when no surviving
            // invocation reads it.
            outputIDs.push(dataSlot.dataID);
            outputVersions.push(unresolvedIntermediateDataVersion);
            providerIDs.push(unresolvedIntermediateDataID);
            outputSlots.push(dataSlot);
            break;
          case "memoizedIntermediate":
            // Resolved like an intermediate: identified by its provider,
            // with a wildcard version. Whether the writing invocation can
            // be skipped is decided in the backward pass by the presence of
            // the retained buffer.
            outputIDs.push(unresolvedIntermediateDataID);
            outputVersions.push(unresolvedIntermediateDataVersion);
            providerIDs.push(graph.resolveDataID(dataSlot.provideKey));
            outputSlots.push(dataSlot);
            break;
          default:
            return unreachable(type);
        }
        if (!consultGraph) {
          break;
        }
      }
    }

    if (!consultGraph) {
      continue;
    }

    const draft: InvocationDraft = {
      procID: invocation.procID,
      inputIDs,
      inputVersions,
      outputIDs,
      outputVersions,
      providerIDs,
    };
    drafts.set(invocation.id, draft);
    const resolved = graphRun.resolve(draft);

    for (let i = 0; i < invocation.outputs.length; i++) {
      resolvedRefs.set(invocation.outputs[i][handleIdKey], {
        dataID: resolved.outputIDs[i],
        version: resolved.outputVersions[i],
      });

      const outputSlot = outputSlots[i];
      if (outputSlot != null) {
        if (outputSlot.type === "memoizedIntermediate") {
          outputSlot.resolvedDataID = resolved.outputIDs[i];
          outputSlot.retainedWrap = retainedBuffers.get(
            resolved.outputIDs[i],
          );
        } else {
          outputSlot.resolvedVersion = resolved.outputVersions[i];
        }
      }
    }

    if (resolved.unchanged) {
      unchangedInvocations.add(invocation.id);
    }
  }

  // Backward pass: an unchanged invocation is skipped when every consumer of
  // each of its intermediate outputs is skipped. Destination outputs need no
  // condition here: unchanged already implies they hold the recorded
  // content. An external-intermediate output holding the recorded content
  // (its claimed version matches the resolved one) needs no condition
  // either, and neither does a memoized-intermediate output whose buffer is
  // retained; an unavailable one is tolerated only when every consumer is
  // skipped.
  const skippedInvocations = new Set<InvocationID>();
  for (let i = order.length - 1; i >= 0; i--) {
    const invocation = order[i];
    if (!unchangedInvocations.has(invocation.id)) {
      continue;
    }

    let skippable = true;
    for (const output of invocation.outputs) {
      const id = output[handleIdKey];
      const dataSlot = internalPlan.dataSlots.get(id);
      if (dataSlot == null) {
        continue;
      }
      if (dataSlot.type === "externalIntermediate") {
        if (
          dataSlot.version != null &&
          dataSlot.version === dataSlot.resolvedVersion
        ) {
          continue;
        }
      } else if (dataSlot.type === "memoizedIntermediate") {
        if (dataSlot.retainedWrap != null) {
          continue;
        }
      } else if (dataSlot.type !== "intermediate") {
        continue;
      }

      const consumers = consumersByHandle.get(id);
      if (consumers == null) {
        continue;
      }
      for (const consumer of consumers) {
        if (!skippedInvocations.has(consumer.id)) {
          skippable = false;
          break;
        }
      }
      if (!skippable) {
        break;
      }
    }

    if (skippable) {
      skippedInvocations.add(invocation.id);
      // A skipped invocation leaves a stale external intermediate
      // untouched: its content is still described by the caller's original
      // claim, not by the recorded version, so the claim is what setVersion
      // may report.
      for (const output of invocation.outputs) {
        const dataSlot = internalPlan.dataSlots.get(output[handleIdKey]);
        if (
          dataSlot != null &&
          dataSlot.type === "externalIntermediate" &&
          dataSlot.resolvedVersion !== dataSlot.version
        ) {
          dataSlot.resolvedVersion = dataSlot.version;
        }
      }
    }
  }

  if (skippedInvocations.size === 0) {
    return { graphRun, drafts, submittedConsumerCounts, dependencyMaps };
  }

  // Prune the skipped invocations, and remove intermediate slots that no
  // surviving invocation references so that they are not reported as leaks.
  const skippedHandles = new Set<HandleId>();
  for (const invocation of order) {
    if (!skippedInvocations.has(invocation.id)) {
      continue;
    }
    invocations.delete(invocation.id);
    for (const input of invocation.inputs) {
      skippedHandles.add(input[handleIdKey]);
    }
    for (const output of invocation.outputs) {
      skippedHandles.add(output[handleIdKey]);
    }
  }

  const survivingHandles = new Set<HandleId>();
  for (const invocation of invocations.values()) {
    for (const input of invocation.inputs) {
      survivingHandles.add(input[handleIdKey]);
    }
    for (const output of invocation.outputs) {
      survivingHandles.add(output[handleIdKey]);
    }
  }

  for (const handleId of skippedHandles) {
    if (survivingHandles.has(handleId)) {
      continue;
    }
    const dataSlot = internalPlan.dataSlots.get(handleId);
    if (dataSlot != null && dataSlot.type === "intermediate") {
      internalPlan.dataSlots.delete(handleId);
    }
  }

  return { graphRun, drafts, submittedConsumerCounts, dependencyMaps: null };
}

/**
 * An internal function to report the resolved versions of destinations and
 * external intermediates to the caller. It must be called only after all
 * invocations of the plan finished successfully, so that reported versions
 * always describe content that is actually available.
 * @param plan The plan whose resolved versions are reported.
 */
function notifyResolvedVersions(plan: Plan): void {
  const reportError = plan.context[contextOptionsKey].reportError;
  for (const dataSlot of plan[internalPlanKey].dataSlots.values()) {
    if (
      dataSlot.type !== "destination" &&
      dataSlot.type !== "externalIntermediate"
    ) {
      continue;
    }
    const resolvedVersion = dataSlot.resolvedVersion;
    const setVersion = dataSlot.setVersion;
    if (resolvedVersion == null || setVersion == null) {
      continue;
    }

    // A throwing user callback must not fail the run.
    try {
      setVersion(resolvedVersion);
    } catch (e: unknown) {
      reportError(e);
    }
  }
}

/**
 * An internal function to retain the buffers of memoized intermediates in
 * the context and to release the retained buffers whose wirings are absent
 * from this run. It must be called only after all invocations of the plan
 * finished successfully: a failed run retains nothing, so the buffers
 * retained by previous runs stay consistent with the committed records.
 * Buffers produced by wirings that could not be resolved against the graph
 * have no identity to be retained under and are released immediately, like
 * plain intermediates.
 * @param plan The plan whose memoized intermediates are retained.
 * @param versioned Whether the incremental pass resolved this plan; only
 * then does the run's wiring set define which retained buffers survive.
 */
function retainMemoizedBuffers(plan: Plan, versioned: boolean): void {
  const reportError = plan.context[contextOptionsKey].reportError;
  const retainedBuffers = plan.context[retainedBuffersKey];

  const touched = new Set<DataID>();
  for (const dataSlot of plan[internalPlanKey].dataSlots.values()) {
    if (dataSlot.type !== "memoizedIntermediate") {
      continue;
    }

    const resolvedDataID = dataSlot.resolvedDataID;
    if (resolvedDataID != null) {
      touched.add(resolvedDataID);
    }

    const container = dataSlot.disposableWrapContainer;
    if (!container.isInitialized) {
      // The writing invocation was skipped (the retained buffer, if any,
      // stays) or never existed.
      continue;
    }

    const wrap = container.extract();
    if (resolvedDataID == null) {
      try {
        wrap[Symbol.dispose]();
      } catch (e: unknown) {
        reportError(e);
      }
      continue;
    }

    const previous = retainedBuffers.get(resolvedDataID);
    if (previous != null && previous !== wrap) {
      try {
        previous[Symbol.dispose]();
      } catch (e: unknown) {
        reportError(e);
      }
    }
    retainedBuffers.set(resolvedDataID, wrap);
  }

  if (!versioned) {
    return;
  }
  for (const [dataID, wrap] of retainedBuffers) {
    if (touched.has(dataID)) {
      continue;
    }
    try {
      wrap[Symbol.dispose]();
    } catch (e: unknown) {
      reportError(e);
    }
    retainedBuffers.delete(dataID);
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
    case "memoizedIntermediate": {
      // When the writing invocation executed, its fresh buffer wins; when it
      // was skipped, the buffer retained by a previous run serves instead.
      const container = dataSlot.disposableWrapContainer;
      if (container.isInitialized) {
        return container.managedObject.body as T;
      }
      const retainedWrap = dataSlot.retainedWrap;
      if (retainedWrap == null) {
        throw new LogicError(
          `memoized intermediate is not available for handle: ${handle}`,
        );
      }
      return retainedWrap.body as T;
    }
    case "destination":
    case "externalIntermediate": {
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
    case "externalIntermediate":
      break;
    case "memoizedIntermediate":
      // Not reference-counted: the wrap survives the whole run to be
      // retained in the context.
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
    case "intermediate":
    case "memoizedIntermediate": {
      const disposableWrap = dataSlot.provide();
      dataSlot.disposableWrapContainer.initialize(disposableWrap);
      return disposableWrap.body as T;
    }
    case "destination":
    case "externalIntermediate": {
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
        // On a successful run, every consumer has released its reference, so
        // a non-freed container here is a dangling handle no invocation
        // consumed.
        if (!dataSlot.disposableWrapContainer.isFreed) {
          hasLeak = true;
        }
        dataSlot.disposableWrapContainer.forceCleanUp();
        break;
      case "memoizedIntermediate":
        // An uninitialized container is normal here: the writing invocation
        // may have been skipped in favor of the retained buffer. On a
        // successful run, the retention pass has already extracted every
        // produced wrap, so an initialized non-freed container only remains
        // when the run failed.
        if (
          dataSlot.disposableWrapContainer.isInitialized &&
          !dataSlot.disposableWrapContainer.isFreed
        ) {
          hasLeak = true;
        }
        dataSlot.disposableWrapContainer.forceCleanUp();
        break;
      case "destination":
        break;
      case "externalIntermediate":
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
