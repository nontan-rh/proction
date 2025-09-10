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
 * const addProc = proc()(function add(output: number[], lht: number[], rht: number[]) {
 *   for (let i = 0; i < output.length; i++) {
 *     output[i] = lht[i] + rht[i];
 *   }
 * });
 *
 * const addFunc = toFunc(addProc, (lht, _rht) => pool.acquire(lht.length));
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

/**
 * Creates an indirect procedure which has a single output. Can be used as a decorator.
 * @typeparam O The output type of the indirect procedure.
 * @typeparam I The list of input types of the indirect procedure.
 * @param f The body function of the indirect procedure.
 * @param decoratorContext The decorator context.
 * @param procOptions The options of the proc.
 * @returns A decorator to generate an indirect procedure.
 */
export function proc(
  procOptions?: ProcOptions,
): <O, I extends readonly unknown[]>(
  f: (output: O, ...inputs: I) => void | Promise<void>,
  decoratorContext?: DecoratorContext,
) => (
  output: Handle<O>,
  ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  return function decoratorFn<
    O,
    I extends readonly unknown[],
  >(
    f: (output: O, ...inputs: I) => void | Promise<void>,
    _decoratorContext?: DecoratorContext,
  ): (
    output: Handle<O>,
    ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
  ) => void {
    const g = (
      output: Handle<O>,
      ...inputs: MappedHandleType<I>
    ) => {
      const plan = getPlan(output, ...inputs);

      const id = plan[internalPlanKey].generateInvocationID();
      const invocation: Invocation = {
        id,
        inputs,
        outputs: [output],
        run: async () => {
          const restoredInputs = restoreInputs(plan, inputs);
          const preparedOutputs = prepareOutput(plan, output);
          await f(preparedOutputs, ...restoredInputs);
          decRefArray(plan, inputs);
          decRef(plan, output);
        },
        middlewares,
        // calculated on run preparation
        next: [],
        numBlockers: 0,
        numResolvedBlockers: 0,
      };
      plan[internalPlanKey].invocations.set(invocation.id, invocation);
    };

    return g;
  };
}

/**
 * Creates an indirect procedure which has multiple outputs. Can be used as a decorator.
 * @typeparam O The list of output types of the indirect procedure.
 * @typeparam I The list of input types of the indirect procedure.
 * @param f The body function of the indirect procedure.
 * @param decoratorContext The decorator context.
 * @param procOptions The options of the proc.
 * @returns A decorator to generate an indirect procedure.
 */
export function procN(
  procOptions?: ProcOptions,
): <
  O extends readonly unknown[],
  I extends readonly unknown[],
>(
  f: (outputs: O, ...inputs: I) => void | Promise<void>,
  decoratorContext?: DecoratorContext,
) => (
  outputs: { [key in keyof O]: Handle<O[key]> }, // expanded for readability of inferred type
  ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = procOptions?.middlewares ?? [];

  return function decoratorFn<
    O extends readonly unknown[],
    I extends readonly unknown[],
  >(
    f: (outputs: O, ...inputs: I) => void | Promise<void>,
    _decoratorContext?: DecoratorContext,
  ): (
    outputs: { [key in keyof O]: Handle<O[key]> }, // expanded for readability of inferred type
    ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
  ) => void {
    const g = (
      outputs: MappedHandleType<O>,
      ...inputs: MappedHandleType<I>
    ) => {
      const plan = getPlan(outputs, ...inputs);

      const id = plan[internalPlanKey].generateInvocationID();
      const invocation: Invocation = {
        id,
        inputs,
        outputs,
        run: async () => {
          const restoredInputs = restoreInputs(plan, inputs);
          const preparedOutputs = prepareMultipleOutput(plan, outputs);
          await f(preparedOutputs, ...restoredInputs);
          decRefArray(plan, inputs);
          decRefArray(plan, outputs);
        },
        middlewares,
        // calculated on run preparation
        next: [],
        numBlockers: 0,
        numResolvedBlockers: 0,
      };
      plan[internalPlanKey].invocations.set(invocation.id, invocation);
    };

    return g;
  };
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
 * An internal type to represent an invocation. Invocation represents a running procedure or function.
 * It is the unit of execution of a Proction program.
 */
interface Invocation {
  readonly id: InvocationID;
  readonly inputs: readonly UntypedHandle[];
  readonly outputs: readonly UntypedHandle[];
  readonly run: () => Promise<void>;
  readonly middlewares: MiddlewareFn[];
  readonly next: Invocation[];
  numBlockers: number;
  numResolvedBlockers: number;
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

      const run = applyMiddlewares(invocation.run, invocation.middlewares);
      runningInvocations.add(invocation.id);
      plan.context[contextOptionsKey].scheduler.spawn(run).then(() => {
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
        runningInvocations.delete(invocation.id);

        notify();
      });
    }

    if (plan.context[contextOptionsKey].assertNoLeak) {
      assertNoLeak(plan);
    }

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
          `dataSlot not found for handle: ${source}`,
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
function assertNoLeak(plan: Plan) {
  for (const dataSlot of plan[internalPlanKey].dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        break;
      case "intermediate":
        if (!dataSlot.disposableWrapContainer.isFreed) {
          throw new AssertionError(
            "intermediate data slot is not freed",
          );
        }
        break;
      case "destination":
        break;
      default:
        return unreachable(type);
    }
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
