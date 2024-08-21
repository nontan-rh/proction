import {
  AssertionError,
  LogicError,
  PreconditionError,
  unreachable,
} from "./_error.ts";
import { Brand } from "./_brand.ts";
import { AllocatorResult } from "./_provider.ts";
import { DelayedRc } from "./_delayedrc.ts";
import { idGenerator } from "./_idgenerator.ts";
export { type AllocatorResult, ProviderWrap } from "./_provider.ts";

const parentPlanKey = Symbol("parentPlan");
const handleIdKey = Symbol("handleId");
const phantomDataKey = Symbol("phantomData");
type HandleId = Brand<number, "handleID">;
export type Handle<T> = {
  [parentPlanKey]: Plan;
  [handleIdKey]: HandleId;
  [phantomDataKey]: () => T;
};
type UntypedHandle = Handle<unknown>;

type MappedHandleType<T> = {
  [key in keyof T]: Handle<T[key]>;
};
type MappedBodyType<T> = {
  [key in keyof T]: BodyType<T[key]>;
};
type BodyType<T> = T extends Handle<infer X> ? X : never;

function isHandle(x: object): x is UntypedHandle {
  return parentPlanKey in x;
}

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
        throw new PreconditionError("Plan inconsitent");
      }
    } else {
      for (const h of t) {
        const p = h[parentPlanKey];
        if (plan == null) {
          plan = p;
        } else if (p !== plan) {
          throw new PreconditionError("Plan inconsitent");
        }
      }
    }
  }

  if (plan == null) {
    throw new PreconditionError("Failed to detect plan");
  }

  return plan;
}

type InvocationBody = () => Promise<void>;
export type Middleware = (next: () => Promise<void>) => Promise<void>;

type ActionOptions = {
  middlewares?: Middleware[];
};

export function proction(
  actionOptions?: ActionOptions,
): <O, I extends readonly unknown[]>(
  f: (output: O, ...inputs: I) => void | Promise<void>,
  decolatorContext?: DecoratorContext,
) => (
  output: Handle<O>,
  ...inputs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const middlewares = actionOptions?.middlewares ?? [];

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

export function proctionN(
  actionOptions?: ActionOptions,
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
  const middlewares = actionOptions?.middlewares ?? [];

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

export function purify<
  O,
  I extends readonly UntypedHandle[],
  A extends O,
>(
  rawAction: (
    output: Handle<O>,
    ...inputs: I
  ) => void,
  allocator: (...inputs: MappedBodyType<I>) => AllocatorResult<A>,
): (
  ...inputs: I
) => Handle<A> {
  return (...inputs: I): Handle<A> => {
    const plan = getPlan(...inputs);
    const output = intermediate(
      plan,
      () => allocator(...restoreInputs(plan, inputs)),
    );
    rawAction(output, ...inputs);
    return output;
  };
}

export function purifyN<
  O extends readonly unknown[],
  I extends readonly UntypedHandle[],
  A extends O,
>(
  rawAction: (
    outputs: MappedHandleType<O>,
    ...inputs: I
  ) => void,
  allocators: {
    [key in keyof O]: (...inputs: MappedBodyType<I>) => AllocatorResult<A[key]>;
  },
): (
  ...inputs: I
) => { [key in keyof O]: Handle<A[key]> } // expanded for readability of inferred type
{
  return (...inputs: I): MappedHandleType<O> => {
    const plan = getPlan(...inputs);

    const partialOutputs = [];
    for (let i = 0; i < allocators.length; i++) {
      const allocator = allocators[i];
      const handle = intermediate(
        plan,
        () => allocator(...restoreInputs(plan, inputs)),
      );
      partialOutputs.push(handle);
    }
    const outputs = partialOutputs as MappedHandleType<O>;

    rawAction(outputs, ...inputs);

    return outputs;
  };
}

type InvocationID = Brand<number, "invocationID">;
interface Invocation {
  readonly id: InvocationID;
  readonly inputs: readonly UntypedHandle[];
  readonly outputs: readonly UntypedHandle[];
  readonly run: () => Promise<void>;
  readonly middlewares: Middleware[];
  readonly next: Invocation[];
  numBlockers: number;
  numResolvedBlockers: number;
}

const contextOptionsKey = Symbol("contextOptions");

export class Context {
  [contextOptionsKey]: ContextOptions;

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

  async run(bodyFn: (inner: InnerContext) => void, options?: RunOptions) {
    const plan: Plan = {
      context: this,
      [internalPlanKey]: new InternalPlan(this),
    };
    plan[internalPlanKey].plan = plan;
    const runParams: InnerContext = {
      source: (value) => source(plan, value),
      sink: (value) => sink(plan, value),
      intermediate: (allocator) => intermediate(plan, allocator),
    };
    bodyFn(runParams);
    await run(plan, options);
  }
}

export type ContextOptions = {
  reportError: (e: unknown) => void;
  assertNoLeak: boolean;
};

const defaultContextOptions: ContextOptions = {
  reportError: () => {},
  assertNoLeak: false,
};

type InnerContext = {
  source<T extends object>(value: T): Handle<T>;
  sink<T extends object>(value: T): Handle<T>;
  intermediate<T>(allocator: () => AllocatorResult<T>): Handle<T>;
};

const undefinedFn = () => {};

const internalPlanKey = Symbol("internalPlan");
type Plan = {
  readonly context: Context;
  [internalPlanKey]: InternalPlan;
};

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
  dataSlots = new Map<HandleId, DataSlot>();

  generateInvocationID = idGenerator((value) => value as InvocationID);
  invocations = new Map<InvocationID, Invocation>();

  constructor(context: Context) {
    this.context = context;
    this.plan = undefined!;
    this.state = "initial";
    this.inputCache = new WeakMap();
    this.outputCache = new WeakMap();
  }
}

type PlanState = "initial" | "planning" | "running" | "done" | "error";

type DataSlot =
  | SourceSlot
  | IntermediateSlot
  | SinkSlot;
type SourceSlot = { type: "source"; body: unknown };
type IntermediateSlot = {
  type: "intermediate";
  allocator: () => AllocatorResult<unknown>;
  allocatorResultContainer: DelayedRc<AllocatorResult<unknown>>;
};
type SinkSlot = { type: "sink"; body: unknown };

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

function sink<T extends object>(plan: Plan, value: T): Handle<T> {
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
    type: "sink",
    body: value,
  });
  plan[internalPlanKey].outputCache.set(value, handle);

  return handle as Handle<T>;
}

function intermediate<T>(
  plan: Plan,
  allocator: () => AllocatorResult<T>,
): Handle<T> {
  const handle = plan[internalPlanKey].generateHandle();

  plan[internalPlanKey].dataSlots.set(handle[handleIdKey], {
    type: "intermediate",
    allocatorResultContainer: new DelayedRc((x) => {
      x[Symbol.dispose]();
    }, plan.context[contextOptionsKey].reportError),
    allocator,
  });

  return handle as Handle<T>;
}

// deno-lint-ignore ban-types
export type RunOptions = {};

const defaultRunOptions: RunOptions = {};

async function run(
  plan: Plan,
  options?: RunOptions,
): Promise<void> {
  const _mergedOptions = { ...defaultRunOptions, ...options };

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

      const run = applyMiddleware(invocation.run, invocation.middlewares);
      runningInvocations.add(invocation.id);
      run().then(() => {
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
          dataSlot.allocatorResultContainer.incRef();
          break;
        case "sink":
          break;
        default:
          return unreachable(type);
      }
    }
  }
}

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
      return dataSlot.allocatorResultContainer.managedObject.body as T;
    case "sink": {
      const body = dataSlot.body;
      return body as T;
    }
    default:
      return unreachable(type);
  }
}

function decRefArray(plan: Plan, handles: readonly UntypedHandle[]): void {
  for (const handle of handles) {
    decRef(plan, handle);
  }
}

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
      dataSlot.allocatorResultContainer.decRef();
      break;
    case "sink":
      break;
    default:
      return unreachable(type);
  }
}

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
      const allocatorResult = dataSlot.allocator();
      dataSlot.allocatorResultContainer.initialize(allocatorResult);
      return allocatorResult.body as T;
    }
    case "sink": {
      const body = dataSlot.body;
      return body as T;
    }
    default:
      return unreachable(type);
  }
}

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

function assertNoLeak(plan: Plan) {
  for (const dataSlot of plan[internalPlanKey].dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        break;
      case "intermediate":
        if (!dataSlot.allocatorResultContainer.isFreed) {
          throw new AssertionError(
            "intermediate data slot is not freed",
          );
        }
        break;
      case "sink":
        break;
      default:
        return unreachable(type);
    }
  }
}

function applyMiddleware(
  body: InvocationBody,
  middlewares: Middleware[],
): InvocationBody {
  return middlewares.reduceRight<InvocationBody>((f, m) => () => m(f), body);
}
