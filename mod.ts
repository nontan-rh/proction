import { AssertionError, BaseError, LogicError, unreachable } from "./error.ts";
import { Brand } from "./brand.ts";
import { Provided } from "./provider.ts";
import { Box } from "./box.ts";
import { Rc } from "./rc.ts";
export { ProviderWrap } from "./provider.ts";

function idGenerator<T>(transform: (x: number) => T): () => T {
  let counter = 0;
  return () => {
    counter += 1;
    return transform(counter);
  };
}

const parentPlanKey = Symbol("parentPlan");
const handleIdKey = Symbol("handleId");
const phantomDataKey = Symbol("phantomData");
type HandleId = Brand<number, "handleID">;
type Handle<T> = {
  [parentPlanKey]: Plan;
  [handleIdKey]: HandleId;
  [phantomDataKey]: () => T;
};
type UntypedHandle = Handle<unknown>;

export function getPlan(
  ...handles: (
    | UntypedHandle
    | readonly UntypedHandle[]
  )[]
): Plan {
  function isHandle(
    x:
      | UntypedHandle
      | readonly UntypedHandle[],
  ): x is UntypedHandle {
    return parentPlanKey in x;
  }

  let plan: Plan | undefined;
  for (const t of handles) {
    if (isHandle(t)) {
      const p = t[parentPlanKey];
      if (plan == null) {
        plan = p;
      } else if (p !== plan) {
        throw new BaseError("Plan inconsitent");
      }
    } else {
      for (const h of t) {
        const p = h[parentPlanKey];
        if (plan == null) {
          plan = p;
        } else if (p !== plan) {
          throw new BaseError("Plan inconsitent");
        }
      }
    }
  }

  if (plan == null) {
    throw new BaseError("Failed to detect plan");
  }

  return plan;
}

type HandleSet<T> = {
  [key in keyof T]: Handle<T[key]>;
};
type BodySet<T> = {
  [key in keyof T]: BodyType<T[key]>;
};
type BodyType<T> = T extends Handle<infer X> ? X : never;

export function singleOutputAction<
  I extends readonly unknown[],
  O,
>(
  f: (output: O, ...inputArgs: I) => void,
): (
  output: Handle<O>,
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const g = (
    output: Handle<O>,
    ...inputArgs: HandleSet<I>
  ) => {
    const plan = getPlan(output, ...inputArgs);

    const id = plan.generateInvocationID();
    const invocation: Invocation = {
      id,
      inputArgs,
      outputSet: [output],
      run: () => {
        const restoredInputs = restoreArgs(plan, inputArgs);
        const preparedOutputs = prepareOutput(plan, output);
        f(preparedOutputs, ...restoredInputs);
        decRefArray(plan, inputArgs);
        decRef(plan, output);
      },
    };
    plan.invocations.set(invocation.id, invocation);
  };

  return g;
}

export function multipleOutputAction<
  I extends readonly unknown[],
  O extends readonly unknown[],
>(
  f: (outputSet: O, ...inputArgs: I) => void,
): (
  outputSet: { [key in keyof O]: Handle<O[key]> }, // expanded for readability of inferred type
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => void {
  const g = (
    outputSet: HandleSet<O>,
    ...inputArgs: HandleSet<I>
  ) => {
    const plan = getPlan(outputSet, ...inputArgs);

    const id = plan.generateInvocationID();
    const invocation: Invocation = {
      id,
      inputArgs,
      outputSet,
      run: () => {
        const restoredInputs = restoreArgs(plan, inputArgs);
        const preparedOutputs = prepareMultipleOutput(plan, outputSet);
        f(preparedOutputs, ...restoredInputs);
        decRefArray(plan, inputArgs);
        decRefArray(plan, outputSet);
      },
    };
    plan.invocations.set(invocation.id, invocation);
  };

  return g;
}

export function singleOutputPurify<
  I extends readonly unknown[],
  O,
  A extends O,
>(
  rawAction: (
    output: Handle<O>,
    ...inputArgs: HandleSet<I>
  ) => void,
  allocator: (...inputArgs: I) => Provided<A>,
): (
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => Handle<A> {
  return (...inputArgs: HandleSet<I>): Handle<A> => {
    const plan = getPlan(...inputArgs);
    const output = intermediate(
      plan,
      () => allocator(...restoreArgs(plan, inputArgs)),
    );
    rawAction(output, ...inputArgs);
    return output;
  };
}

export function multipleOutputPurify<
  I extends readonly unknown[],
  O extends readonly unknown[],
  A extends O,
>(
  rawAction: (
    outputSet: HandleSet<O>,
    ...inputArgs: HandleSet<I>
  ) => void,
  allocators: { [key in keyof O]: (...inputArgs: I) => Provided<A[key]> },
): (
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => { [key in keyof O]: Handle<A[key]> } // expanded for readability of inferred type
{
  return (...inputArgs: HandleSet<I>): HandleSet<O> => {
    const plan = getPlan(...inputArgs);

    const partialOutputSet = [];
    for (let i = 0; i < allocators.length; i++) {
      const allocator = allocators[i];
      const handle = intermediate(
        plan,
        () => allocator(...restoreArgs(plan, inputArgs)),
      );
      partialOutputSet.push(handle);
    }
    const outputSet = partialOutputSet as HandleSet<O>;

    rawAction(outputSet, ...inputArgs);

    return outputSet;
  };
}

type InvocationID = Brand<number, "invocationID">;
interface Invocation {
  readonly id: InvocationID;
  readonly inputArgs: readonly UntypedHandle[];
  readonly outputSet: readonly UntypedHandle[];
  readonly run: () => void;
}

const reportErrorKey = Symbol("reportError");

export class Context {
  [reportErrorKey]: (e: unknown) => void;

  constructor(options?: ContextOptions) {
    const fixedOptions = { ...defaultContextOptions, ...options };

    const reportError = fixedOptions.reportError;
    this[reportErrorKey] = (e) => {
      if (reportError != null) {
        try {
          reportError(e);
        } catch {
          // no recovery
        }
      }
    };
  }

  run(planFn: (p: PlanFnParams) => void, options?: RunOptions) {
    const plan = new Plan(this);
    const runParams: PlanFnParams = {
      input: (value) => input(plan, value),
      output: (value) => output(plan, value),
    };
    planFn(runParams);
    run(plan, options);
  }
}

type ContextOptions = {
  reportError?: (e: unknown) => void;
};

const defaultContextOptions: ContextOptions = {};

type PlanFnParams = {
  input<T extends object>(value: T): Handle<T>;
  output<T extends object>(value: T): Handle<T>;
};

const undefinedFn = () => {};

class Plan {
  context: Context;
  state: PlanState;
  inputCache: WeakMap<object, UntypedHandle>;
  outputCache: WeakMap<object, UntypedHandle>;

  generateHandle: () => UntypedHandle = idGenerator((
    value,
  ) => ({
    [parentPlanKey]: this,
    [handleIdKey]: value as HandleId,
    [phantomDataKey]: undefinedFn,
  }));
  dataSlots = new Map<HandleId, DataSlot>();

  generateInvocationID = idGenerator((value) => value as InvocationID);
  invocations = new Map<InvocationID, Invocation>();

  constructor(context: Context) {
    this.context = context;
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
  allocator: () => Provided<unknown>;
  body: Rc<Box<Provided<unknown>>>;
};
type SinkSlot = { type: "sink"; body: unknown };

function input<T extends object>(plan: Plan, value: T): Handle<T> {
  const cached = plan.inputCache.get(value);
  if (cached) {
    return cached as Handle<T>;
  }

  // validation
  if (plan.outputCache.has(value)) {
    throw new BaseError("the value is already specified as output");
  }

  const handle = plan.generateHandle();

  plan.dataSlots.set(handle[handleIdKey], {
    type: "source",
    body: value,
  });
  plan.inputCache.set(value, handle);

  return handle as Handle<T>;
}

function output<T extends object>(plan: Plan, value: T): Handle<T> {
  const cached = plan.outputCache.get(value);
  if (cached) {
    return cached as Handle<T>;
  }

  // validation
  if (plan.inputCache.has(value)) {
    throw new BaseError("the value is already specified as input");
  }

  const handle = plan.generateHandle();

  plan.dataSlots.set(handle[handleIdKey], {
    type: "sink",
    body: value,
  });
  plan.outputCache.set(value, handle);

  return handle as Handle<T>;
}

function intermediate<T>(
  plan: Plan,
  allocator: () => Provided<T>,
): Handle<T> {
  const handle = plan.generateHandle();

  plan.dataSlots.set(handle[handleIdKey], {
    type: "intermediate",
    body: new Rc(new Box(), (x) => {
      if (!x.isSet) {
        return;
      }
      x.value.release();
      x.clear();
    }, plan.context[reportErrorKey]),
    allocator,
  });

  return handle as Handle<T>;
}

export type RunOptions = {
  assertNoLeak?: boolean;
};

const defaultRunOptions: RunOptions = {
  assertNoLeak: false,
};

function run(
  plan: Plan,
  options?: RunOptions,
): void {
  const fixedOptions = { ...defaultRunOptions, ...options };

  if (plan.state !== "initial") {
    throw new BaseError(
      `invalid state precondition for run(): ${plan.state}`,
    );
  }

  try {
    plan.state = "planning";

    const invocations = prepareInvocations(plan);
    prepareDataSlots(plan, invocations);

    plan.state = "running";

    for (const invocation of invocations) {
      invocation.run();
    }

    if (fixedOptions.assertNoLeak) {
      assertNoLeak(plan);
    }

    plan.state = "done";
  } finally {
    if (plan.state !== "done") {
      plan.state = "error";
    }
  }
}

function prepareInvocations(
  plan: Plan,
): Invocation[] {
  type ToposortState = "temporary" | "permanent";

  const result: Invocation[] = [];

  const outputToInvocation = new Map<HandleId, Invocation>();
  for (const invocation of plan.invocations.values()) {
    for (const output of invocation.outputSet) {
      if (outputToInvocation.has(output[handleIdKey])) {
        throw new LogicError(
          "the output have two parent invocations",
        );
      }
      outputToInvocation.set(output[handleIdKey], invocation);
    }
  }

  const visitedInvocations = new Map<InvocationID, ToposortState>();
  function visitInvocation(invocation: Invocation): void {
    const invocationID = invocation.id;

    const state = visitedInvocations.get(invocationID);
    if (state === "permanent") {
      return;
    } else if (state === "temporary") {
      throw new LogicError("the computation graph has a cycle");
    }

    visitedInvocations.set(invocationID, "temporary");

    for (const inputArg of invocation.inputArgs) {
      visitHandle(inputArg[handleIdKey]);
    }

    visitedInvocations.set(invocationID, "permanent");

    result.unshift(invocation);
  }

  function visitHandle(handleId: HandleId): void {
    const dataSlot = plan.dataSlots.get(handleId);
    if (dataSlot != null) {
      const type = dataSlot.type;
      switch (type) {
        case "source":
          return;
        case "intermediate":
        case "sink":
          break;
        default:
          return unreachable(type);
      }
    }

    const parentInvocation = outputToInvocation.get(handleId);
    if (parentInvocation == null) {
      throw new LogicError(
        `parent invocation not found for handle: ${handleId}`,
      );
    }

    visitInvocation(parentInvocation);
  }

  for (const handleId of plan.dataSlots.keys()) {
    const dataSlot = plan.dataSlots.get(handleId);
    if (
      dataSlot == null ||
      (dataSlot.type !== "sink" && dataSlot.type !== "intermediate")
    ) {
      continue;
    }
    visitHandle(handleId);
  }

  return result.reverse();
}

function prepareDataSlots(
  plan: Plan,
  invocations: Invocation[],
): void {
  for (const invocation of invocations) {
    // reserve intermediate inputs
    for (const inputArg of invocation.inputArgs) {
      const dataSlot = plan.dataSlots.get(inputArg[handleIdKey]);
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
          dataSlot.body.incRef();
          break;
        case "sink":
          break;
        default:
          return unreachable(type);
      }
    }
  }
}

function restoreArgs<T extends readonly UntypedHandle[]>(
  plan: Plan,
  argHandles: T,
): BodySet<T> {
  const restored = [];
  for (const argHandle of argHandles) {
    restored.push(restore(plan, argHandle));
  }
  return restored as BodySet<T>;
}

function restore<T>(plan: Plan, handle: Handle<T>): T {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
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
      if (!dataSlot.body.body.isSet) {
        throw new LogicError("data slot is not set yet");
      }
      return dataSlot.body.body.value.body as T;
    case "sink": {
      const body = dataSlot.body;
      return body as T;
    }
    default:
      return unreachable(type);
  }
}

function decRefArray(plan: Plan, handleSet: readonly UntypedHandle[]): void {
  for (const handle of handleSet) {
    decRef(plan, handle);
  }
}

function decRef(plan: Plan, handle: UntypedHandle): void {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
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
      dataSlot.body.decRef();
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
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new LogicError("data slot not found");
  }

  const type = dataSlot.type;
  switch (type) {
    case "source":
      throw new LogicError(`unexpected data slot type: ${type}`);
    case "intermediate": {
      if (dataSlot.body.isFreed) {
        throw new LogicError("data slot is already freed");
      }
      if (dataSlot.body.body.isSet) {
        throw new LogicError("data slot is already set");
      }
      const body = dataSlot.allocator();
      dataSlot.body.body.value = body;
      return body.body as T;
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
  handleSet: T,
): BodySet<T> {
  const partialPrepared = [];
  for (let i = 0; i < handleSet.length; i++) {
    partialPrepared.push(prepareOutput(
      plan,
      handleSet[i],
    ));
  }
  return partialPrepared as BodySet<T>;
}

function assertNoLeak(plan: Plan) {
  for (const dataSlot of plan.dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        break;
      case "intermediate":
        if (!dataSlot.body.isFreed) {
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
