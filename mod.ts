import {
  SubFunAssertionError,
  SubFunError,
  SubFunLogicError,
  unreachable,
} from "./error.ts";
import { Brand } from "./brand.ts";
import { Provided, Provider, ProviderWrap } from "./provider.ts";
import { Box } from "./box.ts";
import { Rc } from "./rc.ts";

type ObjectKey = string | number | symbol;

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
    | Record<ObjectKey, UntypedHandle>
    | UntypedHandle[]
  )[]
): Plan {
  function isHandle(
    x: UntypedHandle | Record<ObjectKey, UntypedHandle> | UntypedHandle[],
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
        throw new SubFunError("Plan inconsitent");
      }
    } else {
      for (const k in t) {
        const p = t[k][parentPlanKey];
        if (plan == null) {
          plan = p;
        } else if (p !== plan) {
          throw new SubFunError("Plan inconsitent");
        }
      }
    }
  }

  if (plan == null) {
    throw new SubFunError("Failed to detect plan");
  }

  return plan;
}

type UntypedHandleSet<T> = {
  [key in keyof T]: UntypedHandle;
};
type HandleSet<T> = {
  [key in keyof T]: Handle<T[key]>;
};

const outputModeSingle = "single" as const;
const outputModeNamed = "named" as const;

type SingleOutputAction<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, unknown, unknown>,
> = {
  outputMode: typeof outputModeSingle;
  f(output: OutputType<O>, ...inputArgs: I): void; // bivariant
  o: O;
};
type NamedOutputAction<I extends readonly unknown[], O extends ParamSpecSet> = {
  outputMode: typeof outputModeNamed;
  f(outputSet: OutputSet<O>, ...inputArgs: I): void; // bivariant
  o: O;
};
type SingleOutputUntypedAction = {
  outputMode: typeof outputModeSingle;
  f(output: unknown, ...inputArgs: readonly unknown[]): void; // bivariant
  o: TypeSpec<unknown, unknown, unknown>;
};
type NamedOutputUntypedAction = {
  outputMode: typeof outputModeNamed;
  f(outputSet: unknown, ...inputArgs: readonly unknown[]): void; // bivariant
  o: ParamSpecSet;
};
type UntypedAction = SingleOutputUntypedAction | NamedOutputUntypedAction;

const actionKey = Symbol("action");
type SingleOutputActionMeta<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, unknown, unknown>,
> = {
  [actionKey]: SingleOutputAction<I, O>;
};
type NamedOutputActionMeta<
  I extends readonly unknown[],
  O extends ParamSpecSet,
> = {
  [actionKey]: NamedOutputAction<I, O>;
};

export function singleOutputAction<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, unknown, unknown>,
>(
  o: O,
  f: (output: OutputType<O>, ...inputArgs: I) => void,
):
  & ((
    output: Handle<OutputType<O>>, // expanded for readability of inferred type
    ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
  ) => void)
  & SingleOutputActionMeta<I, O> {
  const action: SingleOutputAction<I, O> = {
    outputMode: outputModeSingle,
    f,
    o,
  };

  const g = (
    output: Handle<OutputType<O>>,
    ...inputArgs: HandleSet<I>
  ) => {
    const plan = getPlan(output, ...inputArgs);

    const id = plan.generateInvocationID();
    const invocation: Invocation = {
      id,
      outputMode: outputModeSingle,
      action,
      inputArgs,
      output,
    };
    plan.invocations.set(invocation.id, invocation);
  };
  g[actionKey] = action;

  return g;
}

export function namedOutputAction<
  I extends readonly unknown[],
  O extends ParamSpecSet,
>(
  o: O,
  f: (outputSet: OutputSet<O>, ...inputArgs: I) => void,
):
  & ((
    outputSet: { [key in keyof O]: Handle<OutputType<O[key]>> }, // expanded for readability of inferred type
    ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
  ) => void)
  & NamedOutputActionMeta<I, O> {
  const action: NamedOutputAction<I, O> = {
    outputMode: outputModeNamed,
    f,
    o,
  };

  const g = (
    outputSet: HandleSet<OutputSet<O>>,
    ...inputArgs: HandleSet<I>
  ) => {
    const plan = getPlan(outputSet, ...inputArgs);

    const id = plan.generateInvocationID();
    const invocation: Invocation = {
      id,
      outputMode: outputModeNamed,
      action,
      inputArgs,
      outputSet,
    };
    plan.invocations.set(invocation.id, invocation);
  };
  g[actionKey] = action;

  return g;
}

export function singleOutputPurify<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, unknown, unknown>,
>(
  rawAction:
    & ((
      output: Handle<OutputType<O>>,
      ...inputArgs: HandleSet<I>
    ) => void)
    & SingleOutputActionMeta<I, O>,
): (
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => Handle<InputType<O>> // expanded for readability of inferred type
{
  return (...inputArgs: HandleSet<I>): Handle<InputType<O>> => {
    const plan = getPlan(...inputArgs);
    const output = plan.generateHandle() as Handle<OutputType<O>>;
    rawAction(output, ...inputArgs);
    return output;
  };
}

export function namedOutputPurify<
  I extends readonly unknown[],
  O extends ParamSpecSet,
>(
  rawAction:
    & ((
      outputSet: HandleSet<OutputSet<O>>,
      ...inputArgs: HandleSet<I>
    ) => void)
    & NamedOutputActionMeta<I, O>,
): (
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => { [key in keyof O]: Handle<InputType<O[key]>> } // expanded for readability of inferred type
{
  const action = rawAction[actionKey];
  return (...inputArgs: HandleSet<I>): HandleSet<InputSet<O>> => {
    const plan = getPlan(...inputArgs);

    const partialOutputSet: Partial<HandleSet<OutputSet<O>>> = {};
    for (const key in action.o) {
      const handle = plan.generateHandle() as HandleSet<
        OutputSet<O>
      >[typeof key];
      partialOutputSet[key] = handle;
    }
    const outputSet = partialOutputSet as HandleSet<OutputSet<O>>;

    rawAction(outputSet, ...inputArgs);

    return outputSet;
  };
}

type InvocationID = Brand<number, "invocationID">;
type Invocation = SingleOutputInvocation | NamedOutputInvocation;
type SingleOutputInvocation = {
  id: InvocationID;
  outputMode: typeof outputModeSingle;
  action: SingleOutputUntypedAction;
  inputArgs: readonly UntypedHandle[];
  output: UntypedHandle;
};
type NamedOutputInvocation = {
  id: InvocationID;
  outputMode: typeof outputModeNamed;
  action: NamedOutputUntypedAction;
  inputArgs: readonly UntypedHandle[];
  outputSet: Record<ObjectKey, UntypedHandle>;
};

export class Context {
  run(planFn: (p: PlanFnParams) => void, options?: RunOptions) {
    const plan = new Plan();
    const runParams: PlanFnParams = {
      input: (value) => input(plan, value),
      output: (value) => output(plan, value),
    };
    planFn(runParams);
    run(plan, options);
  }
}

type PlanFnParams = {
  input<T extends object>(value: T): Handle<T>;
  output<T extends object>(value: T): Handle<T>;
};

const undefinedFn = () => {};

class Plan {
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

  constructor() {
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
  body: Rc<Box<Provided<unknown>>>;
};
type SinkSlot = { type: "sink"; body: unknown };

export type ParamSpecSet = {
  [key: ObjectKey]: TypeSpec<unknown, unknown, unknown>;
};

const inputPhantomTypeKey = Symbol("inputType");
const outputPhantomTypeKey = Symbol("outputType");
export type TypeSpec<T extends I & O, I, O> = {
  provider: ProviderWrap<T>;
  [inputPhantomTypeKey]: I;
  [outputPhantomTypeKey]: O;
};

type ProvidedType<S extends TypeSpec<unknown, unknown, unknown>> =
  S["provider"] extends Provider<infer X> ? X : never;
type InputType<S extends TypeSpec<unknown, unknown, unknown>> =
  S[typeof inputPhantomTypeKey];
type OutputType<S extends TypeSpec<unknown, unknown, unknown>> =
  S[typeof outputPhantomTypeKey];

type InputSet<S extends ParamSpecSet> = {
  [key in keyof S]: InputType<S[key]>;
};
type OutputSet<S extends ParamSpecSet> = {
  [key in keyof S]: OutputType<S[key]>;
};

export function typeSpec<T extends I & O, I = T, O = T>(
  provider: Provider<T>,
): TypeSpec<T, I, O> {
  return { provider: new ProviderWrap(provider) } as TypeSpec<T, I, O>;
}

function input<T extends object>(plan: Plan, value: T): Handle<T> {
  const cached = plan.inputCache.get(value);
  if (cached) {
    return cached as Handle<T>;
  }

  // validation
  if (plan.outputCache.has(value)) {
    throw new SubFunError("the value is already specified as output");
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
    throw new SubFunError("the value is already specified as input");
  }

  const handle = plan.generateHandle();

  plan.dataSlots.set(handle[handleIdKey], {
    type: "sink",
    body: value,
  });
  plan.outputCache.set(value, handle);

  return handle as Handle<T>;
}

export type RunOptions = {
  assertNoLeak: boolean;
};

const defaultRunOptions: RunOptions = {
  assertNoLeak: false,
};

function run(
  plan: Plan,
  options?: Partial<RunOptions>,
): void {
  const fixedOptions = { ...defaultRunOptions, ...options };

  if (plan.state !== "initial") {
    throw new SubFunError(
      `invalid state precondition for run(): ${plan.state}`,
    );
  }

  try {
    plan.state = "planning";

    const invocations = prepareInvocations(plan);
    prepareDataSlots(plan, invocations);

    plan.state = "running";

    for (const invocation of invocations) {
      const outputMode = invocation.outputMode;
      switch (outputMode) {
        case "single": {
          const action = invocation.action;
          const restoredInputs = restoreArgs(plan, invocation.inputArgs);
          const preparedOutputs = prepareOutput(
            plan,
            action.o,
            invocation.output,
          );
          action.f(preparedOutputs, ...restoredInputs);
          decRefArray(plan, invocation.inputArgs);
          decRef(plan, invocation.output);
          break;
        }
        case "named": {
          const action = invocation.action;
          const restoredInputs = restoreArgs(plan, invocation.inputArgs);
          const preparedOutputs = prepareNamedOutput(
            plan,
            action.o,
            invocation.outputSet,
          );
          action.f(preparedOutputs, ...restoredInputs);
          decRefArray(plan, invocation.inputArgs);
          decRefSet(plan, invocation.outputSet);
          break;
        }
        default:
          return unreachable(outputMode);
      }
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
    const outputMode = invocation.outputMode;
    switch (outputMode) {
      case "single": {
        const output = invocation.output;
        if (outputToInvocation.has(output[handleIdKey])) {
          throw new SubFunLogicError("the output have two parent invocations");
        }
        outputToInvocation.set(output[handleIdKey], invocation);
        break;
      }
      case "named":
        for (const outputKey in invocation.outputSet) {
          const output = invocation.outputSet[outputKey];
          if (outputToInvocation.has(output[handleIdKey])) {
            throw new SubFunLogicError(
              "the output have two parent invocations",
            );
          }
          outputToInvocation.set(output[handleIdKey], invocation);
        }
        break;
      default:
        unreachable(outputMode);
    }
  }

  const visitedInvocations = new Map<InvocationID, ToposortState>();
  function visitInvocation(invocation: Invocation): void {
    const invocationID = invocation.id;

    const state = visitedInvocations.get(invocationID);
    if (state === "permanent") {
      return;
    } else if (state === "temporary") {
      throw new SubFunLogicError("the computation graph has a cycle");
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
          throw new SubFunLogicError(`unexpected data slot type: ${type}`);
        case "sink":
          break;
        default:
          return unreachable(type);
      }
    }

    const parentInvocation = outputToInvocation.get(handleId);
    if (parentInvocation == null) {
      throw new SubFunLogicError(
        `parent invocation not found for handle: ${handleId}`,
      );
    }

    visitInvocation(parentInvocation);
  }

  for (const handleId of plan.dataSlots.keys()) {
    const dataSlot = plan.dataSlots.get(handleId);
    if (dataSlot == null || dataSlot.type !== "sink") {
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
        throw new SubFunLogicError(
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

    // create intermediate outputs if needed
    const outputMode = invocation.outputMode;
    switch (outputMode) {
      case "single": {
        prepareIntermediateOutput(plan, invocation.output);
        break;
      }
      case "named": {
        prepareNamedIntermediateOutput(
          plan,
          invocation.outputSet,
        );
        break;
      }
      default:
        return unreachable(outputMode);
    }
  }
}

function prepareIntermediateOutput(
  plan: Plan,
  output: UntypedHandle,
) {
  const dataSlot = plan.dataSlots.get(output[handleIdKey]);

  if (dataSlot == null) {
    plan.dataSlots.set(output[handleIdKey], {
      type: "intermediate",
      body: new Rc(new Box(), (x) => {
        if (!x.isSet) {
          return;
        }
        x.value.release();
      }, console.error),
    });
  } else {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        throw new SubFunLogicError(`unexpected data slot type: ${type}`);
      case "intermediate":
        throw new SubFunLogicError(`unexpected data slot type: ${type}`);
      case "sink":
        break;
      default:
        return unreachable(type);
    }
  }
}

function prepareNamedIntermediateOutput<T extends ParamSpecSet>(
  plan: Plan,
  handleSet: HandleSet<OutputSet<T>>,
) {
  for (const outputKey in handleSet) {
    prepareIntermediateOutput(
      plan,
      handleSet[outputKey],
    );
  }
}

function restoreArgs(
  plan: Plan,
  argHandles: readonly UntypedHandle[],
): unknown[] {
  const restored: unknown[] = [];
  for (const argHandle of argHandles) {
    restored.push(restore(plan, argHandle));
  }
  return restored;
}

function restore<T>(plan: Plan, handle: Handle<T>): T {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new SubFunLogicError(
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
        throw new SubFunLogicError("data slot is not set yet");
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

function decRefSet<T>(plan: Plan, handleSet: HandleSet<T>): void {
  for (const key in handleSet) {
    decRef(plan, handleSet[key]);
  }
}

function decRef<T>(plan: Plan, handle: Handle<T>): void {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new SubFunLogicError(
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

function prepareOutput<T extends TypeSpec<unknown, unknown, unknown>>(
  plan: Plan,
  typeSpec: T,
  handle: Handle<OutputType<T>>,
): OutputType<T> {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new SubFunLogicError("data slot not found");
  }

  const type = dataSlot.type;
  switch (type) {
    case "source":
      throw new SubFunLogicError(`unexpected data slot type: ${type}`);
    case "intermediate": {
      if (dataSlot.body.body.isSet) {
        throw new SubFunLogicError("data slot is already set");
      }
      const body = typeSpec.provider.acquire();
      dataSlot.body.body.value = body;
      return body.body;
    }
    case "sink": {
      const body = dataSlot.body;
      return body as OutputType<T>;
    }
    default:
      return unreachable(type);
  }
}

function prepareNamedOutput<T extends ParamSpecSet>(
  plan: Plan,
  paramSpecSet: T,
  handleSet: HandleSet<OutputSet<T>>,
): OutputSet<T> {
  const partialPrepared: Partial<OutputSet<T>> = {};
  for (const key in handleSet) {
    partialPrepared[key] = prepareOutput(
      plan,
      paramSpecSet[key],
      handleSet[key],
    );
  }
  return partialPrepared as OutputSet<T>;
}

function assertNoLeak(plan: Plan) {
  for (const dataSlot of plan.dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "source":
        break;
      case "intermediate":
        if (!dataSlot.body.isFreed) {
          throw new SubFunAssertionError(
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
