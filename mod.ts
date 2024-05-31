import { AssertionError, BaseError, LogicError, unreachable } from "./error.ts";
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

const outputModeSingle = "single" as const;
const outputModeMultiple = "multiple" as const;

type SingleOutputAction<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = {
  outputMode: typeof outputModeSingle;
  f(output: OutputType<O>, ...inputArgs: I): void; // bivariant
  o: O;
  allocator(
    provider: ProviderType<O>,
    ...inputArgs: I
  ): Provided<ProvidedType<O>>; // bivariant
};
type MultipleOutputAction<I extends readonly unknown[], O extends ParamSpecs> =
  {
    outputMode: typeof outputModeMultiple;
    f(outputSet: OutputSet<O>, ...inputArgs: I): void; // bivariant
    o: O;
    allocators: {
      [key in keyof OutputSet<O>]: (
        provider: ProviderType<O[key]>,
        ...inputArgs: I
      ) => Provided<ProvidedType<O[key]>>;
    };
  };

const actionKey = Symbol("action");
type SingleOutputActionMeta<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = {
  [actionKey]: SingleOutputAction<I, O>;
};
type MultipleOutputActionMeta<
  I extends readonly unknown[],
  O extends ParamSpecs,
> = {
  [actionKey]: MultipleOutputAction<I, O>;
};

export function singleOutputAction<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
>(
  o: O,
  f: (output: OutputType<O>, ...inputArgs: I) => void,
  allocator: (
    provider: ProviderType<O>,
    ...inputArgs: I
  ) => Provided<ProvidedType<O>>,
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
    allocator,
  };

  const g = (
    output: Handle<OutputType<O>>,
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
        const preparedOutputs = prepareOutput(
          plan,
          o,
          output,
          restoredInputs,
          allocator,
        );
        f(preparedOutputs, ...restoredInputs);
        decRefArray(plan, inputArgs);
        decRef(plan, output);
      },
    };
    plan.invocations.set(invocation.id, invocation);
  };
  g[actionKey] = action;

  return g;
}

export function multipleOutputAction<
  I extends readonly unknown[],
  O extends ParamSpecs,
>(
  o: O,
  f: (outputSet: OutputSet<O>, ...inputArgs: I) => void,
  allocators: {
    [key in keyof OutputSet<O>]: (
      provider: ProviderType<O[key]>,
      ...inputArgs: I
    ) => Provided<ProvidedType<O[key]>>;
  },
):
  & ((
    outputSet: { [key in keyof O]: Handle<OutputType<O[key]>> }, // expanded for readability of inferred type
    ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
  ) => void)
  & MultipleOutputActionMeta<I, O> {
  const action: MultipleOutputAction<I, O> = {
    outputMode: outputModeMultiple,
    f,
    o,
    allocators,
  };

  const g = (
    outputSet: HandleSet<OutputSet<O>>,
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
        const preparedOutputs = prepareMultipleOutput(
          plan,
          o,
          outputSet,
          restoredInputs,
          allocators,
        );
        f(preparedOutputs, ...restoredInputs);
        decRefArray(plan, inputArgs);
        decRefSet(plan, outputSet);
      },
    };
    plan.invocations.set(invocation.id, invocation);
  };
  g[actionKey] = action;

  return g;
}

export function singleOutputPurify<
  I extends readonly unknown[],
  O extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
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

export function multipleOutputPurify<
  I extends readonly unknown[],
  O extends ParamSpecs,
>(
  rawAction:
    & ((
      outputSet: HandleSet<OutputSet<O>>,
      ...inputArgs: HandleSet<I>
    ) => void)
    & MultipleOutputActionMeta<I, O>,
): (
  ...inputArgs: { [key in keyof I]: Handle<I[key]> } // expanded for readability of inferred type
) => { [key in keyof O]: Handle<InputType<O[key]>> } // expanded for readability of inferred type
{
  const action = rawAction[actionKey];
  return (...inputArgs: HandleSet<I>): HandleSet<InputSet<O>> => {
    const plan = getPlan(...inputArgs);

    const partialOutputSet = [];
    for (let i = 0; i < action.o.length; i++) {
      const handle = plan.generateHandle();
      partialOutputSet.push(handle);
    }
    const outputSet = partialOutputSet as HandleSet<OutputSet<O>>;

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

export type ParamSpecs = readonly TypeSpec<
  unknown,
  readonly unknown[],
  unknown,
  unknown
>[];

const inputPhantomTypeKey = Symbol("inputType");
const outputPhantomTypeKey = Symbol("outputType");
export type TypeSpec<T extends I & O, Args extends readonly unknown[], I, O> = {
  provider: ProviderWrap<T, Args>;
  [inputPhantomTypeKey]: I;
  [outputPhantomTypeKey]: O;
};

type ProviderType<
  S extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = S["provider"];
type ProvidedType<
  S extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = S["provider"] extends ProviderWrap<infer X, infer _> ? X : never;
type InputType<
  S extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = S[typeof inputPhantomTypeKey];
type OutputType<
  S extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
> = S[typeof outputPhantomTypeKey];

type InputSet<S extends ParamSpecs> = {
  [key in keyof S]: InputType<S[key]>;
};
type OutputSet<S extends ParamSpecs> = {
  [key in keyof S]: OutputType<S[key]>;
};

export function typeSpec<
  T extends I & O,
  Args extends readonly unknown[],
  I = T,
  O = T,
>(
  provider: Provider<T, Args>,
): TypeSpec<T, Args, I, O> {
  return { provider: new ProviderWrap(provider) } as TypeSpec<T, Args, I, O>;
}

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
          throw new LogicError(`unexpected data slot type: ${type}`);
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

    // create intermediate outputs if needed
    prepareMultipleIntermediateOutput(plan, invocation.outputSet);
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
        throw new LogicError(`unexpected data slot type: ${type}`);
      case "intermediate":
        throw new LogicError(`unexpected data slot type: ${type}`);
      case "sink":
        break;
      default:
        return unreachable(type);
    }
  }
}

function prepareMultipleIntermediateOutput<T extends ParamSpecs>(
  plan: Plan,
  handleSet: HandleSet<OutputSet<T>>,
) {
  for (const output of handleSet) {
    prepareIntermediateOutput(plan, output);
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

function decRefSet<T extends readonly unknown[]>(
  plan: Plan,
  handleSet: HandleSet<T>,
): void {
  for (const handle of handleSet) {
    decRef(plan, handle);
  }
}

function decRef<T>(plan: Plan, handle: Handle<T>): void {
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

function prepareOutput<
  T extends TypeSpec<unknown, readonly unknown[], unknown, unknown>,
  I extends readonly unknown[],
>(
  plan: Plan,
  typeSpec: T,
  handle: Handle<OutputType<T>>,
  inputs: I,
  allocator: (
    provider: ProviderType<T>,
    ...inputArgs: I
  ) => Provided<ProvidedType<T>>,
): OutputType<T> {
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new LogicError("data slot not found");
  }

  const type = dataSlot.type;
  switch (type) {
    case "source":
      throw new LogicError(`unexpected data slot type: ${type}`);
    case "intermediate": {
      if (dataSlot.body.body.isSet) {
        throw new LogicError("data slot is already set");
      }
      const body = allocator(typeSpec.provider, ...inputs);
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

function prepareMultipleOutput<
  T extends ParamSpecs,
  I extends readonly unknown[],
>(
  plan: Plan,
  paramSpecSet: T,
  handleSet: HandleSet<OutputSet<T>>,
  inputs: I,
  allocators: {
    [key in keyof OutputSet<T>]: (
      provider: ProviderType<T[key]>,
      ...inputArgs: I
    ) => Provided<ProvidedType<T[key]>>;
  },
): OutputSet<T> {
  const partialPrepared = [];
  for (let i = 0; i < handleSet.length; i++) {
    partialPrepared.push(prepareOutput(
      plan,
      paramSpecSet[i],
      handleSet[i],
      inputs,
      allocators[i],
    ));
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
