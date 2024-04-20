import {
  SubFunAssertionError,
  SubFunError,
  SubFunLogicError,
  unreachable,
} from "./error.ts";
import { Brand } from "./brand.ts";
import { Provider } from "./provider.ts";
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
  head:
    | UntypedHandle
    | (Record<ObjectKey, UntypedHandle>),
  ...tail: (
    | UntypedHandle
    | (Record<ObjectKey, UntypedHandle>)
  )[]
): Plan {
  function isHandle(
    x: UntypedHandle | (Record<ObjectKey, UntypedHandle>),
  ): x is UntypedHandle {
    return parentPlanKey in x;
  }

  let plan: Plan | undefined;
  if (isHandle(head)) {
    plan = head[parentPlanKey];
  } else {
    for (const k in head) {
      const p = head[k][parentPlanKey];
      if (plan != null && p !== plan) {
        throw new SubFunError("Plan inconsitent");
      }
      plan = p;
    }
  }

  for (const t of tail) {
    if (isHandle(t)) {
      const p = t[parentPlanKey];
      if (plan != null && p !== plan) {
        throw new SubFunError("Plan inconsitent");
      }
    } else {
      for (const k in t) {
        const p = t[k][parentPlanKey];
        if (plan != null && p !== plan) {
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

type Action<I extends ParamSpecSet, O extends ParamSpecSet> = {
  f: (outputSet: OutputSet<O>, inputSet: InputSet<I>) => void;
  i: I;
  o: O;
};
type UntypedAction = {
  f: (outputSet: unknown, inputSet: unknown) => void;
  i: ParamSpecSet;
  o: ParamSpecSet;
};

const actionKey = Symbol("action");
type ActionMeta<I extends ParamSpecSet, O extends ParamSpecSet> = {
  [actionKey]: Action<I, O>;
};

export function action<I extends ParamSpecSet, O extends ParamSpecSet>(
  o: O,
  i: I,
  f: (outputSet: OutputSet<O>, inputSet: InputSet<I>) => void,
):
  & ((
    outputSet: { [key in keyof O]: Handle<OutputType<O[key]>> }, // expanded for readability of inferred type
    inputSet: { [key in keyof I]: Handle<InputType<I[key]>> }, // expanded for readability of inferred type
  ) => void)
  & ActionMeta<I, O> {
  const action: Action<I, O> = {
    f,
    i,
    o,
  };

  const g = (
    outputSet: HandleSet<OutputSet<O>>,
    inputSet: HandleSet<InputSet<I>>,
  ) => {
    const plan = getPlan(outputSet, inputSet);

    const id = plan.generateInvocationID();
    const invocation: Invocation = {
      id,
      action: action as UntypedAction,
      inputSet,
      outputSet,
    };
    plan.invocations.set(invocation.id, invocation);
  };
  g[actionKey] = action;

  return g;
}

export function purify<I extends ParamSpecSet, O extends ParamSpecSet>(
  rawAction:
    & ((
      outputSet: HandleSet<OutputSet<O>>,
      inputSet: HandleSet<InputSet<I>>,
    ) => void)
    & ActionMeta<I, O>,
): (
  inputSet: { [key in keyof I]: Handle<InputType<I[key]>> }, // expanded for readability of inferred type
) => { [key in keyof O]: Handle<InputType<O[key]>> } // expanded for readability of inferred type
{
  const action = rawAction[actionKey];
  return (inputSet: HandleSet<InputSet<I>>): HandleSet<InputSet<O>> => {
    const plan = getPlan(inputSet);

    const partialOutputSet: Partial<HandleSet<OutputSet<O>>> = {};
    for (const key in action.o) {
      const handle = plan.generateHandle() as HandleSet<
        OutputSet<O>
      >[typeof key];
      partialOutputSet[key] = handle;
    }
    const outputSet = partialOutputSet as HandleSet<OutputSet<O>>;

    rawAction(outputSet, inputSet);

    return outputSet;
  };
}

type InvocationID = Brand<number, "invocationID">;
type Invocation = {
  id: InvocationID;
  action: UntypedAction;
  inputSet: Record<ObjectKey, UntypedHandle>;
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
  | GlobalInputSlot
  | IntermediateSlot
  | GlobalOutputSlot
  | StubOutputSlot;
type GlobalInputSlot = { type: "global-input"; body: unknown };
type IntermediateSlot = { type: "intermediate"; body: Rc<Box<unknown>> };
type GlobalOutputSlot = { type: "global-output"; body: unknown };
type StubOutputSlot = { type: "stub-output" };

export type ParamSpecSet = {
  [key: ObjectKey]: TypeSpec<unknown, unknown, unknown>;
};

const inputPhantomTypeKey = Symbol("inputType");
const outputPhantomTypeKey = Symbol("outputType");
export type TypeSpec<T extends I & O, I, O> = {
  provider: Provider<T>;
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
  return { provider } as TypeSpec<T, I, O>;
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
    type: "global-input",
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
    type: "global-output",
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

    for (let i = invocations.length - 1; i >= 0; i--) {
      const invocation = invocations[i];
      const action = invocation.action;
      if (action == null) {
        throw new SubFunLogicError("action not found");
      }

      const restoredInputs = restoreSet(plan, invocation.inputSet);
      const cleanupList: (() => void)[] = [];
      const preparedOutputs = prepareOutputSet(
        plan,
        action.o,
        invocation.outputSet,
        cleanupList,
      );
      action.f(preparedOutputs, restoredInputs);
      decRefSet(plan, invocation.inputSet);
      for (const cleanup of cleanupList) {
        cleanup();
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
    for (const outputKey in invocation.outputSet) {
      const output = invocation.outputSet[outputKey];
      if (outputToInvocation.has(output[handleIdKey])) {
        throw new SubFunLogicError("the output have two parent invocations");
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
      throw new SubFunLogicError("the computation graph has a cycle");
    }

    visitedInvocations.set(invocationID, "temporary");

    for (const inputKey in invocation.inputSet) {
      visitHandle(invocation.inputSet[inputKey][handleIdKey]);
    }

    visitedInvocations.set(invocationID, "permanent");

    result.unshift(invocation);
  }

  function visitHandle(handleId: HandleId): void {
    const dataSlot = plan.dataSlots.get(handleId);
    if (dataSlot != null) {
      const type = dataSlot.type;
      switch (type) {
        case "global-input":
          return;
        case "intermediate":
          throw new SubFunLogicError(`unexpected data slot type: ${type}`);
        case "global-output":
          break;
        case "stub-output":
          throw new SubFunLogicError(`unexpected data slot type: ${type}`);
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
    if (dataSlot == null || dataSlot.type !== "global-output") {
      continue;
    }
    visitHandle(handleId);
  }

  return result;
}

function prepareDataSlots(
  plan: Plan,
  invocations: Invocation[],
): void {
  // prepare intermediate inputs
  for (const invocation of invocations) {
    const action = invocation.action;
    if (action == null) {
      throw new SubFunLogicError("action not found");
    }
    for (const inputKey in invocation.inputSet) {
      const input = invocation.inputSet[inputKey];
      const dataSlot = plan.dataSlots.get(input[handleIdKey]);
      if (dataSlot != null) {
        const type = dataSlot.type;
        switch (type) {
          case "global-input":
            break;
          case "intermediate":
            dataSlot.body.incRef();
            break;
          case "global-output":
            break;
          case "stub-output":
            throw new SubFunLogicError(`unexpected data slot type: ${type}`);
          default:
            return unreachable(type);
        }
      } else {
        plan.dataSlots.set(input[handleIdKey], {
          type: "intermediate",
          body: new Rc(new Box(), (x) => {
            if (!x.isSet) {
              return;
            }
            action.i[inputKey].provider.release(x.value);
          }, console.error),
        });
      }
    }
  }

  // prepare intermediate outputs
  for (const invocation of invocations) {
    const action = invocation.action;
    if (action == null) {
      throw new SubFunLogicError("action not found");
    }
    for (const outputKey in invocation.outputSet) {
      const output = invocation.outputSet[outputKey];
      const dataSlot = plan.dataSlots.get(output[handleIdKey]);
      if (dataSlot == null) {
        plan.dataSlots.set(output[handleIdKey], { type: "stub-output" });
      }
    }
  }
}

function restoreSet<T>(plan: Plan, handleSet: HandleSet<T>): T {
  const partialRestored: Partial<T> = {};
  for (const key in handleSet) {
    partialRestored[key] = restore(plan, handleSet[key]);
  }
  return partialRestored as T;
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
    case "global-input": {
      const body = dataSlot.body;
      return body as T;
    }
    case "intermediate":
      if (!dataSlot.body.body.isSet) {
        throw new SubFunLogicError("data slot is not set yet");
      }
      return dataSlot.body.body.value as T;
    case "global-output": {
      const body = dataSlot.body;
      return body as T;
    }
    case "stub-output":
      throw new SubFunLogicError(`unexpected data slot type: ${type}`);
    default:
      return unreachable(type);
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
    case "global-input":
      break;
    case "intermediate":
      dataSlot.body.decRef();
      break;
    case "global-output":
      break;
    case "stub-output":
      throw new SubFunLogicError(`unexpected data slot type: ${type}`);
    default:
      return unreachable(type);
  }
}

function prepareOutputSet<T extends ParamSpecSet>(
  plan: Plan,
  paramSpecSet: T,
  handleSet: HandleSet<OutputSet<T>>,
  cleanupList: (() => void)[],
): OutputSet<T> {
  const partialPrepared: Partial<OutputSet<T>> = {};
  for (const key in handleSet) {
    partialPrepared[key] = prepareOutput(
      plan,
      paramSpecSet,
      handleSet,
      key,
      cleanupList,
    );
  }
  return partialPrepared as OutputSet<T>;
}

function prepareOutput<T extends ParamSpecSet, K extends keyof T>(
  plan: Plan,
  paramSpecSet: T,
  handleSet: HandleSet<OutputSet<T>>,
  key: K,
  cleanupList: (() => void)[],
): OutputType<T[K]> {
  const handle = handleSet[key];
  const dataSlot = plan.dataSlots.get(handle[handleIdKey]);
  if (dataSlot == null) {
    throw new SubFunLogicError("data slot not found");
  }

  const type = dataSlot.type;
  switch (type) {
    case "global-input":
      throw new SubFunLogicError(`unexpected data slot type: ${type}`);
    case "intermediate": {
      if (dataSlot.body.body.isSet) {
        throw new SubFunLogicError("data slot is already set");
      }
      const body = paramSpecSet[key].provider.acquire();
      dataSlot.body.body.value = body;
      return body;
    }
    case "global-output": {
      const body = dataSlot.body;
      return body as T[K];
    }
    case "stub-output": {
      const provider = paramSpecSet[key].provider;
      const body = provider.acquire();
      cleanupList.push(() => provider.release(body));
      return body;
    }
    default:
      return unreachable(type);
  }
}

function assertNoLeak(plan: Plan) {
  for (const dataSlot of plan.dataSlots.values()) {
    const type = dataSlot.type;
    switch (type) {
      case "global-input":
        break;
      case "intermediate":
        if (!dataSlot.body.isFreed) {
          throw new SubFunAssertionError(
            "intermediate data slot is not freed",
          );
        }
        break;
      case "global-output":
        break;
      case "stub-output":
        break;
      default:
        return unreachable(type);
    }
  }
}
