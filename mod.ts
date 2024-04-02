import {
  SubFunAssertionError,
  SubFunError,
  SubFunLogicError,
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

const handleMarkKey = Symbol("handleMark");
const phantomDataKey = Symbol("phantomData");
type Handle<T> = number & { [handleMarkKey]: never; [phantomDataKey]: () => T };
type UntypedHandle = Handle<unknown>;

type UntypedHandleSet<T> = {
  [key in keyof T]: UntypedHandle;
};
type HandleSet<T> = {
  [key in keyof T]: Handle<T[key]>;
};

type ActionID = Brand<number, "actionID">;
type Action<I extends ParamSpecSet, O extends ParamSpecSet> = {
  id: ActionID;
  name: ObjectKey;
  f: (inputSet: InputSet<I>, outputSet: OutputSet<O>) => void;
  d: <G extends Partial<HandleSet<OutputSet<O>>> | undefined>(
    plan: Plan,
    inputSet: HandleSet<InputSet<I>>,
    globalOutputSet?: G & Record<Exclude<keyof G, keyof O>, never>,
  ) => Omit<HandleSet<InputSet<O>>, keyof G>;
  i: I;
  o: O;
};
type UntypedAction = {
  id: ActionID;
  name: ObjectKey;
  f: (inputSet: unknown, outputSet: unknown) => void;
  d: (plan: Plan, inputSet: unknown, globalOutputSet?: unknown) => unknown;
  i: ParamSpecSet;
  o: ParamSpecSet;
};

function createAction<I extends ParamSpecSet, O extends ParamSpecSet>(
  generateActionID: () => ActionID,
  name: ObjectKey,
  f: (inputSet: InputSet<I>, outputSet: OutputSet<O>) => void,
  i: I,
  o: O,
): Action<I, O> {
  const actionID = generateActionID();
  const d = <G extends Partial<HandleSet<OutputSet<O>>> | undefined>(
    plan: Plan,
    inputSet: HandleSet<InputSet<I>>,
    globalOutputSet?: G, // TODO: Enable this restriction
    /* & Record<Exclude<keyof G, keyof O>, never> */
  ): Omit<HandleSet<InputSet<O>>, keyof G> => {
    const partialOutputs: Partial<HandleSet<OutputSet<O>>> = {};
    const partialReturnOutputs: Partial<HandleSet<OutputSet<O>>> = {};

    for (const key in globalOutputSet) {
      partialOutputs[key] = globalOutputSet[key];
    }

    for (const key in o) {
      if (key in partialOutputs) {
        continue;
      }

      const handle = plan.generateHandle() as HandleSet<
        OutputSet<O>
      >[typeof key];
      partialReturnOutputs[key] = handle;
      partialOutputs[key] = handle;
    }
    const outputSet = partialOutputs as HandleSet<OutputSet<O>>;
    const returnOutputs = partialReturnOutputs as Omit<
      HandleSet<InputSet<O>>,
      keyof G
    >;

    const invocationID = plan.generateInvocationID();
    const invocation: Invocation = {
      id: invocationID,
      actionID,
      inputSet,
      outputSet,
    };
    plan.invocations.set(invocation.id, invocation);

    return returnOutputs;
  };

  return {
    id: actionID,
    name,
    f,
    d,
    i,
    o,
  };
}

type InvocationID = Brand<number, "invocationID">;
type Invocation = {
  id: InvocationID;
  actionID: ActionID;
  inputSet: Record<ObjectKey, UntypedHandle>;
  outputSet: Record<ObjectKey, UntypedHandle>;
};

type ContextBuilderBody = {
  generateActionID: () => ActionID;
  actions: Map<ObjectKey, UntypedAction>;
};

export class ContextBuilder<A> {
  #body: ContextBuilderBody;
  #consumed = false;

  constructor(body: ContextBuilderBody) {
    this.#body = body;
  }

  static empty(): ContextBuilder<Record<ObjectKey, never>> {
    return new ContextBuilder<Record<ObjectKey, never>>(
      {
        generateActionID: idGenerator((value) => value as ActionID),
        actions: new Map<ObjectKey, UntypedAction>(),
      },
    );
  }

  addAction<
    N extends ObjectKey,
    I extends ParamSpecSet,
    O extends ParamSpecSet,
  >(
    name: N,
    i: I,
    o: O,
    f: (inputSet: InputSet<I>, outputSet: OutputSet<O>) => void,
  ): ContextBuilder<
    & A
    & {
      [name in N]: <G extends (Partial<HandleSet<OutputSet<O>>> | undefined)>(
        inputSet: HandleSet<InputSet<I>>,
        globalOutputSet?: G & Record<Exclude<keyof G, keyof O>, never>,
      ) => Omit<HandleSet<InputSet<O>>, keyof G>;
    }
  > {
    if (this.#consumed) {
      throw new SubFunError("ContextBuilder is already consumed");
    }
    if (this.#body.actions.has(name)) {
      throw new SubFunError(
        `action with name (${
          String(name)
        }) is already registered to ContextBuilder`,
      );
    }
    this.#consumed = true;

    const action = createAction(
      this.#body.generateActionID,
      name,
      f,
      i,
      o,
    ) as UntypedAction;
    this.#body.actions.set(name, action);

    return new ContextBuilder<
      & A
      & {
        [name in N]: <G extends Partial<HandleSet<OutputSet<O>>> | undefined>(
          inputSet: HandleSet<InputSet<I>>,
          globalOutputSet?: G & Record<Exclude<keyof G, keyof O>, never>,
        ) => Omit<HandleSet<InputSet<O>>, keyof G>;
      }
    >(this.#body);
  }

  build(): Context<A> {
    if (this.#consumed) {
      throw new SubFunError("ContextBuilder is already consumed");
    }
    this.#consumed = true;

    const actions = new Map<ActionID, UntypedAction>();
    for (const action of this.#body.actions.values()) {
      actions.set(action.id, action);
    }

    return new Context(actions);
  }
}

export class Context<A> {
  #actions = new Map<ActionID, UntypedAction>();

  constructor(actions: Map<ActionID, UntypedAction>) {
    this.#actions = actions;
  }

  run(planFn: (p: PlanFnParams<A>) => void, options?: RunOptions) {
    const plan = new Plan(this.#actions);
    const boundActions: Record<ObjectKey, unknown> = {};
    for (const action of this.#actions.values()) {
      boundActions[action.name] = (
        inputSet: HandleSet<unknown>,
        globalOutputSet?: HandleSet<unknown>,
      ) => action.d(plan, inputSet, globalOutputSet);
    }
    const runParams: PlanFnParams<A> = {
      input: (value) => input(plan, value),
      output: (value) => output(plan, value),
      actions: boundActions as A,
    };
    planFn(runParams);
    run(plan, options);
  }
}

type PlanFnParams<A> = {
  input<T>(value: T): Handle<T>;
  output<T>(value: T): Handle<T>;
  actions: A;
};

class Plan {
  actions: Map<ActionID, UntypedAction>;

  state: PlanState;

  generateHandle = idGenerator((value) => (value as UntypedHandle));
  dataSlots = new Map<UntypedHandle, DataSlot>();

  generateInvocationID = idGenerator((value) => value as InvocationID);
  invocations = new Map<InvocationID, Invocation>();

  constructor(actions: Map<ActionID, UntypedAction>) {
    this.actions = actions;
    this.state = "initial";
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

function input<T>(plan: Plan, value: T): Handle<T> {
  const handle = plan.generateHandle();
  plan.dataSlots.set(handle, { type: "global-input", body: value });
  return handle as Handle<T>;
}

function output<T>(plan: Plan, value: T): Handle<T> {
  const handle = plan.generateHandle();
  plan.dataSlots.set(handle, { type: "global-output", body: value });
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
      const action = plan.actions.get(invocation.actionID);
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
      action.f(restoredInputs, preparedOutputs);
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

  const outputToInvocation = new Map<UntypedHandle, Invocation>();
  for (const invocation of plan.invocations.values()) {
    for (const outputKey in invocation.outputSet) {
      const output = invocation.outputSet[outputKey];
      if (outputToInvocation.has(output)) {
        throw new SubFunLogicError("the output have two parent invocations");
      }
      outputToInvocation.set(output, invocation);
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
      visitHandle(invocation.inputSet[inputKey]);
    }

    visitedInvocations.set(invocationID, "permanent");

    result.unshift(invocation);
  }

  function visitHandle(handle: UntypedHandle): void {
    const dataSlot = plan.dataSlots.get(handle);
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
          throw new SubFunLogicError(`unknown data slot type: ${type}`);
      }
    }

    const parentInvocation = outputToInvocation.get(handle);
    if (parentInvocation == null) {
      throw new SubFunLogicError(
        `parent invocation not found for handle: ${handle}`,
      );
    }

    visitInvocation(parentInvocation);
  }

  for (const handle of plan.dataSlots.keys()) {
    const dataSlot = plan.dataSlots.get(handle);
    if (dataSlot == null || dataSlot.type !== "global-output") {
      continue;
    }
    visitHandle(handle);
  }

  return result;
}

function prepareDataSlots(
  plan: Plan,
  invocations: Invocation[],
): void {
  // prepare intermediate inputs
  for (const invocation of invocations) {
    for (const inputKey in invocation.inputSet) {
      const action = plan.actions.get(invocation.actionID);
      if (action == null) {
        throw new SubFunLogicError("action not found");
      }

      const input = invocation.inputSet[inputKey];
      const dataSlot = plan.dataSlots.get(input);
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
            throw new SubFunLogicError(`unknown data slot type: ${type}`);
        }
      } else {
        plan.dataSlots.set(input, {
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
    for (const outputKey in invocation.outputSet) {
      const action = plan.actions.get(invocation.actionID);
      if (action == null) {
        throw new SubFunLogicError("action not found");
      }

      const output = invocation.outputSet[outputKey];
      const dataSlot = plan.dataSlots.get(output);
      if (dataSlot == null) {
        plan.dataSlots.set(output, { type: "stub-output" });
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
  const dataSlot = plan.dataSlots.get(handle);
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
      throw new SubFunLogicError(`unknown data slot type: ${type}`);
  }
}

function decRefSet<T>(plan: Plan, handleSet: HandleSet<T>): void {
  for (const key in handleSet) {
    decRef(plan, handleSet[key]);
  }
}

function decRef<T>(plan: Plan, handle: Handle<T>): void {
  const dataSlot = plan.dataSlots.get(handle);
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
      throw new SubFunLogicError(`unknown data slot type: ${type}`);
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
  const dataSlot = plan.dataSlots.get(handle);
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
      throw new SubFunLogicError(`unknown data slot type: ${type}`);
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
        throw new SubFunLogicError(`unknown data slot type: ${type}`);
    }
  }
}
