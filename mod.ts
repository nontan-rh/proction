import { SubFunError, SubFunLogicError } from "./error.ts";
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
type Handle<T> = { [handleMarkKey]: never; [phantomDataKey]: T; value: number };
type UntypedHandle = Handle<unknown>;

type UntypedHandleSet<T> = {
  [key in keyof T]: UntypedHandle;
};
type HandleSet<T> = {
  [key in keyof T]: Handle<T[key]>;
};
type GlobalOutputSet<T> = {
  [key in keyof T]: { handle: Handle<T[key]>; body: T[key] };
};

type ActionID = Brand<number, "actionID">;
type Action<I, O> = {
  id: ActionID;
  f: (inputSet: I, outputSet: O) => void;
  d: (plan: Plan, inputSet: HandleSet<I>) => HandleSet<O>;
  i: ParamSpecSet<I>;
  o: ParamSpecSet<O>;
};
type UntypedAction = Action<unknown, unknown>;

function createAction<I, O>(
  ctx: Context,
  f: (inputSet: I, outputSet: O) => void,
  i: ParamSpecSet<I>,
  o: ParamSpecSet<O>,
): Action<I, O> {
  const actionID = ctx.generateActionID();
  const d = (plan: Plan, inputSet: HandleSet<I>): HandleSet<O> => {
    const partialOutputs: Partial<HandleSet<O>> = {};
    for (const key in o) {
      partialOutputs[key] = plan.generateHandle() as HandleSet<O>[typeof key];
    }
    const outputSet = partialOutputs as HandleSet<O>;

    const invocationID = plan.generateInvocationID();
    const invocation: Invocation = {
      id: invocationID,
      actionID,
      inputSet,
      outputSet,
    };
    plan.invocations.set(invocation.id, invocation);

    return outputSet;
  };

  return {
    id: actionID,
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

export class Context {
  generateActionID = idGenerator((value) => value as ActionID);
  funcToActions = new WeakMap<WeakKey, UntypedAction>();
  actions = new Map<ActionID, UntypedAction>();
}

export class Plan {
  ctx: Context;

  state: PlanState;

  generateHandle = idGenerator((value) => ({ value } as UntypedHandle));

  dataSlots = new Map<UntypedHandle, DataSlot>();

  generateInvocationID = idGenerator((value) => value as InvocationID);
  invocations = new Map<InvocationID, Invocation>();

  constructor(ctx: Context) {
    this.ctx = ctx;
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

export type TypeSpec<T> = {
  provider: Provider<T>;
};

export type ParamSpecSet<T> = {
  [key in keyof T]: ParamSpec<T[key]>;
};

export type ParamSpec<T> = {
  type: TypeSpec<T>;
};

export function action<I, O>(
  ctx: Context,
  i: ParamSpecSet<I>,
  o: ParamSpecSet<O>,
  f: (inputSet: I, outputSet: O) => void,
): (plan: Plan, inputSet: HandleSet<I>) => HandleSet<O> {
  const cachedAction = ctx.funcToActions.get(f);
  if (cachedAction != null) {
    return cachedAction.d as (
      plan: Plan,
      inputSet: HandleSet<I>,
    ) => HandleSet<O>;
  }

  const action = createAction(ctx, f, i, o);
  ctx.funcToActions.set(f, action as UntypedAction);
  ctx.actions.set(action.id, action as UntypedAction);
  return action.d;
}

export function input<T>(plan: Plan, value: T): Handle<T> {
  const handle = plan.generateHandle();
  plan.dataSlots.set(handle, { type: "global-input", body: value });
  return handle as Handle<T>;
}

export function run<T>(plan: Plan, globalOutputSet: GlobalOutputSet<T>): void {
  if (plan.state !== "initial") {
    throw new SubFunError(
      `invalid state precondition for run(): ${plan.state}`,
    );
  }

  try {
    plan.state = "planning";

    const invocations = prepareInvocations(plan, globalOutputSet);
    prepareDataSlots(plan, globalOutputSet, invocations);

    plan.state = "running";

    for (let i = invocations.length - 1; i >= 0; i--) {
      const invocation = invocations[i];
      const action = plan.ctx.actions.get(invocation.actionID);
      if (action == null) {
        throw new SubFunLogicError("action not found");
      }

      const reifiedInputs = restoreSet(plan, invocation.inputSet);
      const cleanupList: (() => void)[] = [];
      const reifiedOutputs = prepareOutputSet(
        plan,
        action.o,
        invocation.outputSet,
        cleanupList,
      );
      action.f(reifiedInputs, reifiedOutputs);
      decRefSet(plan, invocation.inputSet);
      for (const cleanup of cleanupList) {
        cleanup();
      }
    }

    plan.state = "done";
  } finally {
    if (plan.state !== "done") {
      plan.state = "error";
    }
  }
}

function prepareInvocations<T>(
  plan: Plan,
  globalOutputSet: GlobalOutputSet<T>,
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
  function visitInvocation(invocation: Invocation) {
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

  function visitHandle(handle: UntypedHandle) {
    if (plan.dataSlots.has(handle)) {
      return;
    }

    const parentInvocation = outputToInvocation.get(handle);
    if (parentInvocation == null) {
      throw new SubFunLogicError(
        `parent invocation not found for handle: ${handle.value}`,
      );
    }

    visitInvocation(parentInvocation);
  }

  for (const globalOutputKey in globalOutputSet) {
    const globalOutput = globalOutputSet[globalOutputKey];
    visitHandle(globalOutput.handle);
  }

  return result;
}

function prepareDataSlots<T>(
  plan: Plan,
  globalOutputSet: GlobalOutputSet<T>,
  invocations: Invocation[],
) {
  // prepare output
  for (const globalOutputKey in globalOutputSet) {
    const globalOutput = globalOutputSet[globalOutputKey];

    const dataSlot = plan.dataSlots.get(globalOutput.handle);
    if (dataSlot != null) {
      const type = dataSlot.type;
      switch (type) {
        case "global-input":
          throw new SubFunError(`input handle is also specified as output`);
        case "intermediate":
          throw new SubFunLogicError(`unexpected data slot type: ${type}`);
        case "global-output":
          throw new SubFunError("output data slot collision");
        case "stub-output":
          throw new SubFunLogicError(`unexpected data slot type: ${type}`);
        default:
          throw new SubFunLogicError(`unknown data slot type: ${type}`);
      }
    }
    plan.dataSlots.set(globalOutput.handle, {
      type: "global-output",
      body: globalOutput.body,
    });
  }

  // prepare intermediate inputs
  for (const invocation of invocations) {
    for (const inputKey in invocation.inputSet) {
      const action = plan.ctx.actions.get(invocation.actionID);
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
            const paramSpec =
              (action.i as Record<ObjectKey, ParamSpec<unknown>>)[inputKey]; // note: very loose type casting
            paramSpec.type.provider.release(x.value);
          }, console.error),
        });
      }
    }
  }

  // prepare intermediate outputs
  for (const invocation of invocations) {
    for (const outputKey in invocation.outputSet) {
      const action = plan.ctx.actions.get(invocation.actionID);
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
      `datum not saved for handle: ${handle.value}`,
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

function decRefSet<T>(plan: Plan, handleSet: HandleSet<T>) {
  for (const key in handleSet) {
    decRef(plan, handleSet[key]);
  }
}

function decRef<T>(plan: Plan, handle: Handle<T>) {
  const dataSlot = plan.dataSlots.get(handle);
  if (dataSlot == null) {
    throw new SubFunLogicError(
      `data slot not saved for handle: ${handle.value}`,
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

function prepareOutputSet<T>(
  plan: Plan,
  paramSpecSet: ParamSpecSet<T>,
  handleSet: HandleSet<T>,
  cleanupList: (() => void)[],
): T {
  const partialPrepared: Partial<T> = {};
  for (const key in handleSet) {
    partialPrepared[key] = prepareOutput(
      plan,
      paramSpecSet,
      handleSet,
      key,
      cleanupList,
    );
  }
  return partialPrepared as T;
}

function prepareOutput<T, K extends keyof T>(
  plan: Plan,
  paramSpecSet: ParamSpecSet<T>,
  handleSet: HandleSet<T>,
  key: K,
  cleanupList: (() => void)[],
): T[K] {
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
      const body = paramSpecSet[key].type.provider.acquire();
      dataSlot.body.body.value = body;
      return body;
    }
    case "global-output": {
      const body = dataSlot.body;
      return body as T[K];
    }
    case "stub-output": {
      const provider = paramSpecSet[key].type.provider;
      const body = provider.acquire();
      cleanupList.push(() => provider.release(body));
      return body;
    }
    default:
      throw new SubFunLogicError(`unknown data slot type: ${type}`);
  }
}
