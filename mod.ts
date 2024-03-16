const brandKey = Symbol("brand");
type Brand<K, T> = K & { [brandKey]: T };

type ObjectKey = string | number | symbol;

class DeferCalcError extends Error {}

class DeferCalcLogicError extends DeferCalcError {}

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

type ActionID = Brand<number, "actionID">;
type Action<I, O> = {
  id: ActionID;
  f: (inputSet: I) => O;
  d: (plan: Plan, inputSet: HandleSet<I>) => HandleSet<O>;
};
type UntypedAction = {
  id: ActionID;
  f: (inputSet: unknown) => unknown;
  d: (plan: Plan, inputSet: unknown) => unknown;
};

function createAction<I, O>(
  ctx: Context,
  f: (inputSet: I) => O,
  _i: ParamSpecSet<I>,
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

  data = new Map<UntypedHandle, Datum>();

  generateInvocationID = idGenerator((value) => value as InvocationID);
  invocations = new Map<InvocationID, Invocation>();

  constructor(ctx: Context) {
    this.ctx = ctx;
    this.state = "initial";
  }
}

type PlanState = "initial" | "planning" | "running" | "done" | "error";

type Datum = {
  value: unknown;
};

export type ParamSpecSet<T> = {
  [key in keyof T]: ParamSpec<T>;
};

export type ParamSpec<T> = {
  type: "immediate";
};

export function deferred<I, O>(
  ctx: Context,
  f: (inputSet: I) => O,
  i: ParamSpecSet<I>,
  o: ParamSpecSet<O>,
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
  plan.data.set(handle, { value });
  return handle as Handle<T>;
}

export function run<T>(plan: Plan, globalOutputs: HandleSet<T>): T {
  if (plan.state !== "initial") {
    throw new DeferCalcError(
      `invalid state precondition for run(): ${plan.state}`,
    );
  }

  try {
    plan.state = "planning";

    const invocations = toposortInvocations(plan, globalOutputs);

    plan.state = "running";

    for (let i = invocations.length - 1; i >= 0; i--) {
      const invocation = invocations[i];
      const action = plan.ctx.actions.get(invocation.actionID);
      if (action == null) {
        throw new DeferCalcLogicError("action not found");
      }

      const reifiedInputs = restoreSet(plan, invocation.inputSet);
      const reifiedOutputs = action.f(reifiedInputs);
      saveSet(plan, invocation.outputSet, reifiedOutputs);
    }

    const result = restoreSet(plan, globalOutputs);

    plan.state = "done";

    return result;
  } finally {
    if (plan.state !== "done") {
      plan.state = "error";
    }
  }
}

function toposortInvocations<T>(
  plan: Plan,
  globalOutputs: HandleSet<T>,
): Invocation[] {
  type ToposortState = "temporary" | "permanent";

  const result: Invocation[] = [];

  const outputToInvocation = new Map<UntypedHandle, Invocation>();
  for (const invocation of plan.invocations.values()) {
    for (const outputKey in invocation.outputSet) {
      const output = invocation.outputSet[outputKey];
      if (outputToInvocation.has(output)) {
        throw new DeferCalcLogicError("the output have two parent invocations");
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
      throw new DeferCalcLogicError("the computation graph has a cycle");
    }

    visitedInvocations.set(invocationID, "temporary");

    for (const inputKey in invocation.inputSet) {
      visitHandle(invocation.inputSet[inputKey]);
    }

    visitedInvocations.set(invocationID, "permanent");

    result.unshift(invocation);
  }

  function visitHandle(handle: UntypedHandle) {
    if (plan.data.has(handle)) {
      return;
    }

    const parentInvocation = outputToInvocation.get(handle);
    if (parentInvocation == null) {
      throw new DeferCalcLogicError(
        `parent invocation not found for handle: ${handle.value}`,
      );
    }

    visitInvocation(parentInvocation);
  }

  for (const globalOutputKey in globalOutputs) {
    visitHandle(globalOutputs[globalOutputKey]);
  }

  return result;
}

function restoreSet<T>(plan: Plan, handleSet: HandleSet<T>): T {
  const partialReifiedSet: Partial<T> = {};
  for (const key in handleSet) {
    partialReifiedSet[key] = restore(plan, handleSet[key]);
  }
  return partialReifiedSet as T;
}

function restore<T>(plan: Plan, handle: Handle<T>): T {
  const datum = plan.data.get(handle);
  if (datum == null) {
    throw new DeferCalcLogicError(
      `datum not saved for handle: ${handle.value}`,
    );
  }

  return datum.value as T;
}

function saveSet<T>(plan: Plan, handleSet: HandleSet<T>, valueSet: T) {
  for (const key in handleSet) {
    save(plan, handleSet[key], valueSet[key]);
  }
}

function save<T>(plan: Plan, handle: Handle<T>, value: T) {
  if (plan.data.has(handle)) {
    throw new DeferCalcLogicError(
      `datum is already saved for handle: ${handle.value}`,
    );
  }

  plan.data.set(handle, { value });
}
