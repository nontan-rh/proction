# Introduction to Proction

## About

Proction is a utility library for versatile dataflow-based tasks that provides:

- Fine-grained resource management
- Parallel processing with fine-grained control
- Good integration with externally managed resources
- Intuitive interface similar to regular programming

Each feature is provided in a modular, customizable way, and you can combine them as you like.

## Problem: Calculations on Arrays

Let's compute the inner products of many pairs of 3D vectors. Storing the vectors in a structure-of-arrays (SoA) layout, their components are given as six arrays of numbers (`a` through `f`). The array of inner products is then calculated as `(a * b) + (c * d) + (e * f)`.

We'll define `add` and `mul` as independent functions for maintainability.

One implementation might look like:

```ts
// Answer X

function add(lht: number[], rht: number[]): number[] {
  const output: number[] = new Array(lht.length);
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
  return output;
}

function mul(lht: number[], rht: number[]): number[] {
  const output: number[] = new Array(lht.length);
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] * rht[i];
  }
  return output;
}

function innerProduct(a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]): number[] {
  const m1 = mul(a, b);   // alloc
  const m2 = mul(c, d);   // alloc
  const m3 = mul(e, f);   // alloc
  const s1 = add(m1, m2); // alloc
  const s2 = add(s1, m3); // alloc
  return s2;
}
```

We can also define `add` and `mul` to take the output buffer as an argument. This leads to a different implementation:

```ts
// Answer Y

function add(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
}

function mul(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] * rht[i];
  }
}

function innerProduct(output: number[], a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]) {
  const m1 = new Array(output.length); // alloc
  const m2 = new Array(output.length); // alloc
  const s = new Array(output.length);  // alloc
  mul(m1, a, b);
  mul(m2, c, d);
  add(s, m1, m2);
  mul(m1, e, f); // contents of `m1` are no longer required, reuse array
  add(output, s, m1);
}
```

Let's call the first implementation "Answer X" and the second "Answer Y", and compare them.

### Allocation

First, look at allocations. In Answer X, allocations happen inside `add` and `mul`, and buffers are returned from those functions. The number of allocations is 5, which equals the number of invocations of `add` and `mul`.

In contrast, in Answer Y, allocations are done in the caller `innerProduct` and the buffers are passed to the functions. The number of allocations is 3, which is fewer than in Answer X.

We can introduce an object pool into Answer Y to reduce allocations further. Let's call this "Answer Y'". In Answer Y', no allocations are performed after the second invocation of `innerProduct`.

```ts
// Answer Y'

interface ArrayPool {
  acquire(length: number): number[];
  release(obj: number[]): void;
}
const pool: ArrayPool = { /* some implementation */ };

function innerProduct(output: number[], a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]) {
  const m1 = pool.acquire(output.length);
  const m2 = pool.acquire(output.length);
  mul(m1, a, b);
  mul(m2, c, d);
  const s = pool.acquire(output.length);
  add(s, m1, m2);
  pool.release(m1); // `m1` is no longer required
  pool.release(m2); // `m2` is no longer required
  const m3 = pool.acquire(output.length);
  mul(m3, e, f);
  add(output, s, m3);
  pool.release(m3); // `m3` is no longer required
  pool.release(s);  // `s` is no longer required
}
```

### Composability

How about the composability of `add` and `mul`? In Answer X, the implementation of `innerProduct` is quite straightforward. However, in Answer Y, we manage buffer lifetimes manually to reduce allocations, which harms readability.

Answer Y' has the same problem as Answer Y. We must also manage buffer lifetimes manually in this case.

### Overall Comparison

Both Answer X and Answer Y (and Y') have pros and cons. Answer X seems better for maintainability. However, Answer Y (and Y') wins in terms of efficiency. This is a trade-off.

## Solution

Proction was created to tackle this dilemma. It takes the strengths of both Answer X and Answer Y. It also addresses several related problems. Below is an example of how `innerProduct` can be written with Proction. It's a bit verbose for clarity.

```ts
interface ArrayPool {
  acquire(length: number): number[];
  release(obj: number[]): void;
}
const pool: ArrayPool = { /* some implementation */ };
const provide = provider((length) => pool.acquire(length), (obj) => pool.release(obj));

const addProc = proc()((output: number[], lht: number[], rht: number[]) => {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
});
const mulProc = proc()((output: number[], lht: number[], rht: number[]) => {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] * rht[i];
  }
});

const addFunc = toFunc(addProc, (lht, _rht) => provide(lht.length));
const mulFunc = toFunc(mulProc, (lht, _rht) => provide(lht.length));

const ctx = new Context();
async function innerProduct(output: number[], a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]) {
  await run(ctx, ({ $s, $d }) => {
    const m1 = mulFunc($s(a), $s(b));
    const m2 = mulFunc($s(c), $s(d));
    const m3 = mulFunc($s(e), $s(f));
    const s1 = addFunc(m1, m2);
    addProc($d(output), s1, m3);
  });
  // Now `output` stores the result!
}
```

In this example, the `innerProduct` function looks simple as in Answer X, while using an object pool as in Answer Y'.

The following sections describe the core concepts and how they work.

## Core Concepts

There are three core concepts: procedures, functions, and conversions between them.

### Procedures

In this library, a procedure is a routine that takes both input data and an output buffer as arguments. It computes a result and writes it to the given buffer. Consider a procedure that adds corresponding elements of two arrays.

```ts
function addProcedure(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
}
```

`addProcedure` takes `lht` and `rht` as input arguments, and `output` as an output buffer. It doesn't allocate resources, and the caller can pass any `number[]` buffer regardless of how it was allocated. On the other hand, the caller always has to allocate and manage these buffers.

This style is especially useful when the output buffer is externally managed or involves I/O features such as a display framebuffer. In low-level languages like C, this also applies to memory-mapped (`mmap`) regions.

This is the simplest form of routine and the best in terms of cohesion. Proction adopts procedure-style routines as primitives.

### Functions

A function is a routine that takes only input data and returns the output data. The buffers for the output data are allocated internally. An adding function could be written like this:

```ts
function addFunction(lht: number[], rht: number[]): number[] {
  const output: number[] = new Array(lht.length);
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
  return output;
}
```

`addFunction` only takes `lht` and `rht` as input arguments. The result is returned by the function and the array is allocated internally. The caller is free from explicitly allocating an output buffer; however, it cannot control how buffers are allocated. For example, object pools or custom allocators for `ArrayBuffer`s can't be used in this style.

Function-style routines are very easy to use. Proction lets you compose subroutines in this form.

### Indirection and Style Conversion

Proction uses indirect routines and provides tools for creating indirect procedures and converting them into indirect functions.
Indirect routines take and return indirect handles instead of the actual objects. The signature looks like this:

```ts
function indirectAddProcedure(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
```

To create indirect procedures easily, Proction provides the `proc` utility. You can define `indirectAddProcedure` like this:

```ts
const indirectAddProcedure = proc()(
  function addProcedure(output: number[], lht: number[], rht: number[]) {
    for (let i = 0; i < output.length; i++) {
      output[i] = lht[i] + rht[i];
    }
  }
);
```

The `toFunc` utility converts indirect procedures into indirect functions.

```ts
const provide = provider((length) => new Array(length), () => {});

// function indirectAddFunction(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>
const indirectAddFunction = toFunc(indirectAddProcedure, (lht, _rht) => provide(lht.length));
```

This introduces the "provider" concept required for the conversion. In this example, the provider allocates a new array for the result of each function call. We'll describe providers in more detail later.

(As you may notice, "Proction" is a portmanteau of "procedure" and "function.")

## Performing the Calculation

We can call and compose indirect routines to perform more complex computations in a `run` block. `run` and indirect procedures build a computation graph, which the Proction runtime executes.

```ts
// Assume these indirect routines are already defined:
// function addProc(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
// function addFunc(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>;
// function mulProc(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
// function mulFunc(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>;

const ctx = new Context();
async function innerProduct(output: number[], a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]) {
  await run(ctx, ({ $s, $d }) => {
    const m1 = mulFunc($s(a), $s(b));
    const m2 = mulFunc($s(c), $s(d));
    const m3 = mulFunc($s(e), $s(f));
    const s1 = addFunc(m1, m2);
    addProc($d(output), s1, m3);
  });
  // Now `output` stores the result!
}
```

Handles for input data can be created with `$s`, and those for output data with `$d`. Again, you can combine indirect procedures and functions in a very intuitive way.

## Providers

Providers attach to indirect functions and enable you to manage how intermediate buffers are allocated and freed, independently of the implementation details of the underlying indirect procedures.

```ts
interface ArrayPool {
  acquire(length: number): number[];
  release(obj: number[]): void;
}
const pool: ArrayPool = { /* some implementation */ };

const provide = provider((length) => pool.acquire(length), (obj) => pool.release(obj));
const indirectAddFunctionWithPool = toFunc(indirectAddProcedure, (lht, _rht) => provide(lht.length));
```

You can completely reuse `indirectAddProcedure` and customize the resource management. The objects are returned to the provider as soon as they are no longer required. Thanks to object pools, the number of array allocations is minimized and buffers are reused when possible.

## Parallelism

`proc` can take `async` JavaScript functions as their implementation to achieve parallel computing. You can use Web Workers, for example, to take advantage of multi-core CPUs. Here is a very simplified example.

```ts
const worker: Worker = /* some worker implementation */;
const indirectAddProcedure = proc()(
  async function addProcedure(output: number[], lht: number[], rht: number[]) {
    const { promise, resolve } = Promise.withResolvers();
    worker.onmessage = resolve;
    worker.postMessage([output, lht, rht]);
    await promise;
  }
);
```

Function-style indirect routines and Proction's task scheduler prevent data races and help automatically maximize CPU utilization.

## Middlewares

Middlewares in Proction are similar to those in other JavaScript libraries. They wrap indirect routines and must invoke the next action in the chain. Middlewares can be installed when you create indirect procedures with `proc`.

```ts
const add = proc({
  middlewares: [async (next) => {
    console.log("before addProcedure");
    await next();
    console.log("after addProcedure");
  }],
})(
  function addProcedure(output: number[], lht: number[], rht: number[]) {
    for (let i = 0; i < output.length; i++) {
      output[i] = lht[i] + rht[i];
    }
  },
);
```

Because they are defined as `async` JavaScript functions, middlewares are also useful for scheduling routine execution.

```ts
interface Semaphore {
  acquire(): Promise<void>;
  release(): Promise<void>;
}
const semaphore: Semaphore = /* some semaphore implementation */;

const serializationMiddleware = async (next) => {
  await semaphore.acquire();
  try {
    await next();
  } finally {
    await semaphore.release();
  }
};
```

Proction starts all invocations of indirect routines as soon as their dependent tasks complete and input data is ready. Therefore, you can effectively control the degree of parallelism with semaphores, and they can be easily integrated with the middleware feature.

You can also define more sophisticated middlewares that control the order of execution by introducing priorities to the semaphore. Middlewares can be very powerful in Proction.

## Multiple Outputs

To define a routine with multiple outputs, use `procN` / `toFuncN`. The output argument and return value are tuple-like arrays. You can use multiple-output routines in the same way as the single-output ones.

```ts
const sincosProc = procN()(([sinOutput, cosOutput]: [number[], number[]], x: number[]) => {
  for (let i = 0; i < sinOutput.length; i++) {
    sinOutput[i] = Math.sin(x[i]);
    cosOutput[i] = Math.cos(x[i]);
  }
});
// function sincosProc(outputs: [Handle<number[]>, Handle<number[]>], x: Handle<number[]>);

const sincosFunc = toFuncN(sincosProc, [(x) => provide(x.length), (x) => provide(x.length)]);
// function sincosFunc(x: Handle<number[]>): [Handle<number[]>, Handle<number[]>];

const ctx = new Context();
async function innerProduct(sinOutput: number[], cosOutput: number[], x: number[]) {
  await run(ctx, ({ $s, $d }) => {
    sincosProc([$d(sinOutput), $d(cosOutput)], $s(x));
  });
}
```

## Conclusion

Proction reconciles maintainable composition with tight control over resources and execution.

Get started by writing a small procedure with `proc(...)`, derive a function using `toFunc(...)` and a provider, then compose your complex pipeline inside `run(...)`. This keeps function-style readability while retaining procedure-style performance and flexibility.
