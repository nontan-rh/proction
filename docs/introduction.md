# Introduction to Proction

## About

Proction is a utility library for versatile dataflow-based tasks that provides:

- Fine-grained resource management
- Intuitive interface similar to regular programming
- Good integration with externally managed resources
- Type-agnostic data handling beyond numeric vectors/tensors
- Highly customizable scheduling and parallelism

Each feature is provided in a modular, customizable way, and you can combine them as you like.

## Problem: Calculations on Arrays

Consider element-wise `add` and `mul` routines on arrays:

```ts
const add = (a: number[], b: number[]): number[] => {
  const out: number[] = new Array(a.length);
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] + b[i];
  }
  return out;
};

const mul = (a: number[], b: number[]): number[] => {
  const out: number[] = new Array(a.length);
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] * b[i];
  }
  return out;
};
```

Now suppose we want to compute an element-wise inner product of two 3-component vector arrays:

$$(\mathbf{a} \cdot \mathbf{b}) = a_x \times b_x + a_y \times b_y + a_z \times b_z$$

In total, we need to perform multiplication 3 times and addition 2 times.

### Solution A (Function Style)

```ts
const innerProduct = (
  ax: number[], ay: number[], az: number[],
  bx: number[], by: number[], bz: number[],
): number[] => {
  const m1 = mul(ax, bx);
  const m2 = mul(ay, by);
  const m3 = mul(az, bz);
  const s1 = add(m1, m2);
  const s2 = add(s1, m3);
  return s2;
};
```

Simple and readable, but each call creates a new array: **5 allocations** per call to `innerProduct`.

### Solution B (Procedure Style)

We can reduce allocations by passing output buffers as arguments instead of returning new arrays:

```ts
const add = (out: number[], a: number[], b: number[]): void => {
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] + b[i];
  }
};

const mul = (out: number[], a: number[], b: number[]): void => {
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] * b[i];
  }
};

const innerProduct = (
  ax: number[], ay: number[], az: number[],
  bx: number[], by: number[], bz: number[],
): number[] => {
  const m1 = new Array(ax.length);
  const m2 = new Array(ax.length);
  const s1 = new Array(ax.length);
  const s2 = new Array(ax.length);
  mul(m1, ax, bx);
  mul(m2, ay, by);
  add(s1, m1, m2);
  mul(m1, az, bz); // contents of `m1` are no longer required, reuse array
  add(s2, s1, m1);
  return s2;
};
```

Manual buffer management brings allocations down to **4** and allows buffer reuse, but managing buffers manually is tedious.

### Reducing Allocations Further

Since we're already managing buffers manually, we can use an object pool to reuse arrays across calls.

```ts
interface Pool {
  acquire(len: number): number[];
  release(o: number[]): void;
}
declare const pool: Pool;

const innerProduct = (
  ax: number[], ay: number[], az: number[],
  bx: number[], by: number[], bz: number[],
): number[] => {
  const out = new Array(ax.length);
  const m1 = pool.acquire(ax.length);
  const m2 = pool.acquire(ax.length);
  mul(m1, ax, bx);
  mul(m2, ay, by);
  const s1 = pool.acquire(ax.length);
  add(s1, m1, m2);
  pool.release(m1); // `m1` is no longer required
  pool.release(m2); // `m2` is no longer required
  const m3 = pool.acquire(ax.length);
  mul(m3, az, bz);
  add(out, s1, m3);
  pool.release(m3); // `m3` is no longer required
  pool.release(s1); // `s1` is no longer required
  return out;
};
```

With an object pool, the number of steady-state allocations for intermediate buffers drops to **0**. However, the code has become much more complex: the caller must track buffer lifetimes and manage acquire/release calls manually.

### Comparison

- **Solution A** : Easy to compose and read, but allocation-heavy.
- **Solution B** : Gives control over allocation and reuse, but burdens the caller with buffer lifetime management.

Is this strictly a trade-off?

## Proction's Solution

Proction was created to tackle this dilemma. It takes the strengths of both Function Style and Procedure Style. It also addresses several related problems. Below is an example of how `innerProduct` can be written with Proction. It's a bit verbose for clarity.

```ts
import { Context, run, proc, toFunc, provider } from "jsr:@nontan-rh/proction";

interface Pool {
  acquire(len: number): number[];
  release(o: number[]): void;
}
declare const pool: Pool;
const provide = provider((len: number) => pool.acquire(len), (o) => pool.release(o));

const addProc = proc((out: number[], a: number[], b: number[]) => {
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] + b[i];
  }
});
const mulProc = proc((out: number[], a: number[], b: number[]) => {
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] * b[i];
  }
});

const addFunc = toFunc(addProc, (a, _b) => provide(a.length));
const mulFunc = toFunc(mulProc, (a, _b) => provide(a.length));

const ctx = new Context();
async function innerProduct(
  out: number[],
  ax: number[], ay: number[], az: number[],
  bx: number[], by: number[], bz: number[],
) {
  await run(ctx, ({ $s, $d }) => {
    const m1 = mulFunc($s(ax), $s(bx));
    const m2 = mulFunc($s(ay), $s(by));
    const m3 = mulFunc($s(az), $s(bz));
    const s1 = addFunc(m1, m2);
    addProc($d(out), s1, m3);
  });
  // Now `out` stores the result!
}
```

In this example, `innerProduct` remains simple like Solution A, while using a pool like Solution B and effectively keeping steady-state allocations at 0.

The following sections describe the core concepts and how they work.

## Core Concepts

There are three core concepts: procedures, functions, and conversions between them.

### Procedures

In this library, a procedure is a routine that takes both input data and an output buffer as arguments. It computes a result and writes it to the given buffer. Consider a procedure that adds corresponding elements of two arrays.

```ts
function add(out: number[], a: number[], b: number[]) {
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] + b[i];
  }
}
```

`add` takes `a` and `b` as input arguments, and `out` as an output buffer. It doesn't allocate resources, and the caller can pass any `number[]` buffer regardless of how it was allocated. Although this example uses arrays, arguments can be of any object type.

This style is especially useful when the output buffer is externally managed or involves I/O features such as a display framebuffer. In low-level languages like C, this also applies to memory-mapped (`mmap`) regions.

This is the simplest form of routine and the best in terms of cohesion. Proction adopts procedure-style routines as primitives.

### Functions

A function is a routine that takes only input data and returns the output data. The buffers for the output data are allocated internally. An adding function could be written like this:

```ts
function add(a: number[], b: number[]): number[] {
  const out: number[] = new Array(a.length);
  for (let i = 0; i < out.length; i++) {
    out[i] = a[i] + b[i];
  }
  return out;
}
```

`add` only takes `a` and `b` as input arguments. The result is returned by the function and the array is allocated internally. The caller is free from explicitly allocating an output buffer; however, it cannot control how buffers are allocated. For example, object pools or custom allocators for `ArrayBuffer`s can't be used in this style.

Function-style routines are very easy to use. Proction lets you compose subroutines in this form.

### Indirection and Style Conversion

Proction uses indirect routines and provides tools for creating indirect procedures and converting them into indirect functions.
Indirect routines take and return indirect handles instead of the actual objects. The signature looks like this:

```ts
function addProc(out: Handle<number[]>, a: Handle<number[]>, b: Handle<number[]>): void;
```

A `Handle<T>` is an internal reference to a value of type `T` that Proction tracks for dependency and lifetime management; you obtain handles from actual objects via special helper functions later.

To create indirect procedures easily, Proction provides the `proc` utility. You can define `addProc` like this:

```ts
const addProc = proc(
  function add(out: number[], a: number[], b: number[]) {
    for (let i = 0; i < out.length; i++) {
      out[i] = a[i] + b[i];
    }
  }
);
```

The `toFunc` utility converts indirect procedures into indirect functions.

```ts
const provide = provider((length) => new Array(length), () => {});

// function addFunc(a: Handle<number[]>, b: Handle<number[]>): Handle<number[]>
const addFunc = toFunc(addProc, (a, _b) => provide(a.length));
```

This introduces the "provider" concept required for the conversion. In this example, the provider allocates a new array for the result of each function call. We'll describe providers in more detail later.

(As you may notice, "Proction" is a portmanteau of "procedure" and "function.")

## Performing the Calculation

We can call and compose indirect routines to perform more complex computations in a `run` block. `run` and indirect procedures build a computation graph, which the Proction runtime executes.

```ts
// Assume these indirect routines are already defined:
declare function addProc(out: Handle<number[]>, a: Handle<number[]>, b: Handle<number[]>): void;
declare function addFunc(a: Handle<number[]>, b: Handle<number[]>): Handle<number[]>;
declare function mulProc(out: Handle<number[]>, a: Handle<number[]>, b: Handle<number[]>): void;
declare function mulFunc(a: Handle<number[]>, b: Handle<number[]>): Handle<number[]>;

const ctx = new Context();
async function innerProduct(
  out: number[],
  ax: number[], ay: number[], az: number[],
  bx: number[], by: number[], bz: number[],
) {
  await run(ctx, ({ $s, $d }) => {
    const m1 = mulFunc($s(ax), $s(bx));
    const m2 = mulFunc($s(ay), $s(by));
    const m3 = mulFunc($s(az), $s(bz));
    const s1 = addFunc(m1, m2);
    addProc($d(out), s1, m3);
  });
  // Now `out` stores the result!
}
```

Handles for input data can be created with `$s`, and those for output data with `$d`. Again, you can combine indirect procedures and functions in a very intuitive way.

## Providers

Providers attach to indirect functions and enable you to manage how intermediate buffers are allocated and freed, independently of the implementation details of the underlying indirect procedures.

```ts
interface Pool {
  acquire(len: number): number[];
  release(o: number[]): void;
}
declare const pool: Pool;

const provide = provider((len: number) => pool.acquire(len), (obj) => pool.release(obj));
const addFunc = toFunc(addProc, (a, _b) => provide(a.length));
```

You can completely reuse `addProc` and customize the resource management. The objects are returned to the provider as soon as they are no longer required. Concretely, Proction tracks the data-dependency graph and releases provider-managed buffers after all downstream consumers complete. Thanks to object pools, the number of array allocations is minimized and buffers are reused when possible, reducing GC pressure in steady state.

## In-Place Optimization

We can reduce resource usage further by reusing an input buffer as the output buffer. As an example, for an operation like `c = a + b`, we may be able to modify it to `a = a + b` and reduce the buffers required at the same time. This pattern often appears as per-pixel blending into a framebuffer in graphics APIs.

However, mutating routines are often harder to compose. Callers may need to keep track of which values are safe to overwrite, insert explicit copies when necessary, or choose between out-of-place and in-place variants.

Proction supports this pattern with `procI` (and its multi-output variants `procNI1` / `procNIAll`). `procI` lets you provide two implementations: a standard out-of-place implementation and an in-place implementation.

```ts
const double = procI(
  // Out-of-place implementation: writes to `out`, reads `input0`
  function doubleOutOfPlace(out: number[], input: number[]) {
    for (let i = 0; i < out.length; i++) {
      out[i] = input[i] * 2;
    }
  },
  // In-place implementation: modifies `inout` directly
  function doubleInPlace(inout: number[]) {
    for (let i = 0; i < inout.length; i++) {
      inout[i] *= 2;
    }
  }
);

const pureDouble = toFunc(double, (input) => provide(input.length));
```

When you use `pureDouble` in a `run` block, Proction automatically decides which implementation to use. It selects the in-place version when it is safe: when the first input is managed by Proction and has exactly one consumer. Otherwise, it falls back to the out-of-place version.

## Parallelism

`proc` can take `async` JavaScript functions as their implementation to enable parallel computing. You can use Web Workers, for example, to take advantage of multi-core CPUs. Here is a very simplified example.

```ts
declare const worker: Worker;
const addProc = proc(
  async function add(out: number[], a: number[], b: number[]) {
    const { promise, resolve } = Promise.withResolvers();
    worker.onmessage = resolve;
    worker.postMessage([out, a, b]);
    await promise;
  }
);
```

Function-style indirect routines and Proction's task scheduler prevent data races and help automatically maximize CPU utilization.

## Middlewares

Middlewares in Proction are similar to those in other JavaScript libraries. They wrap indirect routines and must invoke the next action in the chain. Middlewares can be installed when you create indirect procedures with `proc`.

```ts
const addProc = proc(
  function add(out: number[], a: number[], b: number[]) {
    for (let i = 0; i < out.length; i++) {
      out[i] = a[i] + b[i];
    }
  },
  {
    middlewares: [async (next) => {
      console.log("before add");
      await next();
      console.log("after add");
    }],
  },
);
```

The `async` nature of middleware also makes it a powerful tool for scheduling routine execution.

```ts
interface Semaphore {
  acquire(): Promise<void>;
  release(): Promise<void>;
}
declare const semaphore: Semaphore;

const limitConcurrency = async (next: () => Promise<void>) => {
  await semaphore.acquire();
  try {
    await next();
  } finally {
    await semaphore.release();
  }
};
```

Proction starts all invocations of indirect routines as soon as their dependent tasks complete and input data is ready. Therefore, you can effectively control the degree of parallelism with semaphores, and they can be easily integrated with the middleware feature.

You can also define more sophisticated middleware that controls execution order by introducing priorities to the semaphore. Middleware can be a powerful abstraction in Proction.

## Multiple Outputs

To define a routine with multiple outputs, use `procN` / `toFuncN`. The output argument and return value are tuple-like arrays. You can use multiple-output routines in the same way as the single-output variants.

```ts
const sincosProc = procN(([sinOut, cosOut]: [number[], number[]], x: number[]) => {
  for (let i = 0; i < sinOut.length; i++) {
    sinOut[i] = Math.sin(x[i]);
    cosOut[i] = Math.cos(x[i]);
  }
});
// function sincosProc(outs: [Handle<number[]>, Handle<number[]>], x: Handle<number[]>): void;

const sincosFunc = toFuncN(sincosProc, [(x) => provide(x.length), (x) => provide(x.length)]);
// function sincosFunc(x: Handle<number[]>): [Handle<number[]>, Handle<number[]>];

const ctx = new Context();
async function sincos(sinOut: number[], cosOut: number[], x: number[]) {
  await run(ctx, ({ $s, $d }) => {
    sincosProc([$d(sinOut), $d(cosOut)], $s(x));
  });
}
```

## Conclusion

Proction reconciles maintainable composition with tight control over resources and execution.

Get started by writing a small procedure with `proc(...)`, derive a function using `toFunc(...)` and a provider, then compose your complex pipeline inside `run(...)`. This keeps function-style readability while retaining procedure-style performance and flexibility.
