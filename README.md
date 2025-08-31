# Proction

An ergonomic resource-aware dataflow processing library.

## About

Proction is an utility library for computation heavy tasks which provides:

- Fine-grained resource management
- Parallel processing and its control
- Good integration with externally managed resources
- Intuitive interface similar to regular programming

Each feature is provided in modular and customizable way and you can combine them as you like.

## Introduction

### Problem: calculation on arrays

Let's think of calculating inner-products of many 3D vectors. The vectors are stored in structure-of-array manner. That is, we should calculate `(a * b) + (c * d) + (e * f)` where `a` to `f` are all the arrays of numbers.

We define `add` and `mul` as independent functions for maintaniability here.

An answer for the question would be like:

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
    output[i] = lht[i] + rht[i];
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

We can also define `add` and `mul` to take the output buffer as an argument. Therefore another answer would be like:

```ts
// Answer Y

function add(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
}

function mul(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
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

Let's call the first answer as "Answer X" and the second one as "Answer Y" and compare them.

#### Allocation

First, look at the allocations. In answer X, allocations are done in `add` and `mul` and allocated buffers are returned from the functions. The number of allocations is 5, which equals to the number of invocations of `add` and `mul` .

On the other hand, in answer Y, allocations are done in the caller `innerProduct` and the buffers are passed to the functions. The number of allocations is 3, which is less than in answer X.

#### Composability

How about the composability of `add` and `mul` ? In answer X, the implementation of `innerProduct` is quite straightforward. However in answer Y, we manage the lifetime of buffers manually to reduce allocations and it spoils the readability.

#### Total comparison

Both answer X and answer Y have pros and cons. Answer X seems better at maintainability. However answer Y wins in terms of efficiency. This is a trade-off, isn't it?

### Motivation

Primarily, Proction is created in order to tackle with this dilemma. Some more common problems are solved. I'm going to introduce the concepts in this library and how they will be used.

### Core Concepts

There are three core concepts in this library: procedures, functions and the conversion between them.

#### Procedures

In this library, a procedure is a routine which takes both input data and output buffer as arguments. A procedure just computes and writes the result to the given resource. Let's think of a procedure adding each element of two arrays.

```ts
function addProcedure(output: number[], lht: number[], rht: number[]) {
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
}
```

`addProcedure` takes `lht` and `rht` as input arguments, and `output` as an output buffer. It doesn't allocate any resource and the caller can pass any `number[]` buffers regardless of how they are allocated. On the other hand, the caller always have to allocate and manage the arrays.

This style is especially useful when the output buffer is externally managed or involving I/O features such as a framebuffer of the display or `mmap` -ed memory zones.

This is the simplest form of the routines and the best in terms of cohesion. Proction adopts procedure-style routines as primitives.

#### Functions

A function is a routine which takes only input data and returns output data. Buffers storing output data are allocated internally. The adding function can be written like this:

```ts
function addFunction(lht: number[], rht: number[]): number[] {
  const output: number[] = new Array(lht.length);
  for (let i = 0; i < output.length; i++) {
    output[i] = lht[i] + rht[i];
  }
  return output;
}
```

`addFunction` only takes `lht` and `rht` as input arguments. The result is returned by the function and the array is allocated internally. The caller is free from explicitly allocating an output buffer, however it cannot control how the buffer is allocated. For example, object pools or custom allocators on `ArrayBuffer` s can't be used in this style.

Function-style routines are very easy to use. Proction lets you compose subroutines in this form.

#### Indirection and style conversion

Proction utilizes indirected routines and provides tools for creating indirect procedures and converting indirect procedures into indirect functions.

Indirect routines takes and returns indirect handles instead of the actual objects. The signature would be like below.

```ts
function indirectAddProcedure(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
```

To make indirect procedures easily, Proction provides `proc` utility. You can define `indirectAddProcedure` like this.

```ts
const indirectAddProcedure = proc()(
  function addProcedure(output: number[], lht: number[], rht: number[]) {
    for (let i = 0; i < output.length; i++) {
      output[i] = lht[i] + rht[i];
    }
  }
);
```

`toFunc` utility renders indirect procedures into indirect functions.

```ts
const prov = provider((length) => new Array(length), () => {});

function indirectAddFunction(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>;
const indirectAddFunction = toFunc(indirectAddProcedure, (lht, _rht) => prov.provide(lht.length));
```

Here is a new concept `provider` required for the conversion. In this example the provider newly allocates an array for every function return value. I'll describe the details of provider and how to optimize the allocation later.

(As you may notice, "Proction" is a portmanteau word of "procedure" and "function.")

### Performing the calculation

We can call and combine indirect routines to perform more complex computation in `run` block.

```ts
function addProc(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
function addFunc(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>;

function mulProc(output: Handle<number[]>, lht: Handle<number[]>, rht: Handle<number[]>);
function mulFunc(lht: Handle<number[]>, rht: Handle<number[]>): Handle<number[]>;

const ctx = new Context();
function innerProduct(output: number[], a: number[], b: number[], c: number[], d: number[], e: number[], f: number[]) {
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

Handles of input data can be created with `$s` and ones of output data with `$d` . As you can see, you can combine indirect procedures and functions in very intuitive way.

### Providers

Providers are attached to indirect functions and enables to manage how intermediate buffers are allocated and freed irrespective of the implementation details of base indirect procedures.

In the previous example, we used a provider which allocates a new array every time a buffer is required. Let's use an object pool to minimize allocations.

```ts
interface ArrayPool {
  acquire(length: number) => number[];
  release(obj: number[]);
}
const pool: ArrayPool = { /* some implementations */ };

const prov = provider((length) => pool.aqcuire(length), (obj) => pool.release(obj));
const indirectAddFunctionWithPool = toFunc(indirectAddProcedure, (lht, _rht) => prov.provide(lht.length));
```

As you see, you can completely reuse `indirectAddProcedure` and customize the resource management. The objects are released to the provider immediately when they become no longer required. Thanks to object pools, the number of array allocations are minimized and buffers are reused if possible.

### Parallelism

`proc` can take `async` JavaScript functions as the body to achieve parallel computing. You can use `Worker` s to utilize the power of multi-core CPU. Here is a very simplified example.

```ts
const worker: Worker;
const indirectAddProcedure = proc()(
  async function addProcedure(output: number[], lht: number[], rht: number[]) {
    const { promise, resolve } = Promise.withResolvers();
    worker.onmessage = resolve;
    worker.postMessage([output, lht, rht]);
    await promise;
  }
);
```

Function-style indirect routines and Proction's task scheduler prevent data races and help maximizing CPU utilization automatically.

### Middlewares

Middlewares in Proction are like those in other JavaScript libraries. They work as a wrapper for indirect routines and it should invoke the next action. Middlewares can be installed when you create indirect procedures with `proc` .

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

Because middlewares are defined as `async` JavaScript functions, middlewares are also useful for scheduling the routine execution.

```ts
interface Semaphore {
  async acquire();
  async release();
}
const semaphore: Semaphore;

const serializationMiddleware = async (next) => {
  await semaphore.acquire();
  await next();
  await semaphore.release();
};
```

Proction starts all invocations of indirect routines as soon as their dependent tasks complete and input data are ready. Therefore you can effectively control the number of parallel execution with semaphores and they can be easily integrated with the middleware feature.

If you want, you can define more complicated middlewares which control the order of execution if you introduce priorities to the semaphore. Middlewares can be very powerful in Proction.

### Conclusion

Proction reconciles maintainable composition with tight control over resources and execution.

Get started by writing a small procedure with `proc(...)`, derive a function using `toFunc(...)` and a simple provider, then compose your pipeline inside `run(...)`. You keep function-style readability while retaining procedure-style performance and flexibility.

## License

See `LICENSE.txt` .

## Notice

This library is under construction and the API design would be changed.
