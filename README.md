# Proction

An ergonomic, resource-aware, dataflow processing library for general-purpose
use.

## About

Proction is a utility library for versatile dataflow-based tasks that provides:

- Fine-grained resource management
- Intuitive interface similar to regular programming
- Good integration with externally managed resources
- Highly customizable scheduling and parallelism

Each feature is provided in a modular, customizable way, and you can combine
them as you like.

## Platforms

The primary target is Deno. The package is available on JSR:
[`jsr:@nontan-rh/proction`](https://jsr.io/@nontan-rh/proction).

## Introduction

See the [introduction article](docs/introduction.md).

## Example

```ts
import { Context, proc, provider, run, toFunc } from "jsr:@nontan-rh/proction";

const pool = {
  acquire(x: number): number[] {
    return new Array(x);
  },
  release(_x: number[]) {/* Do nothing: this is an example */},
};
const provide = provider(
  (x: number) => pool.acquire(x),
  (x) => pool.release(x),
);

const addProc = proc()(
  function add(output: number[], lht: number[], rht: number[]) {
    for (let i = 0; i < output.length; i++) {
      output[i] = lht[i] + rht[i];
    }
  },
);
const addFunc = toFunc(addProc, (lht, _rht) => provide(lht.length));

const ctx = new Context();
async function sum(output: number[], a: number[], b: number[], c: number[]) {
  await run(ctx, ({ $s, $d }) => {
    const s = addFunc($s(a), $s(b));
    addProc($d(output), s, $s(c));
  });
  // Now `output` stores the result!
}

const result = [0];
await sum(result, [1], [2], [3]);
console.log(result); // => [6]
```

## License

See [LICENSE.txt](LICENSE.txt).

## Notice

This library is under development, and the API is subject to change.
