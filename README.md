# Proction

An ergonomic, resource-aware, dataflow-processing library.

## About

Proction is a utility library for computation-heavy tasks that provides:

- Fine-grained resource management
- Parallel processing with fine-grained control
- Good integration with externally managed resources
- Intuitive interface similar to regular programming

Each feature is provided in a modular, customizable way, and you can combine
them as you like.

## Introduction

See the [introduction article](docs/introduction.md).

## Example

```ts
interface ArrayPool {
  acquire(length: number): number[];
  release(obj: number[]): void;
}
const pool: ArrayPool = {/* some implementation */};
const provide = provider((x) => pool.acquire(x), (x) => pool.release(x));

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
```

## License

See [LICENSE.txt](LICENSE.txt).

## Notice

This library is under development, and the API is subject to change.
