export type ParentType = {
  a: number;
};
export type ChildType = {
  a: number;
  b: number;
};

export function assertIsChildTypeOf<A extends B, B>(_a?: A, _b?: B) {}

export function testValue<A>(): A {
  throw new Error("testValue");
}
