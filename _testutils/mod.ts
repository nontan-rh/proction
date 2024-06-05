export class ParentType {
  a: number = 0;
}
export class ChildType extends ParentType {
  b: number = 0;
}

export function assertIsChildTypeOf<A extends B, B>(_a?: A, _b?: B) {}
