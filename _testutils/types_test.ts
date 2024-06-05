import { assertIsChildTypeOf, ChildType, ParentType } from "./types.ts";

let parent: ParentType = undefined!;
let child: ChildType = undefined!;

parent = child;
// @ts-expect-error: ParentType is not assignable to ChildType
child = parent;

// @ts-expect-error: ParentType is not assignable to ChildType
assertIsChildTypeOf<ParentType, ChildType>();
assertIsChildTypeOf<ChildType, ParentType>();
