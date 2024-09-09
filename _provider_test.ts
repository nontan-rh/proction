import {
  assertIsChildTypeOf,
  ChildType,
  ParentType,
} from "./_testutils/mod.ts";
import { AllocatorResult } from "./_provider.ts";

assertIsChildTypeOf<AllocatorResult<ChildType>, AllocatorResult<ParentType>>();
// @ts-expect-error: AllocatorResult is covariant
assertIsChildTypeOf<AllocatorResult<ParentType>, AllocatorResult<ChildType>>();
