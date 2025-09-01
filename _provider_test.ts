import {
  assertIsChildTypeOf,
  type ChildType,
  type ParentType,
} from "./_testutils/mod.ts";
import type { DisposableWrap } from "./_provider.ts";

assertIsChildTypeOf<DisposableWrap<ChildType>, DisposableWrap<ParentType>>();
// @ts-expect-error: DisposableWrap is covariant
assertIsChildTypeOf<DisposableWrap<ParentType>, DisposableWrap<ChildType>>();
