import {
  assertIsChildTypeOf,
  ChildType,
  ParentType,
} from "./_testutils/mod.ts";
import { DisposableWrap } from "./_provider.ts";

assertIsChildTypeOf<DisposableWrap<ChildType>, DisposableWrap<ParentType>>();
// @ts-expect-error: DisposableWrap is covariant
assertIsChildTypeOf<DisposableWrap<ParentType>, DisposableWrap<ChildType>>();
