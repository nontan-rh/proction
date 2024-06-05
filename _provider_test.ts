import {
  assertIsChildTypeOf,
  ChildType,
  ParentType,
} from "./_testutils/mod.ts";
import { AllocatorResult, ProvidedWrap } from "./_provider.ts";

assertIsChildTypeOf<AllocatorResult<ChildType>, AllocatorResult<ParentType>>();
// @ts-expect-error: AllocatorResult is covariant
assertIsChildTypeOf<AllocatorResult<ParentType>, AllocatorResult<ChildType>>();

// @ts-expect-error: ProvidedWrap is invariant
assertIsChildTypeOf<ProvidedWrap<ChildType>, ProvidedWrap<ParentType>>();
// @ts-expect-error: ProvidedWrap is invariant
assertIsChildTypeOf<ProvidedWrap<ParentType>, ProvidedWrap<ChildType>>();
