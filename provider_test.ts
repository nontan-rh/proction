import {
  assertIsChildTypeOf,
  ChildType,
  ParentType,
} from "./_testutils/mod.ts";
import { AllocatorResult, ProvidedWrap } from "./provider.ts";

assertIsChildTypeOf<AllocatorResult<ChildType>, AllocatorResult<ParentType>>();
// @ts-expect-error: AllocatorResult is covariant
assertIsChildTypeOf<AllocatorResult<ParentType>, AllocatorResult<ChildType>>();

assertIsChildTypeOf<ProvidedWrap<ChildType>, ProvidedWrap<ParentType>>();
// @ts-expect-error: ProvidedWrap is covariant
assertIsChildTypeOf<ProvidedWrap<ParentType>, ProvidedWrap<ChildType>>();
