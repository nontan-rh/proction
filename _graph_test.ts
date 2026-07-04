import { assert, assertEquals, assertNotEquals } from "@std/assert";
import {
  alwaysChangedDataVersion,
  type DataID,
  type DataVersion,
  generateProcID,
  Graph,
  type InvocationDraft,
  unknownDataVersion,
  unresolvedIntermediateDataID,
  unresolvedIntermediateDataVersion,
  versionToSourceDataVersion,
} from "./_graph.ts";

Deno.test(function resolveDataIDIdentity() {
  const graph = new Graph();
  const a = {};
  const b = {};

  const aID = graph.resolveDataID(a);
  const bID = graph.resolveDataID(b);

  assertEquals(graph.resolveDataID(a), aID);
  assertEquals(graph.resolveDataID(b), bID);
  assertNotEquals(aID, bID);
});

Deno.test(function sourceVersionsAreDisjointFromGeneratedVersions() {
  const graph = new Graph();

  // Generated versions are even; caller-managed source versions map to odd,
  // so the two namespaces can never collide.
  const run1 = graph.beginRun();
  const run2 = graph.beginRun();
  assertEquals(run1.version % 2, 0);
  assertEquals(run2.version % 2, 0);
  assertNotEquals(run1.version, run2.version);

  assertEquals(versionToSourceDataVersion(0) % 2, 1);
  assertEquals(versionToSourceDataVersion(1) % 2, 1);
  assertNotEquals(versionToSourceDataVersion(1), versionToSourceDataVersion(2));
});

Deno.test(function missAssignsIDsAndVersions() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const run = graph.beginRun();

  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };
  const resolved = run.resolve(draft);

  assertEquals(resolved.unchanged, false);
  assert(resolved.outputIDs[0] >= 0);
  assertEquals(resolved.outputVersions, [run.version]);
});

Deno.test(function identicalResubmissionIsUnchangedAfterCommit() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  const run1 = graph.beginRun();
  const first = run1.resolve(draft);
  run1.commit();

  const run2 = graph.beginRun();
  const second = run2.resolve(draft);

  assertEquals(second.unchanged, true);
  assertEquals(second.outputIDs, first.outputIDs);
  assertEquals(second.outputVersions, first.outputVersions);
});

Deno.test(function uncommittedResolutionLeavesNoRecord() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  // The run failed: commit() is never called.
  const run1 = graph.beginRun();
  const first = run1.resolve(draft);

  const run2 = graph.beginRun();
  const second = run2.resolve(draft);

  assertEquals(second.unchanged, false);
  assertNotEquals(second.outputIDs, first.outputIDs);
});

Deno.test(function duplicateResolutionsInOneRunBothMiss() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  // A record produced by this run is not visible to this run's resolutions:
  // an identically-wired sibling must execute on its first submission.
  const run = graph.beginRun();
  const first = run.resolve(draft);
  const second = run.resolve(draft);

  assertEquals(first.unchanged, false);
  assertEquals(second.unchanged, false);
  assertNotEquals(second.outputIDs, first.outputIDs);
});

Deno.test(function inputVersionBumpChangesThenConverges() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draftWithVersion = (version: number): InvocationDraft => ({
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(version)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  });

  const run1 = graph.beginRun();
  const first = run1.resolve(draftWithVersion(1));
  run1.commit();

  const run2 = graph.beginRun();
  const second = run2.resolve(draftWithVersion(2));
  run2.commit();

  assertEquals(second.unchanged, false);
  assertEquals(second.outputIDs, first.outputIDs);
  assertEquals(second.outputVersions, [run2.version]);
  assertNotEquals(second.outputVersions, first.outputVersions);

  // The record is updated on a changed hit, so an identical resubmission
  // converges to unchanged.
  const run3 = graph.beginRun();
  const third = run3.resolve(draftWithVersion(2));

  assertEquals(third.unchanged, true);
  assertEquals(third.outputVersions, second.outputVersions);
});

Deno.test(function destinationVersionRoundTrip() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const destinationID = graph.resolveDataID({});
  const draft = (
    inputVersion: number,
    outputVersion: DataVersion,
  ): InvocationDraft => ({
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(inputVersion)],
    outputIDs: [destinationID],
    outputVersions: [outputVersion],
    providerIDs: [unresolvedIntermediateDataID],
  });

  const run1 = graph.beginRun();
  const first = run1.resolve(draft(1, unknownDataVersion));
  run1.commit();
  assertEquals(first.unchanged, false);
  assertEquals(first.outputIDs, [destinationID]);

  // A round-tripped version matches.
  const run2 = graph.beginRun();
  const second = run2.resolve(draft(1, first.outputVersions[0]));
  assertEquals(second.unchanged, true);

  // An unknown destination version never matches, but with unchanged inputs
  // the recorded version is kept: the re-execution reproduces the recorded
  // content, so sibling consumers are not invalidated spuriously.
  const run3 = graph.beginRun();
  const third = run3.resolve(draft(1, unknownDataVersion));
  assertEquals(third.unchanged, false);
  assertEquals(third.outputVersions, first.outputVersions);

  // A changed input re-mints the output version, so the old version no
  // longer matches afterwards.
  const run4 = graph.beginRun();
  const fourth = run4.resolve(draft(2, first.outputVersions[0]));
  run4.commit();
  assertEquals(fourth.unchanged, false);
  assertEquals(fourth.outputVersions, [run4.version]);

  const run5 = graph.beginRun();
  const fifth = run5.resolve(draft(2, first.outputVersions[0]));
  assertEquals(fifth.unchanged, false);
});

Deno.test(function alwaysChangedNeverConverges() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [alwaysChangedDataVersion],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  const run1 = graph.beginRun();
  const first = run1.resolve(draft);
  run1.commit();

  const run2 = graph.beginRun();
  const second = run2.resolve(draft);
  run2.commit();

  assertEquals(first.unchanged, false);
  assertEquals(second.unchanged, false);
  assertNotEquals(second.outputVersions, first.outputVersions);
});

Deno.test(function distinctSignaturesAreIndependent() {
  const graph = new Graph();
  const procID = generateProcID();
  const otherProcID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draftWithProc = (id: typeof procID): InvocationDraft => ({
    procID: id,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  });

  const run = graph.beginRun();
  const first = run.resolve(draftWithProc(procID));
  const other = run.resolve(draftWithProc(otherProcID));

  assertEquals(other.unchanged, false);
  assertNotEquals(other.outputIDs, first.outputIDs);
});

Deno.test(function distinctProvidersAreIndependent() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerA = graph.resolveDataID(() => {});
  const providerB = graph.resolveDataID(() => {});
  const draftWithProvider = (providerID: DataID): InvocationDraft => ({
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  });

  const run1 = graph.beginRun();
  const first = run1.resolve(draftWithProvider(providerA));
  run1.commit();

  // The same wiring backed by a different provider is a different
  // computation and must not match the record.
  const run2 = graph.beginRun();
  const second = run2.resolve(draftWithProvider(providerB));

  assertEquals(second.unchanged, false);
  assertNotEquals(second.outputIDs, first.outputIDs);
});

Deno.test(function invalidateDropsTheRecord() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const destinationID = graph.resolveDataID({});
  const draft = (outputVersion: DataVersion): InvocationDraft => ({
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [destinationID],
    outputVersions: [outputVersion],
    providerIDs: [unresolvedIntermediateDataID],
  });

  const run1 = graph.beginRun();
  const first = run1.resolve(draft(unknownDataVersion));
  run1.commit();

  const run2 = graph.beginRun();
  const second = run2.resolve(draft(first.outputVersions[0]));
  assertEquals(second.unchanged, true);
  // The invocation failed after possibly writing part of its output.
  run2.invalidate(draft(first.outputVersions[0]));

  const run3 = graph.beginRun();
  const third = run3.resolve(draft(first.outputVersions[0]));
  assertEquals(third.unchanged, false);
});

Deno.test(function commitEvictsRecordsTheRunDidNotResolve() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  const run1 = graph.beginRun();
  const first = run1.resolve(draft);
  run1.commit();

  // A committed run that does not resolve the invocation drops its record.
  const run2 = graph.beginRun();
  run2.commit();

  // The record was evicted: the invocation re-executes (never a wrong skip).
  const run3 = graph.beginRun();
  const third = run3.resolve(draft);
  assertEquals(third.unchanged, false);
  assertNotEquals(third.outputIDs, first.outputIDs);
});

Deno.test(function uncommittedRunEvictsNothing() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  const run1 = graph.beginRun();
  const first = run1.resolve(draft);
  run1.commit();

  // A failed run never commits and leaves the committed records as they are.
  graph.beginRun();

  const run3 = graph.beginRun();
  const third = run3.resolve(draft);
  assertEquals(third.unchanged, true);
  assertEquals(third.outputIDs, first.outputIDs);
});

Deno.test(function unchangedResolutionCarriesTheRecordOverCommit() {
  const graph = new Graph();
  const procID = generateProcID();
  const sourceID = graph.resolveDataID({});
  const providerID = graph.resolveDataID(() => {});
  const draft: InvocationDraft = {
    procID,
    inputIDs: [sourceID],
    inputVersions: [versionToSourceDataVersion(1)],
    outputIDs: [unresolvedIntermediateDataID],
    outputVersions: [unresolvedIntermediateDataVersion],
    providerIDs: [providerID],
  };

  const run1 = graph.beginRun();
  const first = run1.resolve(draft);
  run1.commit();

  // An unchanged resolution keeps the record alive across the commit.
  const run2 = graph.beginRun();
  const second = run2.resolve(draft);
  assertEquals(second.unchanged, true);
  run2.commit();

  const run3 = graph.beginRun();
  const third = run3.resolve(draft);
  assertEquals(third.unchanged, true);
  assertEquals(third.outputIDs, first.outputIDs);
});
