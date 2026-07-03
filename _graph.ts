import type { Brand } from "./_brand.ts";
import { HashTable } from "./_hashtable.ts";
import { idGenerator } from "./_idgenerator.ts";
import { mixWord, fmix32 } from './_murmurhash.ts';

export type ProcID = Brand<number, "procID">;
export type DataID = Brand<number, "dataID">;
export type DataVersion = Brand<number, "dataVersion">;
export type InvocationID = Brand<number, "invocationID">;

// "Unresolved" values are used for intermediate data nodes.
export const unresolvedIntermediateDataID = -1 as DataID;
export const unresolvedIntermediateDataVersion = -1 as DataVersion;

function matchUnresolvedVersion(unresolved: DataVersion, resolved: DataVersion): boolean {
  if (unresolved === unresolvedIntermediateDataVersion) {
    return true;
  }

  return unresolved === resolved;
}

function matchUnresolvedVersionArray(unresolved: readonly DataVersion[], resolved: readonly DataVersion[]): boolean {
  if (unresolved.length !== resolved.length) {
    return false;
  }

  for (let i = 0; i < unresolved.length; i++) {
    if (!matchUnresolvedVersion(unresolved[i], resolved[i])) {
      return false;
    }
  }

  return true;
}

function equalsVersionArray(l: readonly DataVersion[], r: readonly DataVersion[]): boolean {
  if (l.length !== r.length) {
    return false;
  }

  for (let i = 0; i < l.length; i++) {
    if (l !== r) {
      return false;
    }
  }

  return true;
}

export type InvocationSignature = {
  procID: ProcID,
  inputIDs: readonly DataID[],
  outputIDs: readonly DataID[],
}

function hashInvocationSignature(x: InvocationSignature): number {
  let h = 0;
  h = mixWord(h, x.procID | 0);
  for (const inputID of x.inputIDs) {
    h = mixWord(h, inputID | 0);
  }
  for (const outputID of x.outputIDs) {
    h = mixWord(h, outputID | 0);
  }
  return fmix32(h);
}

function equalsInvocationSignature(l: InvocationSignature, r: InvocationSignature): boolean {
  if (l.procID !== r.procID) {
    return false;
  }

  if (l.inputIDs.length != r.inputIDs.length) {
    return false;
  }
  for (let i = 0; i < l.inputIDs.length; i++) {
    if (l.inputIDs[i] !== r.inputIDs[i]) {
      return false;
    }
  }

  if (l.outputIDs.length != r.outputIDs.length) {
    return false;
  }
  for (let i = 0; i < l.outputIDs.length; i++) {
    if (l.outputIDs[i] !== r.outputIDs[i]) {
      return false;
    }
  }

  return true;
}

export type Invocation = {
  invocationID: InvocationID;
  procID: ProcID,
  inputIDs: readonly DataID[],
  inputVersions: readonly DataVersion[],
  outputIDs: readonly DataID[],
  outputVersions: readonly DataVersion[],
}

export class Graph {
  generateProcID: () => ProcID = idGenerator((x: number) => x as ProcID);

  #externalDataToDataID: WeakMap<object, DataID> = new WeakMap();
  #generateDataID: () => DataID = idGenerator((x: number) => x as DataID);

  #invocations: HashTable<InvocationSignature, Invocation> = new HashTable(hashInvocationSignature, equalsInvocationSignature);
  #generateInvocationID: () => InvocationID = idGenerator((x: number) => x as InvocationID);

  #currentDataVersion: DataVersion = 1 as DataVersion;

  resolveDataID(x: object): DataID {
    const cached = this.#externalDataToDataID.get(x);
    if (cached != null) {
      return cached;
    }

    const dataID = this.#generateDataID();
    this.#externalDataToDataID.set(x, dataID);
    return dataID;
  }

  resolveInvocation(x: Invocation) {
    const cached = this.#invocations.get(x);
    if (cached == null) {
      const outputIDs = x.outputIDs.map(o => o < 0 ? this.#generateDataID() : o);
      const outputVersions = x.outputVersions.map(o => o < 0 ? this.#currentDataVersion : o);
      const saved: Invocation = {
        invocationID: this.#generateInvocationID(),
        procID: x.procID,
        inputIDs: x.inputIDs,
        inputVersions: x.inputVersions,
        outputIDs,
        outputVersions,
      };

      this.#invocations.set(x, saved);
      x.outputIDs = outputIDs;
      x.outputVersions = outputVersions;
      return;
    }

    // Check if versions match
    let outputVersions: readonly DataVersion[];
    if (equalsVersionArray(x.inputVersions, cached.inputVersions) && matchUnresolvedVersionArray(x.outputVersions, cached.outputVersions)) {
      outputVersions = cached.outputVersions;
    } else {
      outputVersions = new Array(cached.outputVersions.length).fill(this.#currentDataVersion);
    }
    x.outputIDs = cached.outputIDs;
    x.outputVersions = outputVersions;
  }
}
