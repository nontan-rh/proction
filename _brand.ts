/**
 * A utility type to create branded types.
 * @typeparam K The actual type.
 * @typeparam T The brand string.
 * @returns The branded type.
 */
export type Brand<K, T extends string> = K & { [key in T]: never };
