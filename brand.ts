export type Brand<K, T extends string> = K & { [key in T]: never };
