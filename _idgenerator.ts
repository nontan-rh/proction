/**
 * An internal utility function to generate unique IDs.
 * @typeparam T The type of the generated ID.
 * @param transform A function to transform the counter into an ID.
 * @returns A function to generate unique IDs.
 */
export function idGenerator<T>(transform: (x: number) => T): () => T {
  let counter = 0;
  return () => {
    counter += 1;
    return transform(counter);
  };
}
