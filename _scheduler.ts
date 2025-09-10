/**
 * The scheduler interface for tasks corresponding to invocations
 */
export interface Scheduler {
  /**
   * A method to execute a task on the scheduler.
   * @param task The executable task whose dependencies are already resolved.
   * @returns A promise resolves on the task completion.
   */
  spawn: (task: () => Promise<void>) => Promise<void>;
}

export const defaultScheduler: Scheduler = {
  spawn: (task: () => Promise<void>) => task(),
};
