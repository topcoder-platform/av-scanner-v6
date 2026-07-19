/** FIFO semaphore that bounds concurrent ClamAV scans within one task. */
export class Semaphore {
  private active = 0;
  private readonly waiters: Array<() => void> = [];

  /**
   * Creates a semaphore for the configured scan concurrency.
   *
   * @param capacity - Maximum number of simultaneous holders.
   * @throws When capacity is not a positive integer.
   */
  constructor(private readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError("Semaphore capacity must be a positive integer");
    }
  }

  /**
   * Waits for a slot and returns an idempotent release callback.
   * ScanProcessor uses this before opening a ClamAV socket.
   *
   * @returns A callback that releases the acquired slot.
   */
  async acquire(): Promise<() => void> {
    if (this.active >= this.capacity) {
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
      });
    }

    this.active += 1;
    let released = false;

    return () => {
      if (released) {
        return;
      }

      released = true;
      this.active -= 1;
      this.waiters.shift()?.();
    };
  }
}
