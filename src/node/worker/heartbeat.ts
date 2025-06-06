import { ObjectId } from "mongodb";
import os from "node:os";
import { getLogger, getOptions, getJobRepository } from "../setup.ts";
import { captureException, flush } from "@sentry/node";
import { EventEmitter } from "node:events";

export const workerId = new ObjectId();

export const heartbeatEvent = new EventEmitter();

async function createWorker() {
  const jobRepository = getJobRepository();
  await jobRepository.upsertWorker({
    _id: workerId,
    hostname: os.hostname(),
    lastSeen: new Date(),
    tags: getOptions().workerTags ?? null,
  });
}

const syncHeartbeatContext: { count: number; ctrl: AbortController | null } = {
  count: 0,
  ctrl: null,
};

// Sync heartbeat is used in the context of addJob sync
// We still need to add workerId to be able to detect crashes
// So the process will report heartbeat as long as it is running a job
// But we don't want to multiply heartbeat requests with concurrent addJob
export async function startSyncHeartbeat(): Promise<() => void> {
  syncHeartbeatContext.count++;

  let teardown: null | (() => Promise<null>) = null;
  if (syncHeartbeatContext.count === 1) {
    syncHeartbeatContext.ctrl = new AbortController();

    teardown = await startHeartbeat(false, syncHeartbeatContext.ctrl.signal);
  }

  return async () => {
    syncHeartbeatContext.count--;
    if (syncHeartbeatContext.count === 0) {
      const ctrl = syncHeartbeatContext.ctrl;
      syncHeartbeatContext.ctrl = null;
      ctrl?.abort();
      await teardown?.();
    }
  };
}

export async function startHeartbeat(
  isWorker: boolean,
  signal: AbortSignal,
): Promise<() => Promise<null>> {
  await createWorker();

  let successiveErrorsCount = 0;
  const jobRepository = getJobRepository();

  const intervalId = setInterval(
    async () => {
      try {
        const result = await jobRepository.updateWorkerHeartbeat(
          workerId,
          new Date(),
        );

        if (!result) {
          const error = new Error(
            "job-processor: worker has been detected as died",
          );
          throw error;
        }

        heartbeatEvent.emit("ping");
        successiveErrorsCount = 0;
      } catch (error) {
        if (signal.aborted) {
          return;
        }

        successiveErrorsCount++;

        getOptions().logger.error({ error }, "job-processor: heartbeat failed");
        captureException(error, { extra: { workerId, successiveErrorsCount } });

        if (successiveErrorsCount < 3) {
          heartbeatEvent.emit("fail");
          getOptions().logger.info("job-processor: waiting for next check");
          return;
        }

        if (!isWorker) {
          // Force recreation in case it was removed
          return createWorker()
            .then(() => {
              heartbeatEvent.emit("ping");
            })
            .catch(() => {
              // Silent
            });
        }

        heartbeatEvent.emit("kill");
        clearInterval(intervalId);

        // Any failure in heartbeat is critical, just kill the process to prevent race conditions
        // Do not exit on tests
        if (process.env["NODE_ENV"] !== "test") {
          await flush();
          // eslint-disable-next-line n/no-process-exit
          process.exit(1);
        }
      }
    },
    // 30 seconds: needs to be way lower than expireAfterSeconds index
    30_000,
  ).unref();

  const teardownPromise: Promise<null> = new Promise((resolve) => {
    signal.addEventListener(
      "abort",
      async () => {
        clearInterval(intervalId);

        getLogger().info("job-processor: abort requested - stopping heartbeat");

        if (isWorker) {
          await jobRepository.removeWorker(workerId).catch((error) => {
            getLogger().error(
              { error },
              "job-processor: worker self-removal failed",
            );
            captureException(error, { extra: { workerId } });
          });
        }

        getLogger().info("job-processor: abort requested - heartbeat stopped");
        heartbeatEvent.emit("stop");
        resolve(null);
      },
      { once: true },
    );
  });

  return () => teardownPromise;
}
