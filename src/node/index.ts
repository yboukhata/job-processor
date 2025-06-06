import { captureException } from "@sentry/node";
import { IJobsSimple } from "../common/model.ts";
import { cronsInit, startCronScheduler } from "./crons/crons.ts";
import { getJobRepository, getLogger } from "./setup.ts";
import { startHeartbeat, startSyncHeartbeat } from "./worker/heartbeat.ts";
import { runJobProcessor } from "./worker/processor.ts";
import { executeJob } from "./worker/worker.ts";

type ScheduleJobParams = Pick<IJobsSimple, "name" | "payload"> &
  Partial<Pick<IJobsSimple, "scheduled_for">>;

type AddJobSimpleParams = ScheduleJobParams & { queued?: boolean };

export async function scheduleJob({
  name,
  payload,
  scheduled_for = new Date(),
}: ScheduleJobParams): Promise<IJobsSimple> {
  const job = await getJobRepository().createJobSimple({
    name,
    payload,
    scheduled_for,
    sync: false,
  });

  return job;
}

export async function createAndRunJob({
  name,
  payload,
}: Pick<IJobsSimple, "name" | "payload">): Promise<number> {
  const job = await getJobRepository().createJobSimple({
    name,
    payload,
    scheduled_for: new Date(),
    sync: true,
  });

  const finallyCb = await startSyncHeartbeat();

  return executeJob(job, null).finally(finallyCb);
}

export async function addJob({
  name,
  payload,
  scheduled_for = new Date(),
  queued = false,
}: AddJobSimpleParams): Promise<number> {
  if (queued) {
    return scheduleJob({ name, payload, scheduled_for }).then(() => 0);
  }

  return createAndRunJob({ name, payload });
}

export async function startJobProcessor(
  sourceSignal: AbortSignal,
): Promise<void> {
  const ctrl = new AbortController();
  const signal = AbortSignal.any([sourceSignal, ctrl.signal]);

  const teardownHeartbeat = await startHeartbeat(true, signal);

  try {
    if (signal.aborted) {
      getLogger().info(
        "job-processor: abort already request - cancelling start",
      );
      return;
    }

    getLogger().info("job-processor: will initialise CRONs");
    await cronsInit();

    if (signal.aborted) {
      getLogger().info(
        "job-processor: abort already request - cancelling start",
      );
      return;
    }

    getLogger().info("job-processor: will start CRON scheduler");
    await startCronScheduler(signal);

    if (signal.aborted) {
      getLogger().info(
        "job-processor: abort already request - cancelling start",
      );
      return;
    }

    getLogger().info("job-processor: will start processor");
    await runJobProcessor(signal);
  } catch (error) {
    if (signal.reason !== error) {
      getLogger().error({ error }, "job-processor crashed");
      captureException(error);
      throw error;
    }
  } finally {
    if (!signal.aborted) {
      // Unexpected error, we need to abort to cancel heartbeat
      ctrl.abort();
    }

    // heartbeat need to be awaited to let last mongodb updates to run
    await teardownHeartbeat();
    getLogger().info("job-processor stopped");
  }
}

export * from "../common/index.ts";
export { initJobProcessor } from "./setup.ts";
export type * from "./setup.ts";

export {
  getProcessorHealthcheck,
  getProcessorStatus,
} from "./monitoring/monitoring.ts";

export async function getSimpleJob(id: string): Promise<IJobsSimple | null> {
  return getJobRepository().getSimpleJob(id);
}
