import * as Sentry from "@sentry/node";
import { formatDuration, intervalToDuration } from "date-fns";
import {
  IJobsCronTask,
  IJobsSimple,
  SimpleJobStatus,
  JobType,
} from "../../common/model.ts";
import {
  CronDef,
  ILogger,
  JobDef,
  getLogger,
  getOptions,
  getJobRepository,
} from "../setup.ts";
import { workerId } from "./heartbeat.ts";
import { notifySentryJobEnd, notifySentryJobStart } from "./sentry.ts";

function getJobSimpleDef(job: IJobsSimple): JobDef | null {
  const options = getOptions();
  return options.jobs[job.name] ?? null;
}

function getCronTaskDef(job: IJobsCronTask): CronDef | null {
  const options = getOptions();
  return options.crons[job.name] ?? null;
}

function stringifyError<T>(error: T): string | T {
  if (error instanceof Error) {
    const message =
      error.stack?.split("\n").slice(0, 3).join("\n") ?? error.message;

    if (!error.cause) return message;

    return `${message}\nCaused by: ${stringifyError(error.cause)}`;
  }

  return error;
}

async function onRunnerExit(
  job: IJobsCronTask | IJobsSimple,
  error: string | null,
  result: unknown,
  jobLogger: ILogger,
) {
  const startDate = job.started_at ?? new Date();
  const endDate = new Date();
  const ts = endDate.getTime() - startDate.getTime();
  const duration =
    formatDuration(intervalToDuration({ start: startDate, end: endDate })) ||
    `${ts}ms`;

  const status = error ? SimpleJobStatus.Errored : SimpleJobStatus.Finished;
  await getJobRepository().updateJob(job._id, {
    status: error ? SimpleJobStatus.Errored : SimpleJobStatus.Finished,
    output: { duration, result, error },
    ended_at: endDate,
    worker_id: null,
  });
  await notifySentryJobEnd(job, !error);

  if (job.type === JobType.Simple) {
    const onJobExited = getJobSimpleDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob =
        (await getJobRepository().getSimpleJob(job._id)) ?? job;
      try {
        await onJobExited(updatedJob);
      } catch (errored) {
        jobLogger.error({ error, job }, "job-processor: onJobExited failed");
        Sentry.captureException(error, { extra: { job } });
      }
    }
  } else {
    const onJobExited = getCronTaskDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob =
        (await getJobRepository().getCronTaskJob(job._id)) ?? job;
      try {
        await onJobExited(updatedJob);
      } catch (errored) {
        jobLogger.error({ error, job }, "job-processor: onJobExited failed");
        Sentry.captureException(error, { extra: { job } });
      }
    }
  }

  return { status, duration };
}

function getJobAbortedCb(
  job: IJobsSimple | IJobsCronTask,
  jobLogger: ILogger,
): () => Promise<void> {
  return async () => {
    try {
      // As soon as the process is abort, we update job status
      // We still wait for completion of the handler
      // but in case it didn't return in time, we still have a better status
      const resumable =
        job.type === JobType.Simple
          ? getJobSimpleDef(job)?.resumable
          : getCronTaskDef(job)?.resumable;

      if (resumable === true) {
        await getJobRepository().updateJob(job._id, {
          status: SimpleJobStatus.Paused,
          worker_id: null,
        });
      } else {
        const error = new Error("[job-processor] Job aborted");
        Sentry.captureException(error, {
          extra: { job },
        });
        getLogger().error({ error, job }, "job-processor: job aborted");
        await onRunnerExit(job, "Interrupted", null, jobLogger);
      }
    } catch (err) {
      Sentry.captureException(err, { extra: { job } });
      getLogger().error(
        { error: err, job },
        "job-processor: job abort unexpected error",
      );
    }
  };
}

async function runner(
  job: IJobsCronTask | IJobsSimple,
  signal: AbortSignal,
): Promise<number> {
  const jobLogger = getLogger().child({
    _id: job._id,
    jobName: job.name,
    created_at: job.created_at,
    updated_at: job.updated_at,
  });

  jobLogger.info("job started");
  job.started_at = job.started_at ?? new Date();
  job.status = SimpleJobStatus.Running;
  job.worker_id = workerId;

  const onAbort = getJobAbortedCb(job, jobLogger);
  signal.addEventListener("abort", onAbort, { once: true });

  await getJobRepository().updateJob(job._id, {
    status: job.status,
    started_at: job.started_at,
    worker_id: job.worker_id,
  });
  let error: string | null = null;
  let result: unknown = undefined;

  try {
    if (job.type === JobType.Simple) {
      const jobDef = getJobSimpleDef(job);
      if (!jobDef) {
        throw new Error("Job not found");
      }
      result = await jobDef.handler(job, signal);
    } else {
      const cronDef = getCronTaskDef(job);
      if (!cronDef) {
        throw new Error("Cron not found");
      }
      result = await cronDef.handler(signal);
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    if (err === signal.reason) {
      // No need to update, it's already handled by onAbort
      return 2;
    }

    Sentry.captureException(err, { extra: { job } });
    jobLogger.error(
      { err, writeErrors: err.writeErrors, error: err },
      "job error",
    );
    error = stringifyError(err) ?? "job-erorr: unknown Error";
  }

  signal.removeEventListener("abort", onAbort);

  const { status, duration } = await onRunnerExit(
    job,
    error,
    result,
    jobLogger,
  );

  jobLogger.info({ status, duration }, "job ended");

  return error ? 1 : 0;
}

export function executeJob(
  job: IJobsCronTask | IJobsSimple,
  signal: AbortSignal | null,
): Promise<number> {
  if ("runWithAsyncContext" in Sentry) {
    return Sentry.runWithAsyncContext(async () => {
      const hub = Sentry.getCurrentHub();
      const transaction = hub?.startTransaction({
        name: `JOB: ${job.name}`,
        op: "processor.job",
      });
      hub?.configureScope((scope) => {
        scope.setSpan(transaction);
        scope.setTag("job", job.name);
        scope.setContext("job", job);
      });
      await notifySentryJobStart(job);
      const start = Date.now();
      try {
        const s = signal ?? new AbortController().signal;
        const result = await runner(job, s);
        await notifySentryJobEnd(job, true);
        return result;
      } catch (err) {
        await notifySentryJobEnd(job, false);
        throw err;
      } finally {
        transaction?.setMeasurement(
          "job.execute",
          Date.now() - start,
          "millisecond",
        );
        transaction?.finish();
      }
    });
  } else {
    // @ts-expect-error Sentry v8
    return Sentry.withIsolationScope(async (scope: Sentry.Scope) => {
      scope.setContext("job", job);
      return await Sentry.startSpan(
        {
          op: "processor.job",
          name: `JOB: ${job.name}`,
          tags: {
            job: job.name,
          },
        },
        async () => {
          await notifySentryJobStart(job);
          const start = Date.now();
          try {
            const s = signal ?? new AbortController().signal;
            const result = await runner(job, s);
            return result;
          } finally {
            Sentry.setMeasurement(
              "job.execute",
              Date.now() - start,
              "millisecond",
            );
          }
        },
      );
    });
  }
}

export async function reportJobCrash(
  job: IJobsCronTask | IJobsSimple,
): Promise<void> {
  try {
    if (job.type === JobType.Simple) {
      const jobDef = getJobSimpleDef(job);
      if (!jobDef) {
        throw new Error("Job not found");
      }
      await jobDef.onJobExited?.(job);
    } else {
      const cronDef = getCronTaskDef(job);
      if (!cronDef) {
        throw new Error("Cron not found");
      }
      await notifySentryJobEnd(job, false);
      await cronDef.onJobExited?.(job);
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    Sentry.captureException(err, { extra: { job } });
    getLogger().error(
      { err, writeErrors: err.writeErrors, error: err, job },
      "reportJobCrash error",
    );
  }
}
