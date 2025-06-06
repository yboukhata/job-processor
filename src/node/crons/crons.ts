import { CronExpressionParser } from "cron-parser";
import { CronDef, getLogger, getOptions, getJobRepository } from "../setup.ts";
import { captureException } from "@sentry/node";
import { EventEmitter } from "node:events";

function parseCronString(
  now: Date,
  cronString: string,
  options: { currentDate: string } | object = {},
): Date {
  const iterator = CronExpressionParser.parse(cronString, {
    tz: "Europe/Paris",
    ...options,
  });

  const next = iterator.next().toDate();

  if (next.getTime() >= now.getTime()) {
    return next;
  }

  return now;
}

interface Cron extends CronDef {
  name: string;
}

function getCrons(): Cron[] {
  return Object.entries(getOptions().crons).map(([name, cronDef]) => ({
    ...cronDef,
    name,
  }));
}

export async function cronsInit() {
  getLogger().debug(`Crons - initialise crons in DB`);

  const CRONS = getCrons();

  // Use adapter for job deletion
  const jobRepository = getJobRepository();
  await jobRepository.deleteCronsNotIn(CRONS.map((c) => c.name));

  for (const cron of CRONS) {
    const now = new Date();
    // Use adapter for upsert/find/update
    const oldJob = await jobRepository.upsertCronJob(cron, now);

    if (oldJob !== null && oldJob.cron_string !== cron.cron_string) {
      await jobRepository.updateCronSchedule(oldJob._id, now);
      await jobRepository.deletePendingCronTasks(cron.name);
      getLogger().info(
        {
          oldJob: { ...oldJob, _id: oldJob._id.toString() },
          now,
          cron,
        },
        `job_processor: cron schedule updated`,
      );
    }
  }
}

export const cronSchedulerEvent = new EventEmitter();

export async function runCronsScheduler(): Promise<void> {
  getLogger().debug(`Crons - Check and run crons`);

  const now = new Date();
  const jobRepository = getJobRepository();
  const crons = await jobRepository.findDueCronJobs(now);

  for (const cron of crons) {
    const next = parseCronString(now, cron.cron_string ?? "", {
      currentDate: cron.scheduled_for,
    });

    // Use adapter for update
    const updated = await jobRepository.updateCronScheduledFor(
      cron._id,
      cron.scheduled_for,
      next,
    );
    if (updated) {
      await jobRepository.createJobCronTask({
        name: cron.name,
        scheduled_for: next,
      });
    }
  }

  cronSchedulerEvent.emit("updated");
}

export async function startCronScheduler(signal: AbortSignal) {
  const CRONS = getCrons();
  if (CRONS.length === 0) return;

  await runCronsScheduler();
  const intervalID = setInterval(async () => {
    try {
      await runCronsScheduler();
    } catch (err) {
      if (!signal.aborted) {
        getLogger().error(
          { error: err },
          "job-processor: cron scheduler failed",
        );
        captureException(err);
      }
    }
  }, 60_000).unref();

  signal.addEventListener(
    "abort",
    () => {
      getLogger().info(
        "job-processor: abort requested - stopping cron scheduler",
      );
      clearInterval(intervalID);
      getLogger().info(
        "job-processor: abort requested - cron scheduler stopped",
      );
    },
    { once: true },
  );
}
