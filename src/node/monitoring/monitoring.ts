import {
  CronStatus,
  IJob,
  IJobsCronTask,
  IJobsSimple,
  isJobCron,
  isJobCronTask,
  isJobSimple,
  isJobSimpleOrCronTask,
  IWorker,
  JobStatus,
  ProcessorStatus,
  WorkerStatus,
  SimpleJobStatus,
} from "../../common/model.ts";
import { getJobRepository } from "../setup.ts";

function buildWorkerStatus(workers: IWorker[], jobs: IJob[]): WorkerStatus[] {
  return workers.map((worker): WorkerStatus => {
    const task =
      jobs.filter(isJobSimpleOrCronTask).find((job) => {
        return job.worker_id === worker._id;
      }) ?? null;

    return { worker, task };
  });
}

function buildQueueStatus(
  jobs: IJob[],
  now: Date,
): Array<IJobsSimple | IJobsCronTask> {
  const pending = jobs
    .filter(isJobSimpleOrCronTask)
    .filter(
      (job) =>
        job.status === SimpleJobStatus.Pending && job.scheduled_for <= now,
    );

  return pending;
}

function buildCronStatus(jobs: IJob[]): CronStatus[] {
  const crons = jobs.filter(isJobCron);

  return crons.map((cron) => {
    const tasks = jobs
      .filter(isJobCronTask)
      .filter((job) => job.name === cron.name);
    const scheduled = tasks.filter(
      (job) => job.status === SimpleJobStatus.Pending,
    );
    const running = tasks.filter(
      (job) =>
        job.status === SimpleJobStatus.Running ||
        job.status === SimpleJobStatus.Paused,
    );
    const history = tasks.filter(
      (job) =>
        job.status !== SimpleJobStatus.Pending &&
        job.status !== SimpleJobStatus.Running,
    );

    return {
      cron,
      scheduled,
      running,
      history,
    };
  });
}

function buildJobStatus(jobs: IJob[]): JobStatus[] {
  const names = Array.from(
    new Set(jobs.filter(isJobSimple).map((job) => job.name)),
  );

  return names.map((name) => {
    return {
      name,
      tasks: jobs.filter(isJobSimple).filter((job) => job.name === name),
    };
  });
}

export async function getProcessorStatus(): Promise<ProcessorStatus> {
  const now = new Date();
  const jobRepository = getJobRepository();
  const [workers, jobs] = await Promise.all([
    jobRepository.findWorkers(),
    jobRepository.findScheduledCronJobs(),
  ]);

  return {
    now,
    workers: buildWorkerStatus(workers, jobs),
    queue: buildQueueStatus(jobs, now),
    crons: buildCronStatus(jobs),
    jobs: buildJobStatus(jobs),
  };
}

export async function getProcessorHealthcheck() {
  const now = new Date();
  const jobRepository = getJobRepository();
  const [workers, jobs] = await Promise.all([
    jobRepository.findWorkers(),
    jobRepository.findRunningJobs(),
  ]);

  return {
    now,
    workers: buildWorkerStatus(workers, jobs),
    queue: buildQueueStatus(jobs, now),
  };
}
