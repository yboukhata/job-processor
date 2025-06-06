import { Jsonify } from "type-fest";
import { z } from "zod";
import { zObjectId } from "zod-mongodb-schema";

export enum JobType {
  Simple = "simple",
  Cron = "cron",
  CronTask = "cron_task",
}

export enum SimpleJobStatus {
  Pending = "pending",
  Running = "running",
  Finished = "finished",
  Errored = "errored",
  Paused = "paused",
}

export enum CronJobStatus {
  Active = "active",
}

export const zDbId = z.union([zObjectId, z.string()]);

const zCronName = z.string().describe("Le nom de la tâche");

// Base schema for common fields across all job types
const ZJobBase = z.object({
  _id: zDbId,
  name: z.string().describe("Le nom de la tâche"),
  updated_at: z.date().describe("Date de mise à jour en base de données"),
  created_at: z.date().describe("Date d'ajout en base de données"),
  scheduled_for: z.date().describe("Date de lancement programmée"),
});

// Schema for jobs that can be executed (Simple and CronTask)
const ZExecutableJob = ZJobBase.extend({
  status: z.nativeEnum(SimpleJobStatus).describe("Statut courant du job"),
  started_at: z.date().nullish().describe("Date de lancement"),
  ended_at: z.date().nullish().describe("Date de fin d'execution"),
  worker_id: zDbId
    .nullable()
    .describe("Worker ID handling the job when running"),
  output: z
    .object({
      duration: z.string(),
      result: z.unknown(),
      error: z.string().nullable(),
    })
    .nullish()
    .describe("Les valeurs de retours du job"),
});

// Now use the base schemas in your specific job types
export const ZJobSimple = ZExecutableJob.extend({
  type: z.literal(JobType.Simple),
  sync: z.boolean().describe("Si le job est synchrone"),
  payload: z
    .record(z.unknown())
    .nullish()
    .describe("La donnée liéé à la tâche"),
});

export const ZJobCron = ZJobBase.extend({
  type: z.literal(JobType.Cron),
  status: z.nativeEnum(CronJobStatus).describe("Statut courant du cron"),
  cron_string: z
    .string()
    .describe("standard cron string exemple: '*/2 * * * *'"),
});

export const ZJobCronTask = ZExecutableJob.extend({
  type: z.literal(JobType.CronTask),
  sentry_id: z
    .string()
    .nullish()
    .describe("Id sentry pour le tracking d'exécution des jobs"),
});

export const ZJob = z.discriminatedUnion("type", [
  ZJobSimple,
  ZJobCron,
  ZJobCronTask,
]);

export const ZWorker = z.object({
  _id: zDbId,
  hostname: z.string().describe("Hostname du worker"),
  lastSeen: z.date().describe("Date du dernier heartbeat reçu"),
  tags: z
    .string()
    .array()
    .nullable()
    .describe("Liste des tags du worker à executer"),
});

export function isJobSimple(job: IJob): job is IJobsSimple {
  return job.type === "simple";
}

export function isJobCron(job: IJob): job is IJobsCron {
  return job.type === "cron";
}

export function isJobCronTask(job: IJob): job is IJobsCronTask {
  return job.type === "cron_task";
}

export function isJobSimpleOrCronTask(
  job: IJob,
): job is IJobsSimple | IJobsCronTask {
  return isJobSimple(job) || isJobCronTask(job);
}

export type CronName = z.output<typeof zCronName>;

export type IJob = z.output<typeof ZJob>;
export type IJobsSimple = z.output<typeof ZJobSimple>;
export type IJobsCron = z.output<typeof ZJobCron>;
export type IJobsCronTask = z.output<typeof ZJobCronTask>;

export type IWorker = z.output<typeof ZWorker>;

const zWorkerStatus = z.object({
  worker: ZWorker,
  task: z.union([ZJobSimple, ZJobCronTask, z.null()]),
});

const zCronStatus = z.object({
  cron: ZJobCron,
  scheduled: z.array(ZJobCronTask),
  running: z.array(ZJobCronTask),
  history: z.array(ZJobCronTask),
});

const zJobStatus = z.object({
  name: z.string(),
  tasks: z.array(ZJobSimple),
});

export const zProcessorStatus = z.object({
  now: z.date(),
  workers: z.array(zWorkerStatus),
  queue: z.array(z.union([ZJobSimple, ZJobCronTask])),
  jobs: z.array(zJobStatus),
  crons: z.array(zCronStatus),
});

export type WorkerStatus = z.output<typeof zWorkerStatus>;

export type CronStatus = z.output<typeof zCronStatus>;

export type JobStatus = z.output<typeof zJobStatus>;

export type ProcessorStatus = z.output<typeof zProcessorStatus>;

export type ProcessorStatusJson = Jsonify<ProcessorStatus>;
