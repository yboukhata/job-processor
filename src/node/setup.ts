import { Db as MongoDb } from "mongodb";
import { Client as PgClient } from "pg";
import { IJobRepository } from "./data/IJobRepository.ts";
import { MongoJobRepository } from "./data/MongoJobRepository.ts";
import { PostgresJobRepository } from "./data/PostgresJobRepository.ts";
import { IJobsCronTask, IJobsSimple } from "../common/model.ts";
import { workerId } from "./worker/heartbeat.ts";

export interface ILogger {
  debug(msg: string): unknown;
  info(data: Record<string, unknown>, msg: string): unknown;
  info(msg: string): unknown;
  child(data: Record<string, unknown>): ILogger;
  error(data: Record<string, unknown>, msg: string): unknown;
}

export type JobDef<T extends string = string> = {
  handler: (job: IJobsSimple, signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsSimple) => Promise<unknown>;
  resumable?: boolean;
  tag?: T | null;
};

export type CronDef<T extends string = string> = {
  cron_string: string;
  handler: (signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsCronTask) => Promise<unknown>;
  resumable?: boolean;
  maxRuntimeInMinutes?: number;
  checkinMargin?: number;
  tag?: T | null;
};

export enum SupportedDbType {
  Mongo = "mongo",
  Postgres = "postgres",
}

export type SupportedDbClient = MongoDb | PgClient;

export type JobProcessorOptions<T extends string = string> = {
  databaseType: SupportedDbType;
  db: SupportedDbClient;
  logger: ILogger;
  jobs: Record<string, JobDef>;
  crons: Record<string, CronDef>;
  workerTags?: T[] | null;
};

let options: JobProcessorOptions | null = null;
let jobRepository: IJobRepository | null = null;

export function getOptions(): JobProcessorOptions {
  if (!options) throw new Error("Job processor is not setup");
  return options;
}

export function getLogger(): ILogger {
  return getOptions().logger;
}

export function getJobRepository(): IJobRepository {
  if (!jobRepository) {
    throw new Error(
      "Database adapter is not initialized, please call initJobProcessor first",
    );
  }
  return jobRepository;
}

export async function initJobProcessor(opts: JobProcessorOptions) {
  if (opts.workerTags != null && opts.workerTags.length === 0) {
    throw new Error("workerTags should not be empty");
  }

  options = opts;

  // Instantiate the correct adapter based on databaseType
  if (opts.databaseType === SupportedDbType.Mongo) {
    jobRepository = new MongoJobRepository(opts.db as MongoDb, workerId);
  } else if (opts.databaseType === SupportedDbType.Postgres) {
    jobRepository = new PostgresJobRepository(opts.db as PgClient, workerId);
  } else {
    throw new Error("Unsupported databaseType");
  }

  await jobRepository.configureDb();
}
