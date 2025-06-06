import { ObjectId } from "mongodb";
import {
  IJob,
  IJobsCron,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
} from "../../common/model.ts";

// Database-agnostic filter and options types
export type DatabaseFilter<T> = {
  [P in keyof T]?:
    | T[P]
    | { $in?: T[P][] }
    | { $nin?: T[P][] }
    | { $lt?: T[P] }
    | { $lte?: T[P] }
    | { $gt?: T[P] }
    | { $gte?: T[P] };
};

export type DatabaseOptions<T> = {
  projection?: (keyof T)[];
  sort?: Array<[keyof T, "asc" | "desc"]>;
};

export interface IJobRepository {
  configureDb(): Promise<void>;
  createJobSimple(
    params: Pick<IJobsSimple, "name" | "payload" | "scheduled_for" | "sync">,
  ): Promise<IJobsSimple>;
  createJobCron(
    params: Pick<IJobsCron, "name" | "cron_string" | "scheduled_for">,
  ): Promise<IJobsCron>;
  createJobCronTask(
    params: Pick<IJobsCron, "name" | "scheduled_for">,
  ): Promise<IJobsCronTask>;
  getSimpleJob(id: string | ObjectId): Promise<IJobsSimple | null>;
  getCronTaskJob(id: string | ObjectId): Promise<IJobsCronTask | null>;
  updateJob(_id: string | ObjectId, data: Partial<IJob>): Promise<void>;

  findDueCronJobs(scheduleForDate: Date): Promise<IJobsCron[]>;
  findScheduledCronJobs(): Promise<IJob[]>;
  findRunningJobs(): Promise<IJob[]>;

  // Cron support
  deleteCronsNotIn(names: string[]): Promise<number>;
  upsertCronJob(
    cron: { name: string; cron_string: string },
    now: Date,
  ): Promise<IJobsCron | null>;
  updateCronSchedule(id: string | ObjectId, now: Date): Promise<void>;
  deletePendingCronTasks(name: string): Promise<number>;
  updateCronScheduledFor(
    id: string | ObjectId,
    oldScheduledFor: Date,
    newScheduledFor: Date,
  ): Promise<boolean>;

  // Worker support
  findWorkers(): Promise<IWorker[]>;
  upsertWorker(worker: IWorker): Promise<void>;
  updateWorkerHeartbeat(id: string | ObjectId, date: Date): Promise<boolean>;
  removeWorker(id: string | ObjectId): Promise<void>;

  // Job detection and picking
  detectExitedJobs(): Promise<IJobsCronTask | IJobsSimple | null>;
  pickNextJob(): Promise<IJobsCronTask | IJobsSimple | null>;
}
