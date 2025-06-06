import {
  Db,
  ObjectId,
  Collection,
  MongoServerError,
  Filter,
  MatchKeysAndValues,
} from "mongodb";
import { zodToMongoSchema } from "zod-mongodb-schema";
import { getOptions } from "../setup.ts";
import {
  IJob,
  IJobsCron,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
  ZJob,
  JobType,
  SimpleJobStatus,
  CronJobStatus,
} from "../../common/model.ts";
import { IJobRepository } from "./IJobRepository.ts";

export class MongoJobRepository implements IJobRepository {
  constructor(
    private db: Db,
    private workerId: ObjectId,
  ) {}

  static JOB_COLLECTION_NAME = "job_processor.jobs";
  static WORKER_COLLECTION_NAME = "job_processor.workers";

  private getJobCollection(): Collection<IJob> {
    return this.db.collection(MongoJobRepository.JOB_COLLECTION_NAME);
  }
  private getWorkerCollection() {
    return this.db.collection<IWorker>(
      MongoJobRepository.WORKER_COLLECTION_NAME,
    );
  }

  async configureDb() {
    await this.executeMigrations();
    await this.configureDbSchemaValidation();
    await this.createIndexes();
  }

  private async executeMigrations() {
    await this.getJobCollection().updateMany(
      {
        type: { $in: [JobType.CronTask, JobType.Simple] },
        worker_id: { $exists: false },
      },
      { $set: { worker_id: null } },
      { bypassDocumentValidation: true },
    );
  }

  private async createIndexes() {
    await Promise.allSettled([
      this.getJobCollection().createIndexes(
        [
          { key: { type: 1, scheduled_for: 1 } },
          { key: { type: 1, status: 1, scheduled_for: 1 } },
          { key: { type: 1, name: 1, status: 1, scheduled_for: 1 } },
          { key: { type: 1, name: 1 } },
          { key: { status: 1 } },
          {
            key: { ended_at: 1 },
            expireAfterSeconds: 3600 * 24 * 90,
          },
        ],
        { background: true },
      ),
      this.getWorkerCollection().createIndexes(
        [{ key: { lastSeen: 1 }, expireAfterSeconds: 300 }],
        { background: true },
      ),
    ]);
  }

  private async createCollectionIfDoesNotExist(jobCollectionName: string) {
    const collectionsInDb = await this.db.listCollections().toArray();
    const collectionExistsInDb = collectionsInDb
      .map(({ name }) => name)
      .includes(jobCollectionName);

    if (!collectionExistsInDb) {
      try {
        await this.db.createCollection(jobCollectionName);
      } catch (err) {
        if ((err as MongoServerError).codeName !== "NamespaceExists") {
          throw err;
        }
      }
    }
  }

  private async configureDbSchemaValidation() {
    await this.createCollectionIfDoesNotExist(
      MongoJobRepository.JOB_COLLECTION_NAME,
    );

    const convertedSchema = zodToMongoSchema(ZJob);

    await this.db.command({
      collMod: MongoJobRepository.JOB_COLLECTION_NAME,
      validationLevel: "strict",
      validationAction: "error",
      validator: {
        $jsonSchema: {
          title: `${MongoJobRepository.JOB_COLLECTION_NAME} validation schema`,
          ...convertedSchema,
        },
      },
    });
  }

  async createJobSimple({
    name,
    payload,
    scheduled_for,
    sync,
  }: Pick<
    IJobsSimple,
    "name" | "payload" | "scheduled_for" | "sync"
  >): Promise<IJobsSimple> {
    const now = new Date();
    const job: IJobsSimple = {
      _id: new ObjectId(),
      name,
      type: JobType.Simple,
      status: sync ? SimpleJobStatus.Running : SimpleJobStatus.Pending,
      payload,
      updated_at: now,
      created_at: now,
      scheduled_for,
      sync,
      worker_id: null,
    };
    await this.getJobCollection().insertOne(job);
    return job;
  }

  async createJobCron({
    name,
    cron_string,
    scheduled_for,
  }: Pick<
    IJobsCron,
    "name" | "cron_string" | "scheduled_for"
  >): Promise<IJobsCron> {
    const now = new Date();
    const job: IJobsCron = {
      _id: new ObjectId(),
      name,
      type: JobType.Cron,
      status: CronJobStatus.Active,
      cron_string,
      updated_at: now,
      created_at: now,
      scheduled_for,
    };
    await this.getJobCollection().insertOne(job);
    return job;
  }

  async createJobCronTask({
    name,
    scheduled_for,
  }: Pick<IJobsCron, "name" | "scheduled_for">): Promise<IJobsCronTask> {
    const now = new Date();
    const job: IJobsCronTask = {
      _id: new ObjectId(),
      name,
      type: JobType.CronTask,
      status: SimpleJobStatus.Pending,
      updated_at: now,
      created_at: now,
      started_at: null,
      ended_at: null,
      scheduled_for,
      worker_id: null,
    };
    await this.getJobCollection().insertOne(job);
    return job;
  }

  async getSimpleJob(id: ObjectId): Promise<IJobsSimple | null> {
    return await this.getJobCollection().findOne<IJobsSimple>({
      _id: id,
      type: JobType.Simple,
    });
  }

  async getCronTaskJob(id: ObjectId): Promise<IJobsCronTask | null> {
    return await this.getJobCollection().findOne<IJobsCronTask>({
      _id: id,
      type: JobType.CronTask,
    });
  }

  async findDueCronJobs(scheduleForDate: Date): Promise<IJobsCron[]> {
    const result = this.getJobCollection()
      .find(
        {
          type: JobType.Cron,
          scheduled_for: { $lte: scheduleForDate },
        },
        { sort: { scheduled_for: 1 } },
      )
      .toArray();
    return result as Promise<IJobsCron[]>;
  }

  async updateJob(_id: ObjectId, data: MatchKeysAndValues<IJob>) {
    await this.getJobCollection().updateOne(
      { _id },
      { $set: { ...data, updated_at: new Date() } },
    );
  }

  async findScheduledCronJobs(): Promise<IJob[]> {
    const result = this.getJobCollection()
      .find({}, { sort: { scheduled_for: -1 } })
      .toArray();

    return result;
  }

  async findRunningJobs(): Promise<IJob[]> {
    const result = this.getJobCollection()
      .find(
        {
          status: { $nin: [SimpleJobStatus.Finished, SimpleJobStatus.Errored] },
        },
        { sort: { scheduled_for: -1 } },
      )
      .toArray();
    return result as Promise<IJob[]>;
  }

  private getWorkerScopeJob(): Filter<IJob> {
    const options = getOptions();
    const workerTags = options.workerTags ?? null;

    if (workerTags === null) {
      return {
        type: { $in: [JobType.Simple, JobType.CronTask] },
      };
    }

    const jobNames = Object.entries(options.jobs)
      .filter(([, def]) => {
        const tag = def.tag ?? null;
        if (tag === null) {
          return true;
        }
        return workerTags.includes(tag);
      })
      .map(([name]) => name);

    const taskNames = Object.entries(options.crons)
      .filter(([, def]) => {
        const tag = def.tag ?? null;
        if (tag === null) {
          return true;
        }
        return workerTags.includes(tag);
      })
      .map(([name]) => name);

    return {
      $or: [
        { type: JobType.Simple, name: { $in: jobNames } },
        { type: JobType.CronTask, name: { $in: taskNames } },
      ],
    };
  }

  async pickNextJob(): Promise<IJobsCronTask | IJobsSimple | null> {
    return this.getJobCollection().findOneAndUpdate(
      {
        $and: [
          this.getWorkerScopeJob(),
          {
            status: { $in: [SimpleJobStatus.Paused, SimpleJobStatus.Pending] },
            scheduled_for: { $lte: new Date() },
          },
        ],
      },
      [
        {
          $set: {
            status: "running",
            worker_id: this.workerId,
            started_at: { $ifNull: ["$started_at", new Date()] },
          },
        },
      ],
      { sort: { scheduled_for: 1 }, includeResultMetadata: false },
    ) as Promise<IJobsCronTask | IJobsSimple | null>;
  }

  async detectExitedJobs(): Promise<IJobsCronTask | IJobsSimple | null> {
    const now = new Date();
    // Any job started more than 5min ago is garanted to be in active worker if not dead
    // We need to be careful in case a worker register just after we get active workers
    return this.getJobCollection().findOneAndUpdate(
      {
        type: { $in: [JobType.Simple, JobType.CronTask] },
        status: SimpleJobStatus.Running,
        started_at: { $lt: new Date(now.getTime() - 5 * 60 * 1000) },
      },
      {
        $set: {
          status: SimpleJobStatus.Errored,
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectedly",
          },
          ended_at: now,
        },
      },
      {
        returnDocument: "after",
        sort: { started_at: 1 },
      },
    ) as Promise<IJobsCronTask | IJobsSimple | null>;
  }

  // Delete all cron jobs not in the provided names list
  async deleteCronsNotIn(names: string[]): Promise<number> {
    const result = await this.getJobCollection().deleteMany({
      name: { $nin: names },
      type: JobType.Cron,
    });
    return result.deletedCount ?? 0;
  }

  // Upsert a cron job, creating it if it does not exist or updating it if it does
  async upsertCronJob(
    cron: { name: string; cron_string: string },
    now: Date,
  ): Promise<IJobsCron | null> {
    const result = await this.getJobCollection().findOneAndUpdate(
      { name: cron.name, type: JobType.Cron },
      {
        $set: { cron_string: cron.cron_string, updated_at: now },
        $setOnInsert: {
          _id: new ObjectId(),
          name: cron.name,
          type: JobType.Cron,
          status: CronJobStatus.Active,
          created_at: now,
          scheduled_for: now,
        },
      },
      {
        returnDocument: "before",
        upsert: true,
        includeResultMetadata: false,
      },
    );

    return result as IJobsCron | null;
  }

  // Update scheduled_for and updated_at for a cron job
  async updateCronSchedule(_id: ObjectId | string, now: Date): Promise<void> {
    await this.getJobCollection().updateOne(
      { _id: typeof _id === "string" ? new ObjectId(_id) : _id },
      { $set: { scheduled_for: now, updated_at: now } },
    );
  }

  // Delete all pending cron tasks for a given cron name
  async deletePendingCronTasks(name: string): Promise<number> {
    const result = await this.getJobCollection().deleteMany({
      name,
      status: SimpleJobStatus.Pending,
      type: JobType.CronTask,
    });
    return result.deletedCount ?? 0;
  }

  // Update scheduled_for for a cron job if it matches the old scheduled_for
  async updateCronScheduledFor(
    _id: ObjectId | string,
    oldScheduledFor: Date,
    newScheduledFor: Date,
  ): Promise<boolean> {
    const result = await this.getJobCollection().updateOne(
      {
        _id: typeof _id === "string" ? new ObjectId(_id) : _id,
        scheduled_for: oldScheduledFor,
      },
      { $set: { scheduled_for: newScheduledFor, updated_at: new Date() } },
    );
    return result.modifiedCount > 0;
  }

  async findWorkers(): Promise<IWorker[]> {
    return this.getWorkerCollection().find({}).toArray();
  }

  async upsertWorker(worker: IWorker): Promise<void> {
    await this.getWorkerCollection().updateOne(
      {
        _id:
          typeof worker._id === "string"
            ? new ObjectId(worker._id)
            : worker._id,
      },
      {
        $set: {
          hostname: worker.hostname,
          lastSeen: worker.lastSeen,
          tags: worker.tags ?? null,
        },
      },
      { upsert: true },
    );
  }

  async updateWorkerHeartbeat(
    id: string | ObjectId,
    date: Date,
  ): Promise<boolean> {
    const res = await this.getWorkerCollection().updateOne(
      { _id: typeof id === "string" ? new ObjectId(id) : id },
      { $set: { lastSeen: date } },
    );
    return res.matchedCount > 0;
  }

  async removeWorker(id: string | ObjectId): Promise<void> {
    await this.getWorkerCollection().deleteOne({
      _id: typeof id === "string" ? new ObjectId(id) : id,
    });
  }
}
