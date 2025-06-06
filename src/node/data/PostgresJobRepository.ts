// PostgresJobRepository.ts
import { ObjectId } from "mongodb";
import { IJobRepository } from "./IJobRepository.ts";
import {
  IJob,
  IJobsCron,
  IJobsCronTask,
  IJobsSimple,
  JobType,
  CronJobStatus,
  SimpleJobStatus,
  IWorker,
} from "../../common/model.ts";
import { getOptions } from "../setup.ts";
import { Client } from "pg";
import { v4 as uuidv4 } from "uuid";

interface PgJobRow {
  id: string;
  name: string;
  type: JobType;
  status: SimpleJobStatus | CronJobStatus;
  payload?: unknown;
  cron_string?: string;
  updated_at: Date;
  created_at: Date;
  scheduled_for: Date;
  sync?: boolean;
  worker_id?: string | null;
  started_at?: Date | null;
  ended_at?: Date | null;
  output?: {
    duration: string;
    result: unknown;
    error: string | null;
  } | null;
}

type JobUpdateFields = Partial<Omit<IJob, "_id" | "type" | "created_at">>;

export class PostgresJobRepository implements IJobRepository {
  constructor(
    private client: Client,
    private workerId: ObjectId,
  ) {}

  async configureDb() {
    // Enable pgcrypto extension for gen_random_uuid()
    await this.client.query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`);

    await this.client.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        status TEXT NOT NULL,
        payload JSONB,
        cron_string TEXT,
        updated_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        scheduled_for TIMESTAMPTZ NOT NULL,
        sync BOOLEAN,
        worker_id TEXT,
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        output JSONB
      );
    `);

    await this.client.query(`
      CREATE TABLE IF NOT EXISTS workers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        lastSeen TIMESTAMPTZ NOT NULL
      );
    `);

    await this.client.query(`
      UPDATE jobs
      SET worker_id = NULL
      WHERE (type = 'cron_task' OR type = 'simple') AND worker_id IS NULL
    `);

    await this.createIndexes();
  }

  private async createIndexes() {
    const indexes = [
      "CREATE INDEX IF NOT EXISTS idx_jobs_type_scheduled_for ON jobs(type, scheduled_for)",
      "CREATE INDEX IF NOT EXISTS idx_jobs_type_status_scheduled_for ON jobs(type, status, scheduled_for)",
      "CREATE INDEX IF NOT EXISTS idx_jobs_type_name_status_scheduled_for ON jobs(type, name, status, scheduled_for)",
      "CREATE INDEX IF NOT EXISTS idx_jobs_type_name ON jobs(type, name)",
      "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)",
      "CREATE INDEX IF NOT EXISTS idx_jobs_ended_at ON jobs(ended_at)",
    ];

    for (const indexQuery of indexes) {
      await this.client.query(indexQuery);
    }
  }

  async createJobSimple(
    params: Pick<IJobsSimple, "name" | "payload" | "scheduled_for" | "sync">,
  ): Promise<IJobsSimple> {
    const now = new Date();
    const res = await this.client.query<PgJobRow>(
      `INSERT INTO jobs (name, type, status, payload, updated_at, created_at, scheduled_for, sync, worker_id)
       VALUES ($1, 'simple', $2, $3, $4, $4, $5, $6, NULL)
       RETURNING *`,
      [
        params.name,
        params.sync ? SimpleJobStatus.Running : SimpleJobStatus.Pending,
        params.payload ? JSON.stringify(params.payload) : null,
        now,
        params.scheduled_for,
        params.sync,
      ],
    );
    if (!res.rows[0]) {
      throw new Error("Failed to create job");
    }

    return this.pgRowToSimpleJob(res.rows[0]);
  }

  async createJobCron(
    params: Pick<IJobsCron, "name" | "cron_string" | "scheduled_for">,
  ): Promise<IJobsCron> {
    const now = new Date();
    const res = await this.client.query<PgJobRow>(
      `INSERT INTO jobs (name, type, status, cron_string, updated_at, created_at, scheduled_for)
       VALUES ($1, 'cron', 'active', $2, $3, $3, $4)
       RETURNING *`,
      [params.name, params.cron_string, now, params.scheduled_for],
    );
    if (!res.rows[0]) {
      throw new Error("Failed to create cron job");
    }

    return this.pgRowToCronJob(res.rows[0]);
  }

  async createJobCronTask(
    params: Pick<IJobsCronTask, "name" | "scheduled_for">,
  ): Promise<IJobsCronTask> {
    const now = new Date();
    const res = await this.client.query<PgJobRow>(
      `INSERT INTO jobs (name, type, status, updated_at, created_at, started_at, ended_at, scheduled_for, worker_id)
       VALUES ($1, 'cron_task', 'pending', $2, $2, NULL, NULL, $3, NULL)
       RETURNING *`,
      [params.name, now, params.scheduled_for],
    );

    if (!res.rows[0]) {
      throw new Error("Failed to create cron task job");
    }
    return this.pgRowToCronTaskJob(res.rows[0]);
  }

  async getSimpleJob(id: string): Promise<IJobsSimple | null> {
    const res = await this.client.query<PgJobRow>(
      `SELECT * FROM jobs WHERE id=$1 AND type='simple' LIMIT 1`,
      [id],
    );

    if (!res.rows[0]) {
      return null;
    }
    return this.pgRowToSimpleJob(res.rows[0]);
  }

  async getCronTaskJob(id: string): Promise<IJobsCronTask | null> {
    const res = await this.client.query<PgJobRow>(
      `SELECT * FROM jobs WHERE id=$1 AND type='cron_task' LIMIT 1`,
      [id],
    );
    return res.rows[0] ? this.pgRowToCronTaskJob(res.rows[0]) : null;
  }

  async findDueCronJobs(scheduleForDate: Date): Promise<IJobsCron[]> {
    const res = await this.client.query<PgJobRow>(
      `SELECT * FROM jobs WHERE type='cron' AND scheduled_for <= $1`,
      [scheduleForDate],
    );
    return res.rows.map((row) => this.pgRowToCronJob(row));
  }

  async findScheduledCronJobs(): Promise<IJob[]> {
    const res = await this.client.query<PgJobRow>(
      `SELECT * FROM jobs WHERE order by scheduled_for ASC`,
    );
    return res.rows.map((row) => this.pgRowToJob(row));
  }

  async findRunningJobs(): Promise<IJob[]> {
    const res = await this.client.query<PgJobRow>(
      `SELECT * FROM jobs WHERE status IN ($1, $2)`,
      [SimpleJobStatus.Running, SimpleJobStatus.Paused],
    );
    return res.rows.map((row) => this.pgRowToJob(row));
  }

  async updateJob(id: string, data: JobUpdateFields): Promise<void> {
    const fields = [];
    const values: unknown[] = [];
    let idx = 1;

    for (const [key, value] of Object.entries(data)) {
      fields.push(`${key}=$${idx++}`);
      values.push(value);
    }
    values.push(id);

    await this.client.query(
      `UPDATE jobs SET ${fields.join(", ")}, updated_at=NOW() WHERE id=$${idx}`,
      values,
    );
  }

  async detectExitedJobs(): Promise<IJobsCronTask | IJobsSimple | null> {
    const res = await this.client.query(
      `UPDATE jobs
       SET status = $1, ended_at = NOW(), output = jsonb_build_object('duration', '--', 'result', NULL, 'error', 'Worker crashed unexpectedly')
       WHERE id = (
         SELECT id FROM jobs
         WHERE (type = $2 OR type = $3)
           AND status = $4
           AND started_at < NOW() - INTERVAL '5 minutes'
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      [
        SimpleJobStatus.Errored,
        JobType.Simple,
        JobType.CronTask,
        SimpleJobStatus.Running,
      ],
    );
    return res.rows[0]
      ? (this.pgRowToJob(res.rows[0]) as IJobsCronTask | IJobsSimple)
      : null;
  }

  // Delete all cron jobs not in the provided names list
  async deleteCronsNotIn(names: string[]): Promise<number> {
    const res = await this.client.query(
      `DELETE FROM jobs WHERE type = 'cron' AND name <> ALL($1::text[])`,
      [names],
    );
    return res.rowCount ?? 0;
  }

  // Upsert a cron job by name, return the previous job if it existed
  async upsertCronJob(
    cron: { name: string; cron_string: string },
    now: Date,
  ): Promise<IJobsCron | null> {
    // Try update first
    const updateRes = await this.client.query(
      `UPDATE jobs
       SET cron_string = $1, updated_at = $2
       WHERE name = $3 AND type = 'cron'
       RETURNING *`,
      [cron.cron_string, now, cron.name],
    );
    if (updateRes.rows[0]) {
      return this.pgRowToCronJob(updateRes.rows[0]);
    }
    // If not found, insert
    const insertRes = await this.client.query(
      `INSERT INTO jobs (id, name, type, status, cron_string, updated_at, created_at, scheduled_for)
       VALUES ($1, $2, 'cron', 'active', $3, $4, $4, $4)
       RETURNING *`,
      [uuidv4(), cron.name, cron.cron_string, now],
    );
    return insertRes.rows[0] ? this.pgRowToCronJob(insertRes.rows[0]) : null;
  }

  // Update scheduled_for and updated_at for a cron job
  async updateCronSchedule(id: string, now: Date): Promise<void> {
    await this.client.query(
      `UPDATE jobs SET scheduled_for = $1, updated_at = $1 WHERE id = $2 AND type = 'cron'`,
      [now, id],
    );
  }

  // Delete all pending cron tasks for a given cron name
  async deletePendingCronTasks(name: string): Promise<number> {
    const res = await this.client.query(
      `DELETE FROM jobs WHERE name = $1 AND status = 'pending' AND type = 'cron_task'`,
      [name],
    );
    return res.rowCount ?? 0;
  }

  // Update scheduled_for for a cron job if it matches the old scheduled_for
  async updateCronScheduledFor(
    id: string,
    oldScheduledFor: Date,
    newScheduledFor: Date,
  ): Promise<boolean> {
    const res = await this.client.query(
      `UPDATE jobs
       SET scheduled_for = $1, updated_at = NOW()
       WHERE id = $2 AND scheduled_for = $3 AND type = 'cron'
       RETURNING id`,
      [newScheduledFor, id, oldScheduledFor],
    );
    return (res.rowCount ?? 0) > 0;
  }

  async findWorkers(): Promise<IWorker[]> {
    const res = await this.client.query(
      `SELECT id, hostname, "lastSeen", tags FROM workers`,
    );
    return res.rows.map((row) => ({
      _id: row.id,
      hostname: row.hostname,
      lastSeen: row.lastSeen,
      tags: row.tags ?? null,
    }));
  }

  async upsertWorker(worker: IWorker): Promise<void> {
    await this.client.query(
      `INSERT INTO workers (id, hostname, "lastSeen", tags)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (id) DO UPDATE SET
         hostname = EXCLUDED.hostname,
         "lastSeen" = EXCLUDED."lastSeen",
         tags = EXCLUDED.tags`,
      [
        typeof worker._id === "string" ? worker._id : worker._id.toString(),
        worker.hostname,
        worker.lastSeen,
        worker.tags ?? null,
      ],
    );
  }

  async updateWorkerHeartbeat(id: string, date: Date): Promise<boolean> {
    const res = await this.client.query(
      `UPDATE workers SET "lastSeen" = $1 WHERE id = $2`,
      [date, id],
    );
    return (res.rowCount ?? 0) > 0;
  }

  async removeWorker(id: string): Promise<void> {
    await this.client.query(`DELETE FROM workers WHERE id = $1`, [id]);
  }

  private pgRowToSimpleJob(row: PgJobRow): IJobsSimple {
    if (row.type !== "simple") throw new TypeError("Expected simple job type");

    return {
      _id: row.id,
      name: row.name,
      type: row.type,
      status: row.status as SimpleJobStatus,
      payload:
        typeof row.payload === "object" &&
        row.payload !== null &&
        !Array.isArray(row.payload)
          ? (row.payload as Record<string, unknown>)
          : null,
      updated_at: row.updated_at,
      created_at: row.created_at,
      scheduled_for: row.scheduled_for,
      sync: row.sync as boolean,
      worker_id: row.worker_id || null,
    };
  }

  private pgRowToCronJob(row: PgJobRow): IJobsCron {
    if (row.type !== JobType.Cron) {
      throw new TypeError("Expected cron job type");
    }

    if (!row.cron_string) {
      throw new Error("Missing cron_string for cron job");
    }

    return {
      _id: row.id,
      name: row.name,
      type: row.type,
      status: row.status as CronJobStatus,
      cron_string: row.cron_string,
      updated_at: row.updated_at,
      created_at: row.created_at,
      scheduled_for: row.scheduled_for,
    };
  }

  private pgRowToCronTaskJob(row: PgJobRow): IJobsCronTask {
    if (row.type !== "cron_task")
      throw new TypeError("Expected cron_task job type");

    return {
      _id: row.id,
      name: row.name,
      type: row.type,
      status: row.status as SimpleJobStatus,
      updated_at: row.updated_at,
      created_at: row.created_at,
      started_at: row.started_at,
      ended_at: row.ended_at,
      scheduled_for: row.scheduled_for,
      worker_id: row.worker_id || null,
    };
  }

  private pgRowToJob(row: PgJobRow): IJob {
    switch (row.type) {
      case "simple":
        return this.pgRowToSimpleJob(row);
      case "cron":
        return this.pgRowToCronJob(row);
      case "cron_task":
        return this.pgRowToCronTaskJob(row);
      default:
        throw new Error(`Unknown job type: ${row.type}`);
    }
  }

  private getWorkerScopeJob() {
    const options = getOptions();
    const workerTags = options.workerTags ?? null;

    if (workerTags === null) {
      // No tag filtering, allow all simple and cron_task jobs
      return {
        where: `(type = $3 OR type = $5)`,
        params: [JobType.Simple, JobType.CronTask],
        jobNames: [],
        taskNames: [],
      };
    }

    // Filter jobs by tags
    const jobNames = Object.entries(options.jobs)
      .filter(([, def]) => {
        const tag = def.tag ?? null;
        return tag === null || workerTags.includes(tag);
      })
      .map(([name]) => name);

    const taskNames = Object.entries(options.crons)
      .filter(([, def]) => {
        const tag = def.tag ?? null;
        return tag === null || workerTags.includes(tag);
      })
      .map(([name]) => name);

    return {
      where: `
        (
          (type = $3 AND name = ANY($4::text[]))
          OR
          (type = $5 AND name = ANY($6::text[]))
        )
      `,
      params: [JobType.Simple, jobNames, JobType.CronTask, taskNames],
      jobNames,
      taskNames,
    };
  }

  async pickNextJob(): Promise<IJobsCronTask | IJobsSimple | null> {
    const workerScope = this.getWorkerScopeJob();

    // Compose the WHERE clause and parameters
    const whereClause = `
      ${workerScope.where}
      AND status IN ('pending', 'paused')
      AND scheduled_for <= NOW()
    `;

    // Compose the parameter list for the query
    // $1: status, $2: worker_id, $3: JobType.Simple, $4: jobNames, $5: JobType.CronTask, $6: taskNames
    const params = [
      SimpleJobStatus.Running,
      this.workerId.toString(),
      JobType.Simple,
      workerScope.jobNames,
      JobType.CronTask,
      workerScope.taskNames,
    ];

    const res = await this.client.query<PgJobRow>(
      `UPDATE jobs
       SET
         status = $1,
         worker_id = $2,
         started_at = COALESCE(started_at, NOW())
       WHERE id = (
         SELECT id
         FROM jobs
         WHERE ${whereClause}
         ORDER BY scheduled_for ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      params,
    );

    if (!res.rows[0]) {
      return null;
    }

    return this.pgRowToJob(res.rows[0]) as IJobsCronTask | IJobsSimple;
  }
}
