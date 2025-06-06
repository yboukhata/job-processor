import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initJobProcessor, SupportedDbType } from "../setup.ts";
import { IJobsCron, IJobsCronTask } from "../index.ts";
import {
  cronSchedulerEvent,
  cronsInit,
  runCronsScheduler,
  startCronScheduler,
} from "./crons.ts";
import { MongoJobRepository } from "../data/MongoJobRepository.ts";
import { CronJobStatus, JobType, SimpleJobStatus } from "../../common/model.ts";

let client: MongoClient;

const now = new Date("2023-11-17T11:00:00.000Z");

beforeAll(async () => {
  client = new MongoClient(
    `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}_${process.env["VITEST_WORKER_ID"]}`,
  );
  await client.connect();

  return async () => {
    await client?.close();
  };
});

beforeEach(async () => {
  await client.db().collection("job_processor.jobs").deleteMany({});

  vi.useFakeTimers();
  vi.setSystemTime(now);

  return () => {
    vi.useRealTimers();
  };
});

function createJobCrons(
  data: Pick<IJobsCron, "name"> &
    Partial<Pick<IJobsCron, "cron_string" | "scheduled_for">>,
): Omit<IJobsCron, "_id"> & { _id: ObjectId } {
  return {
    _id: new ObjectId(),
    type: JobType.Cron,
    cron_string: "* * * * *",
    status: CronJobStatus.Active,
    updated_at: now,
    created_at: now,
    scheduled_for: now,
    ...data,
  };
}

function createCronTaskJob(
  data: Pick<IJobsCronTask, "name" | "status"> &
    Partial<Pick<IJobsCronTask, "worker_id" | "started_at">>,
): Omit<IJobsCronTask, "_id"> & { _id: ObjectId } {
  return {
    _id: new ObjectId(),
    type: JobType.CronTask,
    started_at: null,
    ended_at: null,
    updated_at: now,
    created_at: now,
    worker_id: null,
    scheduled_for: now,
    ...data,
  };
}

describe("cronsInit", () => {
  const crons = [
    createJobCrons({ name: "deletedOne" }),
    createJobCrons({
      name: "updatedOne",
      cron_string: "0 * * * *",
      scheduled_for: new Date(now.getTime() + 3_600_000),
    }),
    createJobCrons({
      name: "keptOne",
      cron_string: "0 */5 * * *",
      scheduled_for: now,
    }),
  ];
  const tasks = [
    createCronTaskJob({ name: "deletedOne", status: SimpleJobStatus.Running }), // We should keep it
    createCronTaskJob({ name: "deletedOne", status: SimpleJobStatus.Pending }), // Should be removed
    createCronTaskJob({ name: "updatedOne", status: SimpleJobStatus.Running }), // We should keep it
    createCronTaskJob({ name: "updatedOne", status: SimpleJobStatus.Pending }), // Should be removed
    createCronTaskJob({ name: "keptOne", status: SimpleJobStatus.Pending }),
  ];

  const expectedCrons = [
    { ...crons[1], cron_string: "0 */2 * * *", scheduled_for: now },
    crons[2],
    {
      _id: expect.any(ObjectId),
      type: JobType.Cron,
      name: "newOne",
      cron_string: "0 * * * *",
      status: CronJobStatus.Active,
      updated_at: now,
      created_at: now,
      scheduled_for: now,
    },
  ];
  const expectedTasks = [tasks[0], tasks[2], tasks[4]];

  beforeEach(async () => {
    const options = {
      logger: {
        debug: vi.fn() as any,
        info: vi.fn() as any,
        error: vi.fn() as any,
        child: vi.fn() as any,
      },
      databaseType: SupportedDbType.Mongo,
      db: client.db(),
      jobs: {},
      crons: {
        newOne: {
          cron_string: "0 * * * *",
          handler: vi.fn(),
        },
        updatedOne: {
          cron_string: "0 */2 * * *",
          handler: vi.fn(),
        },
        keptOne: {
          cron_string: "0 */5 * * *",
          handler: vi.fn(),
        },
      },
    };

    await initJobProcessor(options);
    await client
      ?.db()
      .collection(MongoJobRepository.JOB_COLLECTION_NAME)
      .insertMany([...crons, ...tasks]);
  });

  it("should update crons", async () => {
    await cronsInit();

    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.Cron })
        .toArray(),
    ).toEqual(expectedCrons);
    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.CronTask })
        .toArray(),
    ).toEqual(expectedTasks);
  });

  it.skip("should support concurrency", async () => {
    await Promise.all([
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
    ]);

    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.Cron })
        .toArray(),
    ).toEqual(expectedCrons);
    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.CronTask })
        .toArray(),
    ).toEqual(expectedTasks);
  });

  it("should support concurrency", async () => {
    // Simulate concurrency where one of the cron alreay scheduled a task
    const newTask = createCronTaskJob({
      name: "newOne",
      status: SimpleJobStatus.Pending,
    });
    await client
      ?.db()
      .collection(MongoJobRepository.JOB_COLLECTION_NAME)
      .insertOne(newTask);

    await Promise.all([
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
      cronsInit(),
    ]);

    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.Cron })
        .toArray(),
    ).toEqual(expectedCrons);
    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({ type: JobType.CronTask })
        .toArray(),
    ).toEqual([...expectedTasks, newTask]);
  });
});

describe("runCronsScheduler", () => {
  beforeEach(async () => {
    const options = {
      logger: {
        debug: vi.fn() as any,
        info: vi.fn() as any,
        error: vi.fn() as any,
        child: vi.fn() as any,
      },
      db: client.db(),
      databaseType: SupportedDbType.Mongo,
      jobs: {},
      crons: {
        "Daily at 9am Paris time": {
          cron_string: "0 9 * * *",
          handler: vi.fn(),
        },
      },
    };

    await initJobProcessor(options);
  });

  it("should run schedule crons properly", async () => {
    const createdAt = new Date("2024-02-21T10:30:00.000Z");
    const updatedAt = new Date("2024-02-21T10:40:00.000Z");
    const nextScheduledFor1 = new Date("2024-02-22T08:00:00.000Z");
    const nextCronScheduler = new Date("2024-02-22T08:00:10.000Z");
    const nextScheduledFor2 = new Date("2024-02-23T08:00:00.000Z");

    vi.setSystemTime(createdAt);

    await client
      ?.db()
      .collection(MongoJobRepository.JOB_COLLECTION_NAME)
      .insertOne({
        _id: new ObjectId(),
        type: JobType.Cron,
        cron_string: "0 9 * * *",
        status: CronJobStatus.Active,
        updated_at: createdAt,
        created_at: createdAt,
        scheduled_for: createdAt,
        name: "Daily at 9am Paris time",
      });

    vi.setSystemTime(updatedAt);

    await runCronsScheduler();

    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find()
        .toArray(),
    ).toEqual([
      {
        _id: expect.any(ObjectId),
        created_at: createdAt,
        cron_string: "0 9 * * *",
        name: "Daily at 9am Paris time",
        scheduled_for: nextScheduledFor1,
        status: CronJobStatus.Active,
        type: JobType.Cron,
        updated_at: updatedAt,
      },
      {
        _id: expect.any(ObjectId),
        created_at: updatedAt,
        ended_at: null,
        name: "Daily at 9am Paris time",
        scheduled_for: nextScheduledFor1,
        started_at: null,
        status: SimpleJobStatus.Pending,
        type: JobType.CronTask,
        updated_at: updatedAt,
        worker_id: null,
      },
    ]);

    vi.setSystemTime(nextCronScheduler);

    await runCronsScheduler();

    expect(
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find()
        .toArray(),
    ).toEqual([
      {
        _id: expect.any(ObjectId),
        created_at: createdAt,
        cron_string: "0 9 * * *",
        name: "Daily at 9am Paris time",
        scheduled_for: nextScheduledFor2,
        status: CronJobStatus.Active,
        type: JobType.Cron,
        updated_at: nextCronScheduler,
      },
      {
        _id: expect.any(ObjectId),
        created_at: updatedAt,
        ended_at: null,
        name: "Daily at 9am Paris time",
        scheduled_for: nextScheduledFor1,
        started_at: null,
        status: SimpleJobStatus.Pending,
        type: JobType.CronTask,
        updated_at: updatedAt,
        worker_id: null,
      },
      {
        _id: expect.any(ObjectId),
        created_at: nextCronScheduler,
        ended_at: null,
        name: "Daily at 9am Paris time",
        scheduled_for: nextScheduledFor2,
        started_at: null,
        status: SimpleJobStatus.Pending,
        type: JobType.CronTask,
        updated_at: nextCronScheduler,
        worker_id: null,
      },
    ]);
  });
});

describe("startCronScheduler", () => {
  describe("without crons", () => {
    it("should not start interval when no crons are setup", async () => {
      const options = {
        logger: {
          debug: vi.fn() as any,
          info: vi.fn() as any,
          error: vi.fn() as any,
          child: vi.fn() as any,
        },
        db: client.db(),
        databaseType: SupportedDbType.Mongo,
        jobs: {},
        crons: {},
      };

      await initJobProcessor(options);

      const abortController = new AbortController();
      await startCronScheduler(abortController.signal);

      expect(vi.getTimerCount()).toBe(0);
      abortController.abort();
    });
  });

  describe("when crons", () => {
    const initialCrons = [
      createJobCrons({
        name: "every2Min",
        cron_string: "*/2 * * * *",
        scheduled_for: now,
      }),
      createJobCrons({
        name: "every3Min",
        cron_string: "*/3 * * * *",
        scheduled_for: now,
      }),
    ];

    const in1Min = new Date(now.getTime() + 60_000);
    const in2Min = new Date(now.getTime() + 60_000 * 2);
    const in3Min = new Date(now.getTime() + 60_000 * 3);
    const in4Min = new Date(now.getTime() + 60_000 * 4);
    const in6Min = new Date(now.getTime() + 60_000 * 6);

    const createCronTaskJobExpect = (data: {
      created_at: Date;
      scheduled_for: Date;
      name: string;
    }) => {
      return {
        _id: expect.any(ObjectId),
        type: JobType.CronTask,
        started_at: null,
        ended_at: null,
        status: SimpleJobStatus.Pending,
        updated_at: data.created_at,
        created_at: data.created_at,
        worker_id: null,
        scheduled_for: data.scheduled_for,
        name: data.name,
      };
    };

    const expectedCronsNow = [
      { ...initialCrons[0], scheduled_for: in2Min },
      { ...initialCrons[1], scheduled_for: in3Min },
    ];

    const expectedTasksNow = [
      createCronTaskJobExpect({
        created_at: now,
        scheduled_for: in2Min,
        name: "every2Min",
      }),
      createCronTaskJobExpect({
        created_at: now,
        scheduled_for: in3Min,
        name: "every3Min",
      }),
    ];

    const expectedCronsIn2Min = [
      { ...initialCrons[1], scheduled_for: in3Min },
      { ...initialCrons[0], scheduled_for: in4Min, updated_at: in2Min },
    ];

    const expectedTasksIn2Min = [
      ...expectedTasksNow,
      createCronTaskJobExpect({
        created_at: in2Min,
        scheduled_for: in4Min,
        name: "every2Min",
      }),
    ];

    const expectedCronsIn3Min = [
      { ...initialCrons[0], scheduled_for: in4Min, updated_at: in2Min },
      { ...initialCrons[1], scheduled_for: in6Min, updated_at: in3Min },
    ];

    const expectedTasksIn3Min = [
      ...expectedTasksIn2Min,
      createCronTaskJobExpect({
        created_at: in3Min,
        scheduled_for: in6Min,
        name: "every3Min",
      }),
    ];

    beforeEach(async () => {
      const options = {
        logger: {
          debug: vi.fn() as any,
          info: vi.fn() as any,
          error: vi.fn() as any,
          child: vi.fn() as any,
        },
        db: client.db(),
        databaseType: SupportedDbType.Mongo,
        jobs: {},
        crons: {
          every2Min: {
            cron_string: "*/2 * * * *",
            handler: vi.fn(),
          },
          every3Min: {
            cron_string: "*/3 * * * *",
            handler: vi.fn(),
          },
        },
      };

      await initJobProcessor(options);
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany(initialCrons);
    });

    it("should create cron tasks", async () => {
      const abortController = new AbortController();
      await startCronScheduler(abortController.signal);

      expect(vi.getTimerCount()).toBe(1);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsNow);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksNow);

      // At now + 1min
      let onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersToNextTimerAsync();
      await onUpdate;
      expect(new Date()).toEqual(in1Min);
      expect(vi.getTimerCount()).toBe(1);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsNow);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksNow);

      // At now + 2min
      onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersToNextTimerAsync();
      await onUpdate;
      expect(new Date()).toEqual(in2Min);
      expect(vi.getTimerCount()).toBe(1);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsIn2Min);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksIn2Min);

      // At now + 3min
      onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersToNextTimerAsync();
      await onUpdate;
      expect(new Date()).toEqual(in3Min);
      expect(vi.getTimerCount()).toBe(1);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsIn3Min);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksIn3Min);

      abortController.abort();
    });

    it("should support concurrency", async () => {
      const abortController = new AbortController();
      await Promise.all([
        startCronScheduler(abortController.signal),
        startCronScheduler(abortController.signal),
        startCronScheduler(abortController.signal),
        startCronScheduler(abortController.signal),
        startCronScheduler(abortController.signal),
      ]);

      expect(vi.getTimerCount()).toBe(5);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsNow);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksNow);

      // At now + 1min
      let onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersByTimeAsync(60_000);
      await onUpdate;

      expect(new Date()).toEqual(in1Min);
      expect(vi.getTimerCount()).toBe(5);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsNow);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksNow);

      // At now + 2min
      onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersByTimeAsync(60_000);
      await onUpdate;

      expect(new Date()).toEqual(in2Min);
      expect(vi.getTimerCount()).toBe(5);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsIn2Min);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksIn2Min);

      // At now + 3min
      onUpdate = new Promise((resolve) =>
        cronSchedulerEvent.once("updated", resolve),
      );
      await vi.advanceTimersByTimeAsync(60_000);
      await onUpdate;

      expect(new Date()).toEqual(in3Min);
      expect(vi.getTimerCount()).toBe(5);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.Cron }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedCronsIn3Min);
      expect(
        await client
          ?.db()
          .collection(MongoJobRepository.JOB_COLLECTION_NAME)
          .find({ type: JobType.CronTask }, { sort: { scheduled_for: 1 } })
          .toArray(),
      ).toEqual(expectedTasksIn3Min);

      abortController.abort();
    });
  });
});
