import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { getOptions, initJobProcessor, SupportedDbType } from "../setup.ts";
import { IJobsCronTask, IJobsSimple, IWorker } from "../index.ts";
import { workerId } from "../worker/heartbeat.ts";
import { JobType, SimpleJobStatus } from "../../common/model.ts";
import { MongoJobRepository } from "./MongoJobRepository.ts";

let client: MongoClient | null;
let jobRepository: MongoJobRepository | null;

beforeAll(async () => {
  client = new MongoClient(
    `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}_${process.env["VITEST_WORKER_ID"]}`,
  );
  await client.connect();
  jobRepository = new MongoJobRepository(client.db(), new ObjectId(workerId));
  await initJobProcessor({
    databaseType: SupportedDbType.Mongo,
    db: client.db(),
    logger: {
      debug: vi.fn() as any,
      info: vi.fn() as any,
      error: vi.fn() as any,
      child: vi.fn() as any,
    },
    jobs: {
      anyOne: {
        handler: vi.fn() as any,
        tag: null,
      },
      onlyA: {
        handler: vi.fn() as any,
        tag: "A",
      },
      sameNameTagDiff: {
        handler: vi.fn() as any,
        tag: "B",
      },
    },
    crons: {
      anyOne: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
      },
      onlyB: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
        tag: "B",
      },
      sameNameTagDiff: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
        tag: "A",
      },
    },
  });

  return async () => {
    await client?.close();
  };
});

const now = new Date("2023-11-17T11:00:00.000Z");
const future = new Date("2023-11-17T11:05:00.000Z");
const past = new Date("2023-11-17T10:55:00.000Z");
const agesAgo = new Date("2023-01-01T00:00:00.000Z");

beforeEach(async () => {
  await client
    ?.db()
    .collection(MongoJobRepository.JOB_COLLECTION_NAME)
    .deleteMany({});

  vi.useFakeTimers();
  vi.setSystemTime(now);

  return () => {
    vi.useRealTimers();
  };
});

function createSimpleJob(
  data: Pick<IJobsSimple, "status" | "scheduled_for"> &
    Partial<Pick<IJobsSimple, "worker_id" | "started_at" | "name" | "_id">>,
): Omit<IJobsSimple, "_id"> & { _id: ObjectId } {
  const now = new Date();

  const _id = data._id ? new ObjectId(data._id.toString()) : new ObjectId();

  return {
    name: "hello",
    type: JobType.Simple,
    sync: false,
    payload: { name: "Moroine" },
    output: null,
    started_at: null,
    ended_at: null,
    updated_at: now,
    created_at: now,
    worker_id: null,
    ...data,
    _id,
  };
}

function createCronTaskJob(
  data: Pick<IJobsCronTask, "status" | "scheduled_for"> &
    Partial<Pick<IJobsCronTask, "worker_id" | "started_at" | "name">>,
): Omit<IJobsCronTask, "_id"> & { _id: ObjectId } {
  const now = new Date();

  return {
    _id: new ObjectId(),
    name: "hello",
    type: JobType.CronTask,
    started_at: null,
    ended_at: null,
    updated_at: now,
    created_at: now,
    worker_id: null,
    ...data,
  };
}

function createWorker(
  data: Pick<IWorker, "hostname" | "lastSeen"> & Partial<Pick<IWorker, "_id">>,
): Omit<IWorker, "_id"> & { _id: ObjectId } {
  return {
    tags: null,
    ...data,
    _id: new ObjectId(),
  };
}

describe("pickNextJob", () => {
  describe("when no jobs are scheduled", async () => {
    it("should return null", async () => {
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany([
          createSimpleJob({
            status: SimpleJobStatus.Finished,
            scheduled_for: past,
          }),
          createSimpleJob({
            status: SimpleJobStatus.Pending,
            scheduled_for: future,
          }),
          createSimpleJob({
            status: SimpleJobStatus.Errored,
            scheduled_for: past,
          }),
          createSimpleJob({
            status: SimpleJobStatus.Running,
            scheduled_for: future,
            worker_id: new ObjectId(),
          }),
          createCronTaskJob({
            status: SimpleJobStatus.Finished,
            scheduled_for: past,
          }),
          createCronTaskJob({
            status: SimpleJobStatus.Pending,
            scheduled_for: future,
          }),
          createCronTaskJob({
            status: SimpleJobStatus.Errored,
            scheduled_for: past,
          }),
          createCronTaskJob({
            status: SimpleJobStatus.Running,
            scheduled_for: future,
            worker_id: new ObjectId(),
          }),
        ]);

      expect(await jobRepository!.pickNextJob()).toBe(null);
    });
  });

  describe("when jobs are scheduled", async () => {
    it("should return oldest scheduled and update status + worker_id", async () => {
      const jobs = [
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: future,
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: future,
        }),
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: past,
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
        }),
      ] as const;
      await client
        ?.db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany([...jobs]);

      expect(await jobRepository!.pickNextJob()).toEqual(jobs[3]);
      expect(await jobRepository!.getCronTaskJob(jobs[3]._id)).toEqual({
        ...jobs[3],
        worker_id: workerId,
        status: SimpleJobStatus.Running,
        started_at: now,
      });
    });

    it("should return paused jobs but preserve old started_at", async () => {
      const jobs = [
        createSimpleJob({
          status: SimpleJobStatus.Paused,
          scheduled_for: agesAgo,
          started_at: past,
        }),
      ] as const;
      await client!
        .db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany([...jobs]);

      expect(await jobRepository!.pickNextJob()).toEqual(jobs[0]);
      expect(await jobRepository!.getSimpleJob(jobs[0]._id)).toEqual({
        ...jobs[0],
        worker_id: workerId,
        status: SimpleJobStatus.Running,
        started_at: past,
      });
    });

    it("should pick tagged job if workerTags is null", async () => {
      const jobs = [
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: past,
          name: "onlyA",
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
          name: "onlyB",
        }),
      ] as const;
      await client!
        .db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany([...jobs]);

      expect(await jobRepository!.pickNextJob()).toEqual(jobs[1]);
      expect(await jobRepository!.pickNextJob()).toEqual(jobs[0]);
      expect(await jobRepository!.pickNextJob()).toBe(null);
    });
  });

  describe("when worker tags are set", async () => {
    beforeEach(async () => {
      await initJobProcessor({
        ...getOptions(),
        workerTags: ["A"],
      });
    });

    it("should pick jobs with null tag or matching tag", async () => {
      const jobs = [
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: past,
          name: "onlyA",
        }),
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: past,
          name: "anyOne",
        }),
        createSimpleJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
          name: "sameNameTagDiff",
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
          name: "anyOne",
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
          name: "onlyB",
        }),
        createCronTaskJob({
          status: SimpleJobStatus.Pending,
          scheduled_for: agesAgo,
          name: "sameNameTagDiff",
        }),
      ] as const;
      await client!
        .db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .insertMany([...jobs]);

      const todo = [];
      let next = await jobRepository!.pickNextJob();
      while (next !== null) {
        todo.push(next._id.toString());
        next = await jobRepository!.pickNextJob();
      }
      todo.sort();

      const expected = [
        jobs[0]._id.toString(),
        jobs[1]._id.toString(),
        jobs[3]._id.toString(),
        jobs[5]._id.toString(),
      ];
      expected.sort();

      expect(todo).toEqual(expected);
    });
  });
});

describe("detectExitedJobs", () => {
  it("should marked them as failed", async () => {
    const activeWorkers = [
      createWorker({ hostname: SimpleJobStatus.Paused, lastSeen: past }),
      createWorker({ hostname: "self", lastSeen: past, _id: workerId }),
    ] as const;

    const newlyActiveWorkerId = new ObjectId();
    const removedWorkerIdNotSoLongAgo = new ObjectId();
    const removedWorkerId = new ObjectId();

    const jobs = [
      createSimpleJob({ status: SimpleJobStatus.Pending, scheduled_for: past }),
      createSimpleJob({
        status: SimpleJobStatus.Running,
        scheduled_for: past,
        started_at: past,
        worker_id: removedWorkerIdNotSoLongAgo,
      }),
      createSimpleJob({
        status: SimpleJobStatus.Running,
        scheduled_for: agesAgo,
        started_at: new Date(past.getTime() - 1_000),
        worker_id: removedWorkerId,
      }),
      createSimpleJob({
        status: SimpleJobStatus.Running,
        scheduled_for: past,
        started_at: agesAgo,
        worker_id: activeWorkers[0]._id,
      }),
      createCronTaskJob({
        status: SimpleJobStatus.Running,
        scheduled_for: past,
        started_at: now,
        worker_id: newlyActiveWorkerId,
      }),
    ] as const;
    await client!
      .db()
      .collection(MongoJobRepository.JOB_COLLECTION_NAME)
      .insertMany([...jobs]);

    await client!
      .db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .insertMany([...activeWorkers]);

    const expectedJobs = [
      jobs[0],
      // Less than 5min ago, let it be for now
      jobs[1],
      {
        ...jobs[2],
        status: SimpleJobStatus.Errored,
        output: {
          duration: "--",
          result: null,
          error: "Worker crashed unexpectly",
        },
        ended_at: now,
      },
      jobs[3],
      jobs[4],
    ];

    expect(await jobRepository!.detectExitedJobs()).toEqual(expectedJobs[2]);
    expect(await jobRepository!.detectExitedJobs()).toEqual(null);

    expect(
      await client!
        .db()
        .collection(MongoJobRepository.JOB_COLLECTION_NAME)
        .find({})
        .toArray(),
    ).toEqual(expectedJobs);
  });
});
