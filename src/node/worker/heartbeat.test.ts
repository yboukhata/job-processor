import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initJobProcessor, SupportedDbType } from "../setup.ts";
import {
  heartbeatEvent,
  startHeartbeat,
  startSyncHeartbeat,
  workerId,
} from "./heartbeat.ts";
import { IWorker as IWorkerBase } from "../index.ts";
import { MongoJobRepository } from "../data/MongoJobRepository.ts";

let client: MongoClient | null;
const startDate = new Date("2023-11-17T11:00:00.000Z");

type IWorker = Omit<IWorkerBase, "_id"> & { _id: ObjectId };
const otherWorkers: IWorker[] = [
  {
    _id: new ObjectId(),
    hostname: "a",
    lastSeen: new Date(startDate.getTime() - 1_000),
    tags: null,
  },
  {
    _id: new ObjectId(),
    hostname: "b",
    lastSeen: new Date(startDate.getTime() - 500),
    tags: null,
  },
];

beforeAll(async () => {
  client = new MongoClient(
    `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}_${process.env["VITEST_WORKER_ID"]}`,
  );
  await client.connect();
  await initJobProcessor({
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
  });

  return async () => {
    await client?.close();
  };
});

beforeEach(async () => {
  await client
    ?.db()
    .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
    .deleteMany({});
  await client
    ?.db()
    .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
    .insertMany(otherWorkers);

  vi.useFakeTimers();
  vi.setSystemTime(startDate);

  return () => {
    vi.useRealTimers();
  };
});

describe("startHeartbeat", () => {
  it("should manage heartbeat lifecycle", async () => {
    const abortController = new AbortController();
    await startHeartbeat(true, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    // Once started, it should be referenced in worker collection without affecting other workers
    let workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({}, { sort: { lastSeen: 1 } })
        .toArray()) || [];
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: startDate,
      tags: null,
    });

    // It should have registered interval to update timer
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    let onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({})
        .toArray()) || [];
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 30_000),
      tags: null,
    });

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({})
        .toArray()) || [];
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 60_000),
      tags: null,
    });

    abortController.abort();
    await new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    expect(vi.getTimerCount()).toBe(0);

    workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({})
        .toArray()) || [];
    expect(workers).toHaveLength(2);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
  });

  it("when isWorker=true should exit on heartbeat error after 3 consecutive failure", async () => {
    const abortController = new AbortController();
    await startHeartbeat(true, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .deleteOne({ _id: workerId });

    // First interval
    const onFail1 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail1).resolves.toBeUndefined();

    // Second interval
    const onFail2 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail2).resolves.toBeUndefined();

    // Last interval
    const onKill = new Promise((resolve) => {
      heartbeatEvent.once("kill", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onKill).resolves.toBeUndefined();

    expect(vi.getTimerCount()).toBe(0);
  });

  it("should reset error count after every success", async () => {
    const abortController = new AbortController();
    await startHeartbeat(true, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .deleteOne({ _id: workerId });

    // First interval
    const onFail1 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail1).resolves.toBeUndefined();

    // Second interval
    const onFail2 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail2).resolves.toBeUndefined();

    // Error should be resolved
    await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .insertOne({
        _id: workerId,
        lastSeen: new Date(),
        hostname: "worker_1",
        tags: null,
      });

    // Last interval
    const onPing1 = new Promise((resolve) => {
      heartbeatEvent.once("ping", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onPing1).resolves.toBeUndefined();

    // Last interval
    const onPing2 = new Promise((resolve) => {
      heartbeatEvent.once("ping", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onPing2).resolves.toBeUndefined();

    await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .deleteOne({ _id: workerId });

    // First interval
    const onFail3 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail3).resolves.toBeUndefined();

    // Second interval
    const onFail4 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail4).resolves.toBeUndefined();

    // Last interval
    const onKill = new Promise((resolve) => {
      heartbeatEvent.once("kill", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onKill).resolves.toBeUndefined();

    expect(vi.getTimerCount()).toBe(0);
  });

  it("when isWorker=false should keep trying on heartbeat error", async () => {
    const abortController = new AbortController();
    await startHeartbeat(false, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .deleteOne({ _id: workerId });

    // First interval
    const onFail1 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail1).resolves.toBeUndefined();

    // Second interval
    const onFail2 = new Promise((resolve) => {
      heartbeatEvent.once("fail", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onFail2).resolves.toBeUndefined();

    // Last interval recreate
    const onPing = new Promise((resolve) =>
      heartbeatEvent.once("ping", resolve),
    );
    await vi.runOnlyPendingTimersAsync();
    await expect(onPing).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(1);

    // Expect worker has been re-created
    let workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({}, { sort: { lastSeen: 1 } })
        .toArray()) || [];
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 90_000),
      tags: null,
    });

    const onStop = new Promise((resolve) =>
      heartbeatEvent.once("stop", resolve),
    );
    abortController.abort();
    await onStop;
    expect(vi.getTimerCount()).toBe(0);

    workers =
      (await client
        ?.db()
        .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
        .find({})
        .toArray()) || [];
    // When not ran in worker mode, we should not delete worker
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 90_000),
      tags: null,
    });
  });
});

describe("startSyncHeartbeat", () => {
  it("should manage concurrency", async () => {
    let worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker).toBe(null);

    // 2 Jobs starting concurrently
    const [finallyCb1, finallyCb2] = await Promise.all([
      startSyncHeartbeat(),
      startSyncHeartbeat(),
    ]);

    // Once started, worker should exists
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(startDate);

    // It should have registered interval to update timer
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    let onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(
      new Date(startDate.getTime() + 30_000),
    );

    // Job #2 stops
    finallyCb2();

    // Heartbeat should continue
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(
      new Date(startDate.getTime() + 60_000),
    );

    // New job starting
    const finallyCb3 = await startSyncHeartbeat();

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(
      new Date(startDate.getTime() + 90_000),
    );

    // Job #1 stops
    finallyCb1();

    // Heartbeat should continue
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(
      new Date(startDate.getTime() + 120_000),
    );

    let onStop = new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    // Job #3 stops
    finallyCb3();

    // It should stop
    await onStop;

    expect(vi.getTimerCount()).toBe(0);

    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker).not.toBe(null);

    // 2 Jobs starting concurrently
    const [finallyCb4, finallyCb5] = await Promise.all([
      startSyncHeartbeat(),
      startSyncHeartbeat(),
    ]);

    // Heartbeat should restart
    expect(vi.getTimerCount()).toBe(1);

    // Expect worker has been created
    worker = await client
      ?.db()
      .collection(MongoJobRepository.WORKER_COLLECTION_NAME)
      .findOne({ _id: workerId });
    expect(worker?.["lastSeen"]).toEqual(
      new Date(startDate.getTime() + 120_000),
    );

    onStop = new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    // Jobs stopping concurrently
    finallyCb4();
    finallyCb5();
    await onStop;

    expect(vi.getTimerCount()).toBe(0);
  });
});
