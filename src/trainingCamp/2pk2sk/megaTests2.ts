import { Config, KeySchema } from '../../types/types';
import { DynamoClient } from '../../clients/dynamo-client';
import dotenv from 'dotenv';
import { Table } from '../../table';
//import { PutItemCommandInput } from '@aws-sdk/client-dynamodb';
dotenv.config();

const config: Config = {
  region: process.env.AWS_REGION || '',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_ACCESS_KEY_SECRET || '',
  },
};

const keySchema: KeySchema = {
  pk: {
    name: 'timestamp',
    keys: ['timestamp'],
  },
  sk: {
    name: 'timestamp#count',
    keys: ['timestamp', 'count'],
    separator: '#',
  },
};

type pkDto = {
  timestamp: string;
};

type skDto = {
  timestamp: string;
  count: string;
};

type dataDto = {};

//type itemDto = pkDto & skDto & dataDto;

async function testQuerySimple(tablaDePrueba: Table<pkDto, skDto, dataDto>) {

  await tablaDePrueba.putBatch([
    { timestamp: "1", count: "01" },
    { timestamp: "1", count: "02" },
    { timestamp: "1", count: "03" },
    { timestamp: "1", count: "04" },
    { timestamp: "1", count: "05" },
    { timestamp: "1", count: "06" },
    { timestamp: "1", count: "07" },
    { timestamp: "1", count: "08" },
    { timestamp: "1", count: "09" },
    { timestamp: "1", count: "10" },
  ]);

  for (let i = 1; i <= 5; i++) {
    const item = await tablaDePrueba.getOne({ timestamp: "1" }, { timestamp: "1", count: `0${i}` });
    if (!item) {
      throw new Error(`Item ${i} does not exist`);
    }
  }

  const items2 = await tablaDePrueba.query({ pk: { timestamp: "1" }, limit: 10 }).whereSKGreaterThanOrEqual({ timestamp: "1", count: "06" }).run();
  for (let i = 6; i <= 9; i++) {
    const item = items2.items.find(item => item.count === `0${i}`);
    if (!item) {
      throw new Error(`Item ${i} does not exist`);
    }
  }
  if (!items2.items.find(item => item.count === "10")) {
    throw new Error('Item 10 should exist');
  }

  //now delete all with timestamp 1
  await tablaDePrueba.deletePartition({ timestamp: "1" });

  //assert that all items with timestamp 1 are deleted
  const items = await tablaDePrueba.scan({ limit: 10 }).run();
  if (items.items.length !== 0) {
    throw new Error('Items with timestamp 1 are not deleted');
  }
  console.log('testQuerySimple completed successfully');
}

async function testQueryMedium(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 90 items with timestamp="2" and count from "01" to "90"
  // Split into batches of 25 (DynamoDB limit)
  const items = [];
  for (let i = 1; i <= 25; i++) {
    items.push({
      timestamp: "2",
      count: i.toString().padStart(2, '0'), // "01", "02", ..., "90"
    });
  }

  // Insert in batches of 25
  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Get the first 7 items using getPartitionBatch
  const firstBatch = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
  });
  // Test that we got items from "01" to "07"
  if (firstBatch.items.length !== 7) {
    throw new Error(`Expected 7 items, got ${firstBatch.items.length}`);
  }
  for (let i = 1; i <= 7; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = firstBatch.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in first batch`);
    }
  }

  // Test that lastEvaluatedKey is "07" (timestamp="2", count="07")
  if (!firstBatch.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null');
  }
  if (firstBatch.lastEvaluatedKey.timestamp !== "2" || firstBatch.lastEvaluatedKey.count !== "07") {
    throw new Error(`lastEvaluatedKey timestamp should be "2", got ${firstBatch.lastEvaluatedKey.timestamp}`);
  }

  // Test that firstEvaluatedKey is null/undefined
  if (!firstBatch.firstEvaluatedKey || firstBatch.firstEvaluatedKey.timestamp !== "2" || firstBatch.firstEvaluatedKey?.count !== "01") {
    throw new Error(`firstEvaluatedKey should be undefined, got ${firstBatch.firstEvaluatedKey}`);
  }

  if (firstBatch.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (firstBatch.hasPrevious != false) {
    throw new Error('hasPrevious should be false');
  }

  // Use the lastEvaluatedKey as pivot to get the next 7 items
  const secondBatch = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: firstBatch.lastEvaluatedKey,
      direction: 'forward',
    },
  });
  // Test that we got items from "08" to "14"
  if (secondBatch.items.length !== 7) {
    throw new Error(`Expected 7 items in second batch, got ${secondBatch.items.length}`);
  }
  for (let i = 8; i <= 14; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = secondBatch.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in second batch`);
    }
  }

  // Test that lastEvaluatedKey is "14"
  if (!secondBatch.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in second batch');
  }
  if (secondBatch.lastEvaluatedKey.timestamp !== "2" || secondBatch.lastEvaluatedKey.count !== "14") {
    throw new Error(`lastEvaluatedKey timestamp should be "2", got ${secondBatch.lastEvaluatedKey.timestamp}`);
  }

  if (!secondBatch.firstEvaluatedKey || secondBatch.firstEvaluatedKey?.timestamp !== "2" || secondBatch.firstEvaluatedKey?.count !== "08") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "08", got ${secondBatch.firstEvaluatedKey?.timestamp} and ${secondBatch.firstEvaluatedKey?.count}`);
  }

  if (secondBatch.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (secondBatch.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch3 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: secondBatch.lastEvaluatedKey,
      direction: 'forward',
    },
  });

  if (batch3.items.length !== 7) {
    throw new Error(`Expected 7 items in batch3, got ${batch3.items.length}`);
  }
  for (let i = 15; i <= 21; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = batch3.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in batch3`);
    }
  }

  if (!batch3.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch3');
  }
  if (batch3.lastEvaluatedKey.timestamp !== "2" || batch3.lastEvaluatedKey.count !== "21") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "21", got ${batch3.lastEvaluatedKey.timestamp} and ${batch3.lastEvaluatedKey.count}`);
  }
  if (!batch3.firstEvaluatedKey || batch3.firstEvaluatedKey.timestamp !== "2" || batch3.firstEvaluatedKey?.count !== "15") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "15", got ${batch3.firstEvaluatedKey?.timestamp} and ${batch3.firstEvaluatedKey?.count}`);
  }

  if (batch3.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (batch3.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch4 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: batch3.firstEvaluatedKey,
      direction: 'backward',
    },
  });

  if (batch4.items.length !== 7) {
    throw new Error(`Expected 7 items in batch4, got ${batch4.items.length}`);
  }
  for (let i = 14; i >= 8; i--) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = batch4.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in batch4`);
    }
  }

  if (!batch4.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch4');
  }
  if (batch4.lastEvaluatedKey.timestamp !== "2" || batch4.lastEvaluatedKey.count !== "08") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "08", got ${batch4.lastEvaluatedKey.timestamp} and ${batch4.lastEvaluatedKey.count}`);
  }
  if (!batch4.firstEvaluatedKey || batch4.firstEvaluatedKey.timestamp !== "2" || batch4.firstEvaluatedKey?.count !== "14") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "14", got ${batch4.firstEvaluatedKey?.timestamp} and ${batch4.firstEvaluatedKey?.count}`);
  }

  if (batch4.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (batch4.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch5 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: batch4.lastEvaluatedKey,
      direction: 'backward',
    },
  });


  if (batch5.items.length !== 7) {
    throw new Error(`Expected 7 items in batch5, got ${batch5.items.length}`);
  }
  for (let i = 7; i >= 1; i--) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = batch5.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in batch5`);
    }
  }

  if (!batch5.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch5');
  }
  if (batch5.lastEvaluatedKey.timestamp !== "2" || batch5.lastEvaluatedKey.count !== "01") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "07", got ${batch5.lastEvaluatedKey.timestamp} and ${batch5.lastEvaluatedKey.count}`);
  }
  if (!batch5.firstEvaluatedKey || batch5.firstEvaluatedKey.timestamp !== "2" || batch5.firstEvaluatedKey?.count !== "07") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "01", got ${batch5.firstEvaluatedKey?.timestamp} and ${batch5.firstEvaluatedKey?.count}`);
  }

  if (batch5.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch5.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch6 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: batch3.lastEvaluatedKey,
      direction: 'forward',
    },
  });

  if (batch6.items.length !== 4) {
    throw new Error(`Expected 4 items in batch6, got ${batch6.items.length}`);
  }
  for (let i = 22; i <= 25; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = batch6.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in batch6`);
    }
  }
  if (batch6.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch6.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  if (!batch6.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch6');
  }
  if (batch6.lastEvaluatedKey.timestamp !== "2" || batch6.lastEvaluatedKey.count !== "25") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "25", got ${batch6.lastEvaluatedKey.timestamp} and ${batch6.lastEvaluatedKey.count}`);
  }
  if (!batch6.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch6');
  }
  if (batch6.firstEvaluatedKey.timestamp !== "2" || batch6.firstEvaluatedKey.count !== "22") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "22", got ${batch6.firstEvaluatedKey.timestamp} and ${batch6.firstEvaluatedKey.count}`);
  }

  const batch7 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 7,
    pagination: {
      pivot: batch6.lastEvaluatedKey,
      direction: 'forward',
    },
  });

  if (batch7.items.length !== 0) {
    throw new Error(`Expected 0 items in batch7, got ${batch7.items.length}`);
  }
  if (batch7.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch7.hasPrevious != false) {
    throw new Error('hasPrevious should be true');
  }
  if (batch7.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be null in batch7');
  }
  if (batch7.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should be null in batch7');
  }

  const batch8 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 20,
    pagination: {
      pivot: batch6.firstEvaluatedKey,
      direction: 'backward',
    },
  });

  if (batch8.items.length !== 20) {
    throw new Error(`Expected 20 items in batch8, got ${batch8.items.length}`);
  }
  for (let i = 2; i <= 21; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const item = batch8.items.find(item => item.count === expectedCount);
    if (!item) {
      throw new Error(`Item with count ${expectedCount} not found in batch8`);
    }
  }

  if (!batch8.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch8');
  }
  if (batch8.lastEvaluatedKey.timestamp !== "2" || batch8.lastEvaluatedKey.count !== "02") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "01", got ${batch8.lastEvaluatedKey.timestamp} and ${batch8.lastEvaluatedKey.count}`);
  }
  if (!batch8.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch8');
  }
  if (batch8.firstEvaluatedKey.timestamp !== "2" || batch8.firstEvaluatedKey.count !== "21") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "21", got ${batch8.firstEvaluatedKey.timestamp} and ${batch8.firstEvaluatedKey.count}`);
  }

  if (batch8.hasNext != true) {
    throw new Error('hasNext should be false');
  }
  if (batch8.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch9 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 20,
    pagination: {
      pivot: batch8.lastEvaluatedKey,
      direction: 'backward',
    },
  });

  if (batch9.items.length !== 1) {
    throw new Error(`Expected 1 item in batch9, got ${batch9.items.length}`);
  }
  if (batch9.items[0].count !== "01") {
    throw new Error(`Item with count "01" not found in batch9`);
  }
  if (!batch9.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch9');
  }
  if (batch9.lastEvaluatedKey.timestamp !== "2" || batch9.lastEvaluatedKey.count !== "01") {
    throw new Error(`lastEvaluatedKey timestamp should be "2" and count "01", got ${batch9.lastEvaluatedKey.timestamp} and ${batch9.lastEvaluatedKey.count}`);
  }
  if (!batch9.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch9');
  }
  if (batch9.firstEvaluatedKey.timestamp !== "2" || batch9.firstEvaluatedKey.count !== "01") {
    throw new Error(`firstEvaluatedKey timestamp should be "2" and count "01", got ${batch9.firstEvaluatedKey.timestamp} and ${batch9.firstEvaluatedKey.count}`);
  }
  if (batch9.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch9.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch10 = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: "2" },
    limit: 20,
    pagination: {
      pivot: batch9.lastEvaluatedKey,
      direction: 'backward',
    },
  });
  if (batch10.items.length !== 0) {
    throw new Error(`Expected 0 items in batch10, got ${batch10.items.length}`);
  }
  if (batch10.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch10.hasPrevious != false) {
    throw new Error('hasPrevious should be true');
  }
  if (batch10.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be null in batch10');
  }
  if (batch10.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should be null in batch10');
  }
  // Finally delete the partition
  await tablaDePrueba.deletePartition({ timestamp: "2" });

  // Assert that all items with timestamp 2 are deleted
  const itemsAfterDelete = await tablaDePrueba.scan({ limit: 100 }).run();
  const remainingItems = itemsAfterDelete.items.filter(item => item.timestamp === "2");
  if (remainingItems.length !== 0) {
    throw new Error(`Items with timestamp 2 are not deleted. Found ${remainingItems.length} items`);
  }
  console.log('testQueryMedium completed successfully');
}

async function main() {
  const client = new DynamoClient(config);
  const nombreTabla = 'tabla';
  const tablaDePrueba = client.table<
    pkDto,
    skDto,
    dataDto
  >(nombreTabla, keySchema);
  await tablaDePrueba.flush();
  const scanResult4 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult4.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
  await testQuerySimple(tablaDePrueba);
  await testQueryMedium(tablaDePrueba);
  await tablaDePrueba.flush();
  const scanResult5 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult5.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
}



/*
putBatch(items, override?) (line 362)

updateRaw(updateItemCommandInput) (line 390)
updateBatch(updates) (line 401)

deleteRaw(deleteItemCommandInput) (line 434)

getPartitionBatch(qparams) (line 494)
query(qparams) (line 522)

queryRaw(commandInput) (line 564)
scanRaw(commandInput) (line 578)
*/
main().catch(console.error);
