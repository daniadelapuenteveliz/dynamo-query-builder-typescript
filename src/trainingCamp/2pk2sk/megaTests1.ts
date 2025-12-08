import { KeySchema, Config } from '../../types/types';
import { DynamoClient } from '../../clients/dynamo-client';
import dotenv from 'dotenv';
import { Table } from '../../table';
import { PutItemCommandInput } from '@aws-sdk/client-dynamodb';
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

type dataDto = {
  A: number;
  B: string;
};

type itemDto = pkDto & skDto & dataDto;
async function testMetadata(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  const tableName = tablaDePrueba.getTableName();
  if (tableName !== 'tabla') {
    throw new Error('Table name is not correct');
  }
  console.log('table name is correct', tableName);
  const tableInDynamo = await tablaDePrueba.getTableNameInDynamo();
  if (tableInDynamo !== 'tabla') {
    throw new Error('Table name in dynamo is not correct');
  }
  console.log('table name in dynamo is correct', tableInDynamo);
  const dynamoKeySchema: any = await tablaDePrueba.getDynamoKeySchema();
  if (
    dynamoKeySchema[0].AttributeName !== 'timestamp' ||
    dynamoKeySchema[0].KeyType !== 'HASH'
  ) {
    throw new Error('Timestamp is not correct in dynamo key schema');
  }
  if (
    dynamoKeySchema[1].AttributeName !== 'timestamp#count' ||
    dynamoKeySchema[1].KeyType !== 'RANGE'
  ) {
    throw new Error('timestamp#count is not correct in dynamo key schema');
  }
  console.log('dynamo key schema is correct');
  try {
    const globalSecondaryIndexes =
      await tablaDePrueba.getGlobalSecondaryIndexes();
    console.log(globalSecondaryIndexes);
  } catch (error: any) {
    console.log(error.code);
    if (error.code !== 'GLOBAL_SECONDARY_INDEXES_NOT_FOUND') {
      throw error;
    }
  }
}

async function testPut(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  const item: itemDto = {
    timestamp: '1',
    count: '1',
    A: 1,
    B: 'test',
  };

  await tablaDePrueba.put(item);

  const testItem1 = await tablaDePrueba.getOne(
    { timestamp: '1' },
    { timestamp: '1', count: '1' }
  );
  console.log(testItem1);
  if (
    testItem1.timestamp !== '1' ||
    testItem1.count !== '1' ||
    testItem1.A !== 1 ||
    testItem1.B !== 'test'
  ) {
    throw new Error('Item is not correct');
  }

  //scan and check that only one item is present
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  console.log(scanResult);
  if (scanResult.items.length !== 1) {
    throw new Error('Scan result is not correct');
  }
  //try create again
  try {
    await tablaDePrueba.put(item);
  } catch (error: any) {
    console.log(error.code);
    if (error.code !== 'ITEM_ALREADY_EXISTS') {
      throw error;
    }
  }

  //update and check
  await tablaDePrueba.update(
    { timestamp: '1' },
    { timestamp: '1', count: '1' },
    { A: 2, B: 'test2' }
  );
  const testItem2 = await tablaDePrueba.getOne(
    { timestamp: '1' },
    { timestamp: '1', count: '1' }
  );
  console.log(testItem2);
  if (
    testItem2.timestamp !== '1' ||
    testItem2.count !== '1' ||
    testItem2.A !== 2 ||
    testItem2.B !== 'test2'
  ) {
    throw new Error('Item is not correct');
  }
  //delete and scan and check that no items are present
  await tablaDePrueba.delete(
    { timestamp: '1' },
    { timestamp: '1', count: '1' }
  );
  const scanResult2 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult2.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }

  //update item that does not exist
  try {
    await tablaDePrueba.update(
      { timestamp: '2' },
      { timestamp: '2', count: '2' },
      { A: 3, B: 'test3' }
    );
  } catch (error: any) {
    console.log(error.code);
    if (error.code !== 'ITEM_DOES_NOT_EXIST') {
      throw error;
    }
  }

  //try delete item that does not exist
  try {
    await tablaDePrueba.delete(
      { timestamp: '2' },
      { timestamp: '2', count: '2' }
    );
  } catch (error: any) {
    console.log(error.code);
    if (error.code !== 'ITEM_DOES_NOT_EXIST') {
      throw error;
    }
  }
}

async function putRaw(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Before the first put, scan and get 0 items
  const initialScan = await tablaDePrueba.scan({ limit: 10 }).run();
  console.log('Initial scan result:', initialScan.items.length);
  if (initialScan.items.length !== 0) {
    throw new Error('Expected 0 items in initial scan');
  }

  // Put 1 item with pk=0 (timestamp="0")
  const item1: PutItemCommandInput = {
    TableName: 'tabla',
    Item: {
      timestamp: { S: '0' },
      'timestamp#count': { S: '0#01' },
      A: { N: '1' },
      B: { S: 'test1' },
    },
  };
  await tablaDePrueba.putRaw(item1);

  // Get and assert that the object is there
  const retrievedItem1 = await tablaDePrueba.getOne(
    { timestamp: '0' },
    { timestamp: '0', count: '01' }
  );
  console.log('Retrieved item 1:', retrievedItem1);
  if (
    retrievedItem1.timestamp !== '0' ||
    retrievedItem1.count !== '01' ||
    retrievedItem1.A !== 1 ||
    retrievedItem1.B !== 'test1'
  ) {
    throw new Error('Item 1 is not correct');
  }

  // Put 10 items with pk=1 (timestamp="1") and SK incremental
  const items10: PutItemCommandInput[] = [];
  for (let i = 1; i <= 10; i++) {
    if (i < 10) {
      items10.push({
        TableName: 'tabla',
        Item: {
          timestamp: { S: '1' },
          'timestamp#count': { S: `1#0${i}` },
          A: { N: String(i) },
          B: { S: `test${i}` },
        },
      });
    } else {
      items10.push({
        TableName: 'tabla',
        Item: {
          timestamp: { S: '1' },
          'timestamp#count': { S: `1#${i}` },
          A: { N: String(i) },
          B: { S: `test${i}` },
        },
      });
    }
  }

  for (const item of items10) {
    await tablaDePrueba.putRaw(item);
  }

  // Use getPartitionBatch to get all items and assert all were inserted
  const partitionBatch = await tablaDePrueba.getPartitionBatch({
    pk: { timestamp: '1' },
    limit: 20,
  });
  console.log('Partition batch result:', partitionBatch.items.length);
  if (partitionBatch.items.length !== 10) {
    throw new Error(`Expected 10 items, got ${partitionBatch.items.length}`);
  }

  // Verify all 10 items are present
  for (let i = 1; i <= 10; i++) {
    const found = partitionBatch.items.find(
      item =>
        item.timestamp === '1' && item.count === (i < 10 ? `0${i}` : String(i))
    );
    if (!found) {
      throw new Error(`Item with count ${i} not found`);
    }
    if (Number(found.A) !== i || found.B !== `test${i}`) {
      throw new Error(`Item with count ${i} has incorrect data`);
    }
  }

  // Put 60 items with pk=2 (timestamp="2") and SK incremental
  const items60: PutItemCommandInput[] = [];
  for (let i = 1; i <= 60; i++) {
    if (i < 10) {
      items60.push({
        TableName: 'tabla',
        Item: {
          timestamp: { S: '2' },
          'timestamp#count': { S: `2#0${i}` },
          A: { N: String(i) },
          B: { S: `test${i}` },
        },
      });
    } else {
      items60.push({
        TableName: 'tabla',
        Item: {
          timestamp: { S: '2' },
          'timestamp#count': { S: `2#${i}` },
          A: { N: String(i) },
          B: { S: `test${i}` },
        },
      });
    }
  }

  for (const item of items60) {
    await tablaDePrueba.putRaw(item);
  }

  // Use getPartitionBatch with pagination (limit 13) to assert everything was inserted
  let allItems: itemDto[] = [];
  let lastEvaluatedKey: any = undefined;
  let hasMore = true;
  let pageCount = 0;

  let breakCount = 100;
  while (hasMore) {
    if (breakCount === 0) {
      throw new Error('Break count is 0');
    }
    breakCount--;
    const batchParams: any = {
      pk: { timestamp: '2' },
      limit: 13,
    };

    if (lastEvaluatedKey) {
      batchParams.pagination = {
        pivot: lastEvaluatedKey,
        direction: 'forward' as const,
      };
    }

    const batch = await tablaDePrueba.getPartitionBatch(batchParams);
    allItems = allItems.concat(batch.items as unknown as itemDto[]);
    lastEvaluatedKey = batch.lastEvaluatedKey;
    hasMore = batch.hasNext;
    pageCount++;

    console.log(
      `Page ${pageCount}: Retrieved ${batch.items.length} items, hasNext: ${hasMore}`
    );
  }

  console.log(
    `Total items retrieved: ${allItems.length} in ${pageCount} pages`
  );
  if (allItems.length !== 60) {
    throw new Error(`Expected 60 items, got ${allItems.length}`);
  }

  // Verify all 60 items are present
  for (let i = 1; i <= 60; i++) {
    const found = allItems.find(
      item =>
        item.timestamp === '2' && item.count === (i < 10 ? `0${i}` : String(i))
    );
    if (!found) {
      throw new Error(
        `Item with count ${i} (${i < 10 ? `0${i}` : String(i)}) not found`
      );
    }
    if (Number(found.A) !== i || found.B !== `test${i}`) {
      throw new Error(
        `Item with count ${i} (${i < 10 ? `0${i}` : String(i)}) has incorrect data`
      );
    }
  }

  await tablaDePrueba.deletePartition({ timestamp: '2' });
  await tablaDePrueba.deletePartition({ timestamp: '1' });
  await tablaDePrueba.deletePartition({ timestamp: '0' });
  const scanResult3 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult3.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
  console.log('putRaw test completed successfully');
}

async function main() {
  const client = new DynamoClient(config);
  const nombreTabla = 'tabla';
  const tablaDePrueba = client.table<pkDto, skDto, dataDto>(
    nombreTabla,
    keySchema
  );
  await tablaDePrueba.flush();
  const scanResult4 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult4.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
  await testMetadata(tablaDePrueba);
  await testPut(tablaDePrueba);
  await putRaw(tablaDePrueba);
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
