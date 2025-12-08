import { Config, KeySchema } from '../../types/types';
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
    name: 'A#B#C#D',
    keys: ['A', 'B', 'C', 'D'],
    separator: '#',
  },
  sk: {
    name: 'E#F#G#H',
    keys: ['E', 'F', 'G', 'H'],
    separator: '#',
  },
};

type pkDto = {
  A: string;
  B: string;
  C: string;
  D: string;
};

type skDto = {
  E: string;
  F: string;
  G: string;
  H: string;
};

type dataDto = {
  I: string;
  J: string;
};

type itemDto = pkDto & skDto & dataDto;
async function testMetadata(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  const tableName = tablaDePrueba.getTableName();
  if (tableName !== 'tabla2') {
    throw new Error('Table name is not correct');
  }
  console.log('table name is correct', tableName);
  const tableInDynamo = await tablaDePrueba.getTableNameInDynamo();
  if (tableInDynamo !== 'tabla2') {
    throw new Error('Table name in dynamo is not correct');
  }
  console.log('table name in dynamo is correct', tableInDynamo);
  const dynamoKeySchema: any = await tablaDePrueba.getDynamoKeySchema();
  if (
    dynamoKeySchema[0].AttributeName !== 'A#B#C#D' ||
    dynamoKeySchema[0].KeyType !== 'HASH'
  ) {
    throw new Error('Timestamp is not correct in dynamo key schema');
  }
  if (
    dynamoKeySchema[1].AttributeName !== 'E#F#G#H' ||
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
    A: 'a',
    B: 'b',
    C: 'c',
    D: 'd',
    E: 'e',
    F: 'f',
    G: 'g',
    H: 'h',
    I: 'i',
    J: 'j',
  };

  await tablaDePrueba.put(item);

  const testItem1 = await tablaDePrueba.getOne(
    { A: 'a', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' }
  );
  console.log(testItem1);
  if (
    testItem1.A !== 'a' ||
    testItem1.B !== 'b' ||
    testItem1.C !== 'c' ||
    testItem1.D !== 'd' ||
    testItem1.E !== 'e' ||
    testItem1.F !== 'f' ||
    testItem1.G !== 'g' ||
    testItem1.H !== 'h'
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
    { A: 'a', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' },
    { I: 'i2', J: 'j2' }
  );
  const testItem2 = await tablaDePrueba.getOne(
    { A: 'a', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' }
  );
  console.log(testItem2);
  if (
    testItem2.A !== 'a' ||
    testItem2.B !== 'b' ||
    testItem2.C !== 'c' ||
    testItem2.D !== 'd' ||
    testItem2.E !== 'e' ||
    testItem2.F !== 'f' ||
    testItem2.G !== 'g' ||
    testItem2.H !== 'h' ||
    testItem2.I !== 'i2' ||
    testItem2.J !== 'j2'
  ) {
    throw new Error('Item is not correct');
  }
  //delete and scan and check that no items are present
  await tablaDePrueba.delete(
    { A: 'a', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' }
  );
  const scanResult2 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult2.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }

  //update item that does not exist
  try {
    await tablaDePrueba.update(
      { A: 'b', B: 'b', C: 'c', D: 'd' },
      { E: 'e', F: 'f', G: 'g', H: 'h' },
      { I: 'i2', J: 'j2' }
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
      { A: 'b', B: 'b', C: 'c', D: 'd' },
      { E: 'e', F: 'f', G: 'g', H: 'h' }
    );
  } catch (error: any) {
    console.log(error.code);
    if (error.code !== 'ITEM_DOES_NOT_EXIST') {
      throw error;
    }
  }

  //delete and check with scan
  await tablaDePrueba.delete(
    { A: 'a', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' }
  );
  const scanResult3 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult3.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
  console.log('delete and check with scan test completed successfully');
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
    TableName: 'tabla2',
    Item: {
      'A#B#C#D': { S: 'a1#b#c#d' },
      'E#F#G#H': { S: 'e#f#g#h' },
      I: { S: 'i' },
      J: { S: 'j' },
    },
  };
  await tablaDePrueba.putRaw(item1);

  // Get and assert that the object is there
  const retrievedItem1 = await tablaDePrueba.getOne(
    { A: 'a1', B: 'b', C: 'c', D: 'd' },
    { E: 'e', F: 'f', G: 'g', H: 'h' }
  );
  console.log('Retrieved item 1:', retrievedItem1);
  if (
    retrievedItem1.A !== 'a1' ||
    retrievedItem1.B !== 'b' ||
    retrievedItem1.C !== 'c' ||
    retrievedItem1.D !== 'd' ||
    retrievedItem1.E !== 'e' ||
    retrievedItem1.F !== 'f' ||
    retrievedItem1.G !== 'g' ||
    retrievedItem1.H !== 'h' ||
    retrievedItem1.I !== 'i' ||
    retrievedItem1.J !== 'j'
  ) {
    throw new Error('Item 1 is not correct');
  }

  // Put 10 items with pk=1 (timestamp="1") and SK incremental
  const items10: PutItemCommandInput[] = [];
  for (let i = 1; i <= 10; i++) {
    items10.push({
      TableName: 'tabla2',
      Item: {
        'A#B#C#D': { S: 'a2#b#c#d' },
        'E#F#G#H': { S: 'e#f#g#h' + (i < 10 ? `0${i}` : String(i)) },
        I: { S: 'i' + (i < 10 ? `0${i}` : String(i)) },
        J: { S: 'j' + (i < 10 ? `0${i}` : String(i)) },
      },
    });
  }

  for (const item of items10) {
    await tablaDePrueba.putRaw(item);
  }

  // Use getPartitionBatch to get all items and assert all were inserted
  const partitionBatch = await tablaDePrueba.getPartitionBatch({
    pk: { A: 'a2', B: 'b', C: 'c', D: 'd' },
    limit: 20,
  });
  console.log(partitionBatch);
  console.log('Partition batch result:', partitionBatch.items.length);
  if (partitionBatch.items.length !== 10) {
    throw new Error(`Expected 10 items, got ${partitionBatch.items.length}`);
  }

  // Verify all 10 items are present
  for (let i = 1; i <= 10; i++) {
    const found = partitionBatch.items.find(
      item =>
        item.A === 'a2' &&
        item.B === 'b' &&
        item.C === 'c' &&
        item.D === 'd' &&
        item.E === 'e' &&
        item.F === 'f' &&
        item.G === 'g' &&
        item.H === 'h' + (i < 10 ? `0${i}` : String(i)) &&
        item.I === 'i' + (i < 10 ? `0${i}` : String(i)) &&
        item.J === 'j' + (i < 10 ? `0${i}` : String(i))
    );
    if (!found) {
      throw new Error(`Item with count ${i} not found`);
    }
  }

  // Put 60 items with pk=2 (timestamp="2") and SK incremental
  const items60: PutItemCommandInput[] = [];
  for (let i = 1; i <= 60; i++) {
    items60.push({
      TableName: 'tabla2',
      Item: {
        'A#B#C#D': { S: 'a3#b#c#d' },
        'E#F#G#H': { S: 'e#f#g#h' + (i < 10 ? `0${i}` : String(i)) },
        I: { S: 'i' + (i < 10 ? `0${i}` : String(i)) },
        J: { S: 'j' + (i < 10 ? `0${i}` : String(i)) },
      },
    });
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
      pk: { A: 'a3', B: 'b', C: 'c', D: 'd' },
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
        item.A === 'a3' &&
        item.B === 'b' &&
        item.C === 'c' &&
        item.D === 'd' &&
        item.E === 'e' &&
        item.F === 'f' &&
        item.G === 'g' &&
        item.H === 'h' + (i < 10 ? `0${i}` : String(i)) &&
        item.I === 'i' + (i < 10 ? `0${i}` : String(i)) &&
        item.J === 'j' + (i < 10 ? `0${i}` : String(i))
    );
    if (!found) {
      throw new Error(
        `Item with count ${i} (${i < 10 ? `0${i}` : String(i)}) not found`
      );
    }
  }

  await tablaDePrueba.deletePartition({ A: 'a1', B: 'b', C: 'c', D: 'd' });
  await tablaDePrueba.deletePartition({ A: 'a2', B: 'b', C: 'c', D: 'd' });
  await tablaDePrueba.deletePartition({ A: 'a3', B: 'b', C: 'c', D: 'd' });
  const scanResult3 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult3.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
  console.log('putRaw test completed successfully');
}

async function main() {
  const client = new DynamoClient(config);
  const nombreTabla = 'tabla2';
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

main().catch(console.error);
