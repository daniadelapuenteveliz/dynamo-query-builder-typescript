import { Config, KeySchema } from '../../types/types';
import { DynamoClient } from '../../clients/dynamo-client';
import dotenv from 'dotenv';
import { Table } from '../../table';
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
  symbol: string;
};

async function testQueryMedium(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 30 items with partition "2" and H from "00" to "29"
  // Split into batches of 25 (DynamoDB limit)
  // * * - * - * - - * * - - - - - - - - - - - - - * * * - * * *
  const ids = [0, 1, 3, 5, 8, 9, 23, 24, 25, 27, 28, 29];
  const items = [];
  for (let i = 0; i <= 29; i++) {
    let symbol = "-";
    if (ids.includes(i)) {
      symbol = "*";
    }
    items.push({
      A: "2",
      B: "b",
      C: "c",
      D: "d",
      E: "e",
      F: "f",
      G: "g",
      H: i.toString().padStart(2, '0'),
      symbol: symbol
    });
  }

  // Insert in batches of 25
  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }


  const batch1 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  ).run();

  if (batch1.items.length !== 4) {
    throw new Error('items should be 4 in batch1');
  }

  if (batch1.items[0].H !== "00" && batch1.items[0].symbol !== "*") {
    throw new Error('first item should be 00 and * in batch1');
  }
  if (batch1.items[1].H !== "01" && batch1.items[1].symbol !== "-") {
    throw new Error('second item should be 01 and - in batch1');
  }
  if (batch1.items[2].H !== "03" && batch1.items[2].symbol !== "-") {
    throw new Error('third item should be 03 and - in batch1');
  }
  if (batch1.items[3].H !== "05" && batch1.items[3].symbol !== "-") {
    throw new Error('fourth item should be 05 and - in batch1');
  }
  if (!batch1.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch1');
  }
  if (batch1.lastEvaluatedKey.H !== "05") {
    throw new Error('lastEvaluatedKey should be 05 in batch1');
  }
  if (batch1.hasNext != true) {
    throw new Error('hasNext should be true in batch1');
  }

  const batch2 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch1.lastEvaluatedKey)
    .run();

  if (batch2.items.length !== 2) {
    throw new Error('items should be 2 in batch2');
  }

  if (batch2.items[0].H !== "08" && batch2.items[0].symbol !== "*") {
    throw new Error('first item should be 08 and * in batch2');
  }
  if (batch2.items[1].H !== "09" && batch2.items[1].symbol !== "*") {
    throw new Error('second item should be 09 and * in batch2');
  }
  if (!batch2.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch2');
  }
  if (batch2.lastEvaluatedKey.H !== "09") {
    throw new Error('lastEvaluatedKey should be 09 in batch2');
  }
  if (batch2.hasNext != true) {
    throw new Error('hasNext should be true in batch2');
  }

  const batch3 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch2.lastEvaluatedKey)
    .run();

  if (batch3.items.length !== 0) {
    throw new Error('items should be 0 in batch3');
  }
  if (batch3.lastEvaluatedKey != batch3.firstEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be the same as firstEvaluatedKey in batch3');
  }
  if (!batch3.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch3');
  }
  if (batch3.lastEvaluatedKey.H !== "15") {
    throw new Error('lastEvaluatedKey should be 15 in batch3');
  }
  if (batch3.hasNext != true) {
    throw new Error('hasNext should be true in batch3');
  }
  if (batch3.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch3');
  }

  const batch4 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch3.lastEvaluatedKey)
    .run();

  if (batch4.items.length !== 0) {
    throw new Error('items should be 0 in batch3');
  }
  if (batch4.lastEvaluatedKey != batch4.firstEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be the same as firstEvaluatedKey in batch3');
  }
  if (!batch4.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch3');
  }
  if (batch4.lastEvaluatedKey.H !== "21") {
    throw new Error('lastEvaluatedKey should be 21 in batch4');
  }
  if (batch4.hasNext != true) {
    throw new Error('hasNext should be true in batch3');
  }
  if (batch4.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch3');
  }

  const batch5 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch4.lastEvaluatedKey)
    .run();

  if (batch5.items.length !== 4) {
    throw new Error('items should be 4 in batch5');
  }
  if (batch5.items[0].H !== "23" && batch5.items[0].symbol !== "*") {
    throw new Error('first item should be 23 and * in batch5');
  }
  if (batch5.items[1].H !== "24" && batch5.items[1].symbol !== "*") {
    throw new Error('second item should be 24 and * in batch5');
  }
  if (batch5.items[2].H !== "25" && batch5.items[2].symbol !== "*") {
    throw new Error('third item should be 25 and * in batch5');
  }
  if (batch5.items[3].H !== "27" && batch5.items[3].symbol !== "*") {
    throw new Error('fourth item should be 27 and * in batch5');
  }
  if (!batch5.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch5');
  }
  if (batch5.lastEvaluatedKey.H !== "27") {
    throw new Error('lastEvaluatedKey should be 27 in batch5');
  }
  if (batch5.hasNext != true) {
    throw new Error('hasNext should be true in batch5');
  }
  if (batch5.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch5');
  }

  const batch6 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortAscending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch5.lastEvaluatedKey)
    .run();
  if (batch6.items.length !== 2) {
    throw new Error('items should be 2 in batch6');
  }
  if (batch6.items[0].H !== "28" && batch6.items[0].symbol !== "*") {
    throw new Error('first item should be 28 and * in batch6');
  }
  if (batch6.items[1].H !== "29" && batch6.items[1].symbol !== "*") {
    throw new Error('second item should be 29 and * in batch6');
  }
  if (!batch6.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch6');
  }
  if (batch6.lastEvaluatedKey.H !== "29") {
    throw new Error('lastEvaluatedKey should be 29 in batch6');
  }
  if (batch6.hasNext != false) {
    throw new Error('hasNext should be false in batch6');
  }
  if (batch6.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch6');
  }
  if (!batch6.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch6');
  }

  const batch7 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortDescending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch6.firstEvaluatedKey)
    .run();
  if (batch7.items.length !== 4) {
    throw new Error('items should be 4 in batch7');
  }
  if (batch7.items[0].H !== "27" && batch7.items[0].symbol !== "*") {
    throw new Error('first item should be 27 and * in batch7');
  }
  if (batch7.items[1].H !== "25" && batch7.items[1].symbol !== "*") {
    throw new Error('second item should be 25 and * in batch7');
  }
  if (batch7.items[2].H !== "24" && batch7.items[2].symbol !== "*") {
    throw new Error('third item should be 24 and * in batch7');
  }
  if (batch7.items[3].H !== "23" && batch7.items[3].symbol !== "*") {
    throw new Error('fourth item should be 23 and * in batch7');
  }
  if (!batch7.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch7');
  }
  if (batch7.lastEvaluatedKey.H !== "23") {
    throw new Error('lastEvaluatedKey should be 23 in batch7');
  }
  if (batch7.hasNext != true) {
    throw new Error('hasNext should be true in batch7');
  }
  if (batch7.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch7');
  }
  if (!batch7.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch7');
  }
  if (batch7.firstEvaluatedKey.H !== "27") {
    throw new Error('firstEvaluatedKey should be 27 in batch7');
  }

  const batch8 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortDescending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch7.lastEvaluatedKey)
    .run();

  if (batch8.items.length !== 0) {
    throw new Error('items should be 0 in batch8');
  }
  if (batch8.lastEvaluatedKey != batch8.firstEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be the same as firstEvaluatedKey in batch8');
  }
  if (!batch8.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch8');
  }
  if (batch8.lastEvaluatedKey.H !== "17") {
    throw new Error('lastEvaluatedKey should be 17 in batch8');
  }
  if (batch8.hasNext != true) {
    throw new Error('hasNext should be true in batch8');
  }
  if (batch8.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch8');
  }

  const batch9 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortDescending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch8.lastEvaluatedKey)
    .run();

  if (batch9.items.length !== 0) {
    throw new Error('items should be 0 in batch9');
  }
  if (batch9.lastEvaluatedKey != batch9.firstEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be the same as firstEvaluatedKey in batch9');
  }
  if (!batch9.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch9');
  }
  if (batch9.lastEvaluatedKey.H !== "11") {
    throw new Error('lastEvaluatedKey should be 11 in batch9');
  }
  if (batch9.hasNext != true) {
    throw new Error('hasNext should be true in batch9');
  }
  if (batch9.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch9');
  }

  const batch10 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortDescending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch9.lastEvaluatedKey)
    .run();

  if (batch10.items.length !== 3) {
    throw new Error('items should be 3 in batch10');
  }
  if (batch10.items[0].H !== "09" && batch10.items[0].symbol !== "*") {
    throw new Error('first item should be 09 and * in batch10');
  }
  if (batch10.items[1].H !== "08" && batch10.items[1].symbol !== "*") {
    throw new Error('second item should be 08 and * in batch10');
  }
  if (batch10.items[2].H !== "05" && batch10.items[2].symbol !== "*") {
    throw new Error('third item should be 05 and * in batch10');
  }
  if (!batch10.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch10');
  }
  if (batch10.lastEvaluatedKey.H !== "05") {
    throw new Error('lastEvaluatedKey should be 05 in batch10');
  }
  if (batch10.hasNext != true) {
    throw new Error('hasNext should be true in batch10');
  }
  if (batch10.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch10');
  }

  const batch11 = await tablaDePrueba.query({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 5,
  }).sortDescending().filter(
    { 'symbol': '*' }
  )
    .pivot(batch10.lastEvaluatedKey)
    .run();
  if (batch11.items.length !== 3) {
    throw new Error('items should be 3 in batch11');
  }
  if (batch11.items[0].H !== "03" && batch11.items[0].symbol !== "*") {
    throw new Error('first item should be 03 and * in batch11');
  }
  if (batch11.items[1].H !== "01" && batch11.items[1].symbol !== "*") {
    throw new Error('second item should be 01 and * in batch11');
  }
  if (batch11.items[2].H !== "00" && batch11.items[2].symbol !== "*") {
    throw new Error('third item should be 00 and * in batch11');
  }
  if (!batch11.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch11');
  }
  if (batch11.lastEvaluatedKey.H !== "00") {
    throw new Error('lastEvaluatedKey should be 01 in batch11');
  }
  if (batch11.hasNext != false) {
    throw new Error('hasNext should be false in batch11');
  }
  if (batch11.hasPrevious != true) {
    throw new Error('hasPrevious should be true in batch11');
  }
  // Finally delete the partition
  await tablaDePrueba.deletePartition({ A: "2", B: "b", C: "c", D: "d" });

  // Assert that all items with partition "2" are deleted
  const itemsAfterDelete = await tablaDePrueba.scan({ limit: 100 }).run();
  const remainingItems = itemsAfterDelete.items.filter(item => item.A === "2" && item.B === "b" && item.C === "c" && item.D === "d");
  if (remainingItems.length !== 0) {
    throw new Error(`Items with partition "2" are not deleted. Found ${remainingItems.length} items`);
  }
}

async function main() {
  const client = new DynamoClient(config);
  const nombreTabla = 'tabla2';
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
  await testQueryMedium(tablaDePrueba);
  await tablaDePrueba.flush();
  const scanResult5 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult5.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
}

main().catch(console.error);

