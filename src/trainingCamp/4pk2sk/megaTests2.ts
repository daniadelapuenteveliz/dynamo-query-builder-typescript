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

type dataDto = {};

async function testQuerySimple(tablaDePrueba: Table<pkDto, skDto, dataDto>) {

  await tablaDePrueba.putBatch([
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "01" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "02" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "03" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "04" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "05" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "06" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "07" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "08" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "09" },
    { A: "1", B: "b", C: "c", D: "d", E: "e", F: "f", G: "g", H: "10" },
  ]);

  for (let i = 1; i <= 5; i++) {
    const item = await tablaDePrueba.getOne(
      { A: "1", B: "b", C: "c", D: "d" },
      { E: "e", F: "f", G: "g", H: `0${i}` }
    );
    if (!item) {
      throw new Error(`Item ${i} does not exist`);
    }
  }

  const items2 = await tablaDePrueba.query({
    pk: { A: "1", B: "b", C: "c", D: "d" },
    limit: 10
  }).whereSKGreaterThanOrEqual({ E: "e", F: "f", G: "g", H: "06" }).run();
  for (let i = 6; i <= 9; i++) {
    const item = items2.items.find(item => item.H === `0${i}`);
    if (!item) {
      throw new Error(`Item ${i} does not exist`);
    }
  }
  if (!items2.items.find(item => item.H === "10")) {
    throw new Error('Item 10 should exist');
  }

  //now delete all with partition "1"
  await tablaDePrueba.deletePartition({ A: "1", B: "b", C: "c", D: "d" });

  //assert that all items with partition "1" are deleted
  const items = await tablaDePrueba.scan({ limit: 10 }).run();
  if (items.items.length !== 0) {
    throw new Error('Items with partition "1" are not deleted');
  }
}

async function testQueryMedium(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 25 items with partition "2" and H from "01" to "25"
  // Split into batches of 25 (DynamoDB limit)
  const items = [];
  for (let i = 1; i <= 25; i++) {
    items.push({
      A: "2",
      B: "b",
      C: "c",
      D: "d",
      E: "e",
      F: "f",
      G: "g",
      H: i.toString().padStart(2, '0'), // "01", "02", ..., "25"
    });
  }

  // Insert in batches of 25
  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Get the first 7 items using getPartitionBatch
  const firstBatch = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 7,
  });
  // Test that we got items from "01" to "07"
  if (firstBatch.items.length !== 7) {
    throw new Error(`Expected 7 items, got ${firstBatch.items.length}`);
  }
  for (let i = 1; i <= 7; i++) {
    const expectedH = i.toString().padStart(2, '0');
    const item = firstBatch.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in first batch`);
    }
  }

  // Test that lastEvaluatedKey is "07" (A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="07")
  if (!firstBatch.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null');
  }
  if (firstBatch.lastEvaluatedKey.A !== "2" || firstBatch.lastEvaluatedKey.B !== "b" || firstBatch.lastEvaluatedKey.C !== "c" || firstBatch.lastEvaluatedKey.D !== "d" || firstBatch.lastEvaluatedKey.E !== "e" || firstBatch.lastEvaluatedKey.F !== "f" || firstBatch.lastEvaluatedKey.G !== "g" || firstBatch.lastEvaluatedKey.H !== "07") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="07", got A=${firstBatch.lastEvaluatedKey.A}, H=${firstBatch.lastEvaluatedKey.H}`);
  }

  // Test that firstEvaluatedKey is "01"
  if (!firstBatch.firstEvaluatedKey || firstBatch.firstEvaluatedKey.A !== "2" || firstBatch.firstEvaluatedKey.B !== "b" || firstBatch.firstEvaluatedKey.C !== "c" || firstBatch.firstEvaluatedKey.D !== "d" || firstBatch.firstEvaluatedKey.E !== "e" || firstBatch.firstEvaluatedKey.F !== "f" || firstBatch.firstEvaluatedKey.G !== "g" || firstBatch.firstEvaluatedKey?.H !== "01") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="01", got ${JSON.stringify(firstBatch.firstEvaluatedKey)}`);
  }

  if (firstBatch.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (firstBatch.hasPrevious != false) {
    throw new Error('hasPrevious should be false');
  }

  // Use the lastEvaluatedKey as pivot to get the next 7 items
  const secondBatch = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = secondBatch.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in second batch`);
    }
  }

  // Test that lastEvaluatedKey is "14"
  if (!secondBatch.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in second batch');
  }
  if (secondBatch.lastEvaluatedKey.A !== "2" || secondBatch.lastEvaluatedKey.B !== "b" || secondBatch.lastEvaluatedKey.C !== "c" || secondBatch.lastEvaluatedKey.D !== "d" || secondBatch.lastEvaluatedKey.E !== "e" || secondBatch.lastEvaluatedKey.F !== "f" || secondBatch.lastEvaluatedKey.G !== "g" || secondBatch.lastEvaluatedKey.H !== "14") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="14", got A=${secondBatch.lastEvaluatedKey.A}, H=${secondBatch.lastEvaluatedKey.H}`);
  }

  if (!secondBatch.firstEvaluatedKey || secondBatch.firstEvaluatedKey?.A !== "2" || secondBatch.firstEvaluatedKey?.B !== "b" || secondBatch.firstEvaluatedKey?.C !== "c" || secondBatch.firstEvaluatedKey?.D !== "d" || secondBatch.firstEvaluatedKey?.E !== "e" || secondBatch.firstEvaluatedKey?.F !== "f" || secondBatch.firstEvaluatedKey?.G !== "g" || secondBatch.firstEvaluatedKey?.H !== "08") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="08", got ${JSON.stringify(secondBatch.firstEvaluatedKey)}`);
  }

  if (secondBatch.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (secondBatch.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch3 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = batch3.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in batch3`);
    }
  }

  if (!batch3.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch3');
  }
  if (batch3.lastEvaluatedKey.A !== "2" || batch3.lastEvaluatedKey.B !== "b" || batch3.lastEvaluatedKey.C !== "c" || batch3.lastEvaluatedKey.D !== "d" || batch3.lastEvaluatedKey.E !== "e" || batch3.lastEvaluatedKey.F !== "f" || batch3.lastEvaluatedKey.G !== "g" || batch3.lastEvaluatedKey.H !== "21") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="21", got A=${batch3.lastEvaluatedKey.A}, H=${batch3.lastEvaluatedKey.H}`);
  }
  if (!batch3.firstEvaluatedKey || batch3.firstEvaluatedKey.A !== "2" || batch3.firstEvaluatedKey.B !== "b" || batch3.firstEvaluatedKey.C !== "c" || batch3.firstEvaluatedKey.D !== "d" || batch3.firstEvaluatedKey.E !== "e" || batch3.firstEvaluatedKey.F !== "f" || batch3.firstEvaluatedKey.G !== "g" || batch3.firstEvaluatedKey?.H !== "15") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="15", got ${JSON.stringify(batch3.firstEvaluatedKey)}`);
  }

  if (batch3.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (batch3.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch4 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = batch4.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in batch4`);
    }
  }

  if (!batch4.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch4');
  }
  if (batch4.lastEvaluatedKey.A !== "2" || batch4.lastEvaluatedKey.B !== "b" || batch4.lastEvaluatedKey.C !== "c" || batch4.lastEvaluatedKey.D !== "d" || batch4.lastEvaluatedKey.E !== "e" || batch4.lastEvaluatedKey.F !== "f" || batch4.lastEvaluatedKey.G !== "g" || batch4.lastEvaluatedKey.H !== "08") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="08", got A=${batch4.lastEvaluatedKey.A}, H=${batch4.lastEvaluatedKey.H}`);
  }
  if (!batch4.firstEvaluatedKey || batch4.firstEvaluatedKey.A !== "2" || batch4.firstEvaluatedKey.B !== "b" || batch4.firstEvaluatedKey.C !== "c" || batch4.firstEvaluatedKey.D !== "d" || batch4.firstEvaluatedKey.E !== "e" || batch4.firstEvaluatedKey.F !== "f" || batch4.firstEvaluatedKey.G !== "g" || batch4.firstEvaluatedKey?.H !== "14") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="14", got ${JSON.stringify(batch4.firstEvaluatedKey)}`);
  }

  if (batch4.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (batch4.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch5 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = batch5.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in batch5`);
    }
  }

  if (!batch5.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch5');
  }
  if (batch5.lastEvaluatedKey.A !== "2" || batch5.lastEvaluatedKey.B !== "b" || batch5.lastEvaluatedKey.C !== "c" || batch5.lastEvaluatedKey.D !== "d" || batch5.lastEvaluatedKey.E !== "e" || batch5.lastEvaluatedKey.F !== "f" || batch5.lastEvaluatedKey.G !== "g" || batch5.lastEvaluatedKey.H !== "01") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="01", got A=${batch5.lastEvaluatedKey.A}, H=${batch5.lastEvaluatedKey.H}`);
  }
  if (!batch5.firstEvaluatedKey || batch5.firstEvaluatedKey.A !== "2" || batch5.firstEvaluatedKey.B !== "b" || batch5.firstEvaluatedKey.C !== "c" || batch5.firstEvaluatedKey.D !== "d" || batch5.firstEvaluatedKey.E !== "e" || batch5.firstEvaluatedKey.F !== "f" || batch5.firstEvaluatedKey.G !== "g" || batch5.firstEvaluatedKey?.H !== "07") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="07", got ${JSON.stringify(batch5.firstEvaluatedKey)}`);
  }

  if (batch5.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch5.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch6 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = batch6.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in batch6`);
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
  if (batch6.lastEvaluatedKey.A !== "2" || batch6.lastEvaluatedKey.B !== "b" || batch6.lastEvaluatedKey.C !== "c" || batch6.lastEvaluatedKey.D !== "d" || batch6.lastEvaluatedKey.E !== "e" || batch6.lastEvaluatedKey.F !== "f" || batch6.lastEvaluatedKey.G !== "g" || batch6.lastEvaluatedKey.H !== "25") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="25", got A=${batch6.lastEvaluatedKey.A}, H=${batch6.lastEvaluatedKey.H}`);
  }
  if (!batch6.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch6');
  }
  if (batch6.firstEvaluatedKey.A !== "2" || batch6.firstEvaluatedKey.B !== "b" || batch6.firstEvaluatedKey.C !== "c" || batch6.firstEvaluatedKey.D !== "d" || batch6.firstEvaluatedKey.E !== "e" || batch6.firstEvaluatedKey.F !== "f" || batch6.firstEvaluatedKey.G !== "g" || batch6.firstEvaluatedKey.H !== "22") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="22", got A=${batch6.firstEvaluatedKey.A}, H=${batch6.firstEvaluatedKey.H}`);
  }

  const batch7 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    throw new Error('hasPrevious should be false');
  }
  if (batch7.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be null in batch7');
  }
  if (batch7.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should be null in batch7');
  }

  const batch8 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    const expectedH = i.toString().padStart(2, '0');
    const item = batch8.items.find(item => item.H === expectedH);
    if (!item) {
      throw new Error(`Item with H ${expectedH} not found in batch8`);
    }
  }

  if (!batch8.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch8');
  }
  if (batch8.lastEvaluatedKey.A !== "2" || batch8.lastEvaluatedKey.B !== "b" || batch8.lastEvaluatedKey.C !== "c" || batch8.lastEvaluatedKey.D !== "d" || batch8.lastEvaluatedKey.E !== "e" || batch8.lastEvaluatedKey.F !== "f" || batch8.lastEvaluatedKey.G !== "g" || batch8.lastEvaluatedKey.H !== "02") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="02", got A=${batch8.lastEvaluatedKey.A}, H=${batch8.lastEvaluatedKey.H}`);
  }
  if (!batch8.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch8');
  }
  if (batch8.firstEvaluatedKey.A !== "2" || batch8.firstEvaluatedKey.B !== "b" || batch8.firstEvaluatedKey.C !== "c" || batch8.firstEvaluatedKey.D !== "d" || batch8.firstEvaluatedKey.E !== "e" || batch8.firstEvaluatedKey.F !== "f" || batch8.firstEvaluatedKey.G !== "g" || batch8.firstEvaluatedKey.H !== "21") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="21", got A=${batch8.firstEvaluatedKey.A}, H=${batch8.firstEvaluatedKey.H}`);
  }

  if (batch8.hasNext != true) {
    throw new Error('hasNext should be true');
  }
  if (batch8.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch9 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
    limit: 20,
    pagination: {
      pivot: batch8.lastEvaluatedKey,
      direction: 'backward',
    },
  });

  if (batch9.items.length !== 1) {
    throw new Error(`Expected 1 item in batch9, got ${batch9.items.length}`);
  }
  if (batch9.items[0].H !== "01") {
    throw new Error(`Item with H "01" not found in batch9`);
  }
  if (!batch9.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should not be null in batch9');
  }
  if (batch9.lastEvaluatedKey.A !== "2" || batch9.lastEvaluatedKey.B !== "b" || batch9.lastEvaluatedKey.C !== "c" || batch9.lastEvaluatedKey.D !== "d" || batch9.lastEvaluatedKey.E !== "e" || batch9.lastEvaluatedKey.F !== "f" || batch9.lastEvaluatedKey.G !== "g" || batch9.lastEvaluatedKey.H !== "01") {
    throw new Error(`lastEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="01", got A=${batch9.lastEvaluatedKey.A}, H=${batch9.lastEvaluatedKey.H}`);
  }
  if (!batch9.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should not be null in batch9');
  }
  if (batch9.firstEvaluatedKey.A !== "2" || batch9.firstEvaluatedKey.B !== "b" || batch9.firstEvaluatedKey.C !== "c" || batch9.firstEvaluatedKey.D !== "d" || batch9.firstEvaluatedKey.E !== "e" || batch9.firstEvaluatedKey.F !== "f" || batch9.firstEvaluatedKey.G !== "g" || batch9.firstEvaluatedKey.H !== "01") {
    throw new Error(`firstEvaluatedKey should be A="2", B="b", C="c", D="d", E="e", F="f", G="g", H="01", got A=${batch9.firstEvaluatedKey.A}, H=${batch9.firstEvaluatedKey.H}`);
  }
  if (batch9.hasNext != false) {
    throw new Error('hasNext should be false');
  }
  if (batch9.hasPrevious != true) {
    throw new Error('hasPrevious should be true');
  }

  const batch10 = await tablaDePrueba.getPartitionBatch({
    pk: { A: "2", B: "b", C: "c", D: "d" },
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
    throw new Error('hasPrevious should be false');
  }
  if (batch10.lastEvaluatedKey) {
    throw new Error('lastEvaluatedKey should be null in batch10');
  }
  if (batch10.firstEvaluatedKey) {
    throw new Error('firstEvaluatedKey should be null in batch10');
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
  await testQuerySimple(tablaDePrueba);
  await testQueryMedium(tablaDePrueba);
  await tablaDePrueba.flush();
  const scanResult5 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult5.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }
}

main().catch(console.error);

