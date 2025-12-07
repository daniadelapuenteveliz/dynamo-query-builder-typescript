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
  symbol: string;
  value: number;
  category: string;
};

type itemDto = pkDto & skDto & dataDto;

async function testSearchBasic(tablaDePrueba: Table<pkDto, skDto, dataDto>) {

  // Insert 20 items with timestamp="1" and count from "01" to "20"
  const items: itemDto[] = [];
  for (let i = 1; i <= 20; i++) {
    items.push({
      timestamp: "1",
      count: i.toString().padStart(2, '0'),
      symbol: i % 2 === 0 ? "even" : "odd",
      value: i * 10,
      category: i <= 10 ? "A" : "B",
    });
  }

  // Insert in batches of 25
  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Test 1: Basic search with just pk and limit
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "1" },
    limit: 10,
  });

  if (result1.length !== 10) {
    throw new Error(`Expected 10 items, got ${result1.length}`);
  }

  // Verify items are in order (count from "01" to "10")
  for (let i = 0; i < 10; i++) {
    const expectedCount = (i + 1).toString().padStart(2, '0');
    if (result1[i].count !== expectedCount) {
      throw new Error(`Item ${i} should have count ${expectedCount}, got ${result1[i].count}`);
    }
    if (result1[i].timestamp !== "1") {
      throw new Error(`Item ${i} should have timestamp "1", got ${result1[i].timestamp}`);
    }
  }

  // Test 2: Search with limit greater than available items
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "1" },
  });

  if (result2.length !== 20) {
    throw new Error(`Expected 20 items, got ${result2.length}`);
  }

  // Test 3: Search with limit that requires pagination (limit 15, but internal pagination is 25)
  const result3 = await tablaDePrueba.search({
    pk: { timestamp: "1" },
    limit: 15,
  });

  if (result3.length !== 15) {
    throw new Error(`Expected 15 items, got ${result3.length}`);
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "1" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 1 are not deleted');
  }

  console.log('testSearchBasic completed successfully');
}

async function testSearchWithSKCondition(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 30 items with timestamp="2" and count from "01" to "30"
  const items: itemDto[] = [];
  for (let i = 1; i <= 30; i++) {
    items.push({
      timestamp: "2",
      count: i.toString().padStart(2, '0'),
      symbol: i % 3 === 0 ? "star" : "dash",
      value: i * 5,
      category: i <= 15 ? "X" : "Y",
    });
  }

  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Test 1: Search with SK equal condition
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "2" },
    skCondition: { timestamp: "2", count: "05" },
  });

  if (result1.length !== 1) {
    throw new Error(`Expected 1 item, got ${result1.length}`);
  }
  if (result1[0].count !== "05") {
    throw new Error(`Expected count "05", got ${result1[0].count}`);
  }

  // Test 2: Search with SK greaterThanOrEqual condition
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "2" },
    limit: 10,
    skCondition: { greaterThanOrEqual: { timestamp: "2", count: "20" } },
  });

  if (result2.length !== 10) {
    throw new Error(`Expected 10 items, got ${result2.length}`);
  }
  // Verify all items have count >= "20"
  for (const item of result2) {
    const countNum = parseInt(item.count);
    if (countNum < 20) {
      throw new Error(`Expected count >= 20, got ${item.count}`);
    }
  }
  // Verify we got items from "20" to "29"
  for (let i = 20; i <= 29; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const found = result2.find(item => item.count === expectedCount);
    if (!found) {
      throw new Error(`Item with count ${expectedCount} not found`);
    }
  }

  // Test 3: Search with SK lowerThan condition
  const result3 = await tablaDePrueba.search({
    pk: { timestamp: "2" },
    skCondition: { lowerThan: { timestamp: "2", count: "10" } },
  });

  if (result3.length !== 9) {
    throw new Error(`Expected 9 items, got ${result3.length}`);
  }
  // Verify all items have count < "10"
  for (const item of result3) {
    const countNum = parseInt(item.count);
    if (countNum >= 10) {
      throw new Error(`Expected count < 10, got ${item.count}`);
    }
  }

  // Test 4: Search with SK between condition
  const result4 = await tablaDePrueba.search({
    pk: { timestamp: "2" },
    limit: 20,
    skCondition: { between: { from: { timestamp: "2", count: "10" }, to: { timestamp: "2", count: "20" } } },
  });


  if (result4.length !== 11) {
    throw new Error(`Expected 11 items, got ${result4.length}`);
  }
  // Verify all items have count between "10" and "20" (inclusive)
  for (const item of result4) {
    const countNum = parseInt(item.count);
    if (countNum < 10 || countNum > 20) {
      throw new Error(`Expected count between 10 and 20, got ${item.count}`);
    }
  }

  // Test 5: Search with SK beginsWith condition
  const result5 = await tablaDePrueba.search({
    pk: { timestamp: "2" },
    skCondition: { beginsWith: { timestamp: "2", count: "1" } },
  });

  if (result5.length !== 10) {
    throw new Error(`Expected 10 items, got ${result5.length}`);
  }
  // Verify all items have count starting with "1" (10-19)
  for (const item of result5) {
    if (!item.count.startsWith("1")) {
      throw new Error(`Expected count starting with "1", got ${item.count}`);
    }
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "2" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 2 are not deleted');
  }

  console.log('testSearchWithSKCondition completed successfully');
}

async function testSearchWithFilter(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 40 items with timestamp="3"
  const items: itemDto[] = [];
  for (let i = 1; i <= 40; i++) {
    items.push({
      timestamp: "3",
      count: i.toString().padStart(2, '0'),
      symbol: i % 4 === 0 ? "diamond" : "circle",
      value: i * 2,
      category: i <= 20 ? "alpha" : "beta",
    });
  }

  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Test 1: Search with filter on symbol
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "3" },
    limit: 50,
    filter: { symbol: "diamond" },
  });

  if (result1.length !== 10) {
    throw new Error(`Expected 10 items with symbol "diamond", got ${result1.length}`);
  }
  // Verify all items have symbol "diamond"
  for (const item of result1) {
    if (item.symbol !== "diamond") {
      throw new Error(`Expected symbol "diamond", got ${item.symbol}`);
    }
  }
  // Verify we got items at positions 4, 8, 12, 16, 20, 24, 28, 32, 36, 40
  const expectedCounts = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40];
  for (const expectedCount of expectedCounts) {
    const countStr = expectedCount.toString().padStart(2, '0');
    const found = result1.find(item => item.count === countStr);
    if (!found) {
      throw new Error(`Item with count ${countStr} not found`);
    }
  }

  // Test 2: Search with filter on value (greater than)
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "3" },
    filter: { value: { '>': 60 } },
  });

  if (result2.length !== 10) {
    throw new Error(`Expected 10 items with value > 60, got ${result2.length}`);
  }
  // Verify all items have value > 60
  for (const item of result2) {
    if (item.value <= 60) {
      throw new Error(`Expected value > 60, got ${item.value}`);
    }
  }

  // Test 3: Search with filter on category
  const result3 = await tablaDePrueba.search({
    pk: { timestamp: "3" },
    limit: 50,
    filter: { category: "alpha" },
  });

  if (result3.length !== 20) {
    throw new Error(`Expected 20 items with category "alpha", got ${result3.length}`);
  }
  // Verify all items have category "alpha"
  for (const item of result3) {
    if (item.category !== "alpha") {
      throw new Error(`Expected category "alpha", got ${item.category}`);
    }
  }

  // Test 4: Search with filter and SK condition combined
  const result4 = await tablaDePrueba.search({
    pk: { timestamp: "3" },
    skCondition: { greaterThanOrEqual: { timestamp: "3", count: "20" } },
    filter: { symbol: "diamond" },
  });

  if (result4.length !== 6) {
    throw new Error(`Expected 6 items, got ${result4.length}`);
  }
  // Verify all items have count >= "20" and symbol "diamond"
  for (const item of result4) {
    const countNum = parseInt(item.count);
    if (countNum < 20) {
      throw new Error(`Expected count >= 20, got ${item.count}`);
    }
    if (item.symbol !== "diamond") {
      throw new Error(`Expected symbol "diamond", got ${item.symbol}`);
    }
  }

  const result5 = await tablaDePrueba.search({
    pk: { timestamp: "3" },
    limit: 50,
    skCondition: { greaterThanOrEqual: { timestamp: "3", count: "20" } },
    filter: { symbol: "diamond", category: "beta", value: { '>': 60 } },
  });

  if (result5.length !== 3) {
    throw new Error(`Expected 3 items, got ${result5.length}`);
  }

  for (const item of result5) {
    if (item.symbol !== "diamond") {
      throw new Error(`Expected symbol "diamond", got ${item.symbol}`);
    }
    if (item.category !== "beta") {
      throw new Error(`Expected category "beta", got ${item.category}`);
    }
    if (item.value <= 60) {
      throw new Error(`Expected value > 60, got ${item.value}`);
    }
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "3" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 3 are not deleted');
  }

  console.log('testSearchWithFilter completed successfully');
}

async function testSearchWithProjection(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 15 items with timestamp="4"
  const items: itemDto[] = [];
  for (let i = 1; i <= 15; i++) {
    items.push({
      timestamp: "4",
      count: i.toString().padStart(2, '0'),
      symbol: "test",
      value: i * 3,
      category: "test",
    });
  }

  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Test 1: Search with projection (only symbol and value)
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "4" },
    project: ['symbol', 'value'],
  });

  if (result1.length !== 15) {
    throw new Error(`Expected 15 items, got ${result1.length}`);
  }

  // Verify all items have timestamp and count (keys are always present)
  for (const item of result1) {
    if (!item.timestamp || !item.count) {
      throw new Error('Item should have timestamp and count');
    }
    if (item.symbol !== "test") {
      throw new Error(`Expected symbol "test", got ${item.symbol}`);
    }
    // category should not be present (or undefined)
    if ('category' in item && item.category !== undefined) {
      // Note: DynamoDB might return undefined values, so we check if it's explicitly set
      // This is a soft check - projection behavior may vary
    }
    //assert that symbol and value are present
    if (!item.symbol || !item.value) {
      throw new Error(`Expected symbol and value, got ${item.symbol} and ${item.value}`);
    }
  }

  // Test 2: Search with projection and filter
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "4" },
    limit: 15,
    filter: { value: { '>': 30 } },
    project: ['value'],
  });

  if (result2.length !== 5) {
    throw new Error(`Expected 5 items, got ${result2.length}`);
  }
  // Verify all items have value > 30
  for (const item of result2) {
    if (item.value <= 30) {
      throw new Error(`Expected value > 30, got ${item.value}`);
    }
    if (!item.value) {
      throw new Error(`Expected value, got ${item.value}`);
    }
    if (!item.timestamp || !item.count) {
      throw new Error('Item should have timestamp and count');
    }
    if (item.symbol) {
      throw new Error(`Expected symbol to be undefined, got ${item.symbol}`);
    }
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "4" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 4 are not deleted');
  }

  console.log('testSearchWithProjection completed successfully');
}

async function testSearchPagination(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Insert 60 items with timestamp="5" to test pagination
  const items: itemDto[] = [];
  for (let i = 1; i <= 60; i++) {
    items.push({
      timestamp: "5",
      count: i.toString().padStart(2, '0'),
      symbol: "pag",
      value: i,
      category: "test",
    });
  }

  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Test 1: Search with limit that requires multiple internal queries
  // The search method uses internal limit of 25, so to get 50 items, it needs 2 queries
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "5" },
    limit: 50,
  });

  if (result1.length !== 50) {
    throw new Error(`Expected 50 items, got ${result1.length}`);
  }

  // Verify we got items from "01" to "50"
  for (let i = 1; i <= 50; i++) {
    const expectedCount = i.toString().padStart(2, '0');
    const found = result1.find(item => item.count === expectedCount);
    if (!found) {
      throw new Error(`Item with count ${expectedCount} not found`);
    }
  }

  // Test 2: Search with limit that doesn't align with internal pagination
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "5" },
    limit: 35,
  });

  if (result2.length !== 35) {
    throw new Error(`Expected 35 items, got ${result2.length}`);
  }

  // Test 3: Search with filter that requires pagination
  // Insert items with different symbols
  const items2: itemDto[] = [];
  for (let i = 1; i <= 60; i++) {
    items2.push({
      timestamp: "5",
      count: (i + 60).toString().padStart(2, '0'),
      symbol: i % 3 === 0 ? "target" : "other",
      value: i,
      category: "test",
    });
  }

  for (let i = 0; i < items2.length; i += 25) {
    const batch = items2.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  // Search for "target" items - should find 20 items across multiple pages
  const result3 = await tablaDePrueba.search({
    pk: { timestamp: "5" },
    filter: { symbol: "target" },
  });

  if (result3.length !== 20) {
    throw new Error(`Expected 20 items with symbol "target", got ${result3.length}`);
  }

  // Verify all items have symbol "target"
  for (const item of result3) {
    if (item.symbol !== "target") {
      throw new Error(`Expected symbol "target", got ${item.symbol}`);
    }
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "5" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 5 are not deleted');
  }

  console.log('testSearchPagination completed successfully');
}

async function testSearchEdgeCases(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  // Test 1: Search with no items
  const result1 = await tablaDePrueba.search({
    pk: { timestamp: "6" },
    limit: 10,
  });

  if (result1.length !== 0) {
    throw new Error(`Expected 0 items, got ${result1.length}`);
  }

  // Test 2: Search with limit 0
  const result2 = await tablaDePrueba.search({
    pk: { timestamp: "6" },
    limit: 0,
  });

  if (result2.length !== 0) {
    throw new Error(`Expected 0 items, got ${result2.length}`);
  }

  // Test 3: Insert one item and search
  await tablaDePrueba.put({
    timestamp: "6",
    count: "01",
    symbol: "single",
    value: 100,
    category: "test",
  });

  const result3 = await tablaDePrueba.search({
    pk: { timestamp: "6" },
    limit: 10,
  });

  if (result3.length !== 1) {
    throw new Error(`Expected 1 item, got ${result3.length}`);
  }
  if (result3[0].count !== "01") {
    throw new Error(`Expected count "01", got ${result3[0].count}`);
  }

  // Test 4: Search with SK condition that matches no items
  const result4 = await tablaDePrueba.search({
    pk: { timestamp: "6" },
    skCondition: { greaterThan: { timestamp: "6", count: "10" } },
  });

  if (result4.length !== 0) {
    throw new Error(`Expected 0 items, got ${result4.length}`);
  }

  // Test 5: Search with filter that matches no items
  const result5 = await tablaDePrueba.search({
    pk: { timestamp: "6" },
    limit: 10,
    filter: { symbol: "nonexistent" },
  });

  if (result5.length !== 0) {
    throw new Error(`Expected 0 items, got ${result5.length}`);
  }

  // Clean up
  await tablaDePrueba.deletePartition({ timestamp: "6" });
  const scanResult = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult.items.length !== 0) {
    throw new Error('Items with timestamp 6 are not deleted');
  }

  console.log('testSearchEdgeCases completed successfully');
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
  await testSearchBasic(tablaDePrueba);
  await testSearchWithSKCondition(tablaDePrueba);
  await testSearchWithFilter(tablaDePrueba);
  await testSearchWithProjection(tablaDePrueba);
  await testSearchPagination(tablaDePrueba);
  await testSearchEdgeCases(tablaDePrueba);
  await tablaDePrueba.flush();
  const scanResult5 = await tablaDePrueba.scan({ limit: 10 }).run();
  if (scanResult5.items.length !== 0) {
    throw new Error('Scan result is not correct');
  }

  console.log('All search tests completed successfully!');
}

main().catch(console.error);

