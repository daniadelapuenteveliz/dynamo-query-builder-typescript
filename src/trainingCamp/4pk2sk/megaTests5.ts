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
  status: string;
  priority: number;
  value: number;
  active: boolean;
};

type itemDto = pkDto & skDto & dataDto;

async function insert1000Records(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  console.log('Inserting 1000 records...');
  const items: itemDto[] = [];

  // Create 1000 items with different partitions, statuses, priorities, etc.
  // Partitions: "1", "2", "3", "4" (250 items each)
  // H: "001" to "250" for each partition
  // Status: active, inactive, pending (mixed)
  // Priority: 1-10 (distributed)
  // Value: 1-1000 (sequential)
  // Active: true/false (mixed)

  for (let i = 0; i < 1000; i++) {
    const partitionIndex = Math.floor(i / 250);
    const A = (partitionIndex + 1).toString(); // "1", "2", "3", "4"
    const H = ((i % 250) + 1).toString().padStart(3, '0'); // "001" to "250"

    const statuses = ['active', 'inactive', 'pending'];
    const status = statuses[i % 3];

    const priority = (i % 10) + 1;
    const value = i + 1;
    const active = i % 2 === 0;

    items.push({
      A,
      B: 'b',
      C: 'c',
      D: 'd',
      E: 'e',
      F: 'f',
      G: 'g',
      H,
      status,
      priority,
      value,
      active,
    });
  }

  // Insert in batches of 25
  for (let i = 0; i < items.length; i += 25) {
    const batch = items.slice(i, i + 25);
    await tablaDePrueba.putBatch(batch);
  }

  console.log('1000 records inserted successfully');
  return items;
}

async function testDeleteBySKEqual(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 1: Delete by SK equal ===');

  // Verify items exist before deletion
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { equal: { E: 'e', F: 'f', G: 'g', H: '001' } },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      `Expected to find 1 item with partition=1 and H=001 before deletion, got ${beforeSearch.length}`
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=1 and H=001
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { equal: { E: 'e', F: 'f', G: 'g', H: '001' } },
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone after deletion
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { equal: { E: 'e', F: 'f', G: 'g', H: '001' } },
    limit: 10,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteBySKGreaterThan(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 2: Delete by SK greaterThan ===');

  // Verify items exist (partition=1, H > 200)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThan: { E: 'e', F: 'f', G: 'g', H: '200' } },
  });

  const expectedCount = 50; // H values 201-250
  if (beforeSearch.length < expectedCount) {
    throw new Error(
      `Expected at least ${expectedCount} items before deletion, got ${beforeSearch.length}`
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=1 and H > 200
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThan: { E: 'e', F: 'f', G: 'g', H: '200' } },
    limit: 100,
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThan: { E: 'e', F: 'f', G: 'g', H: '200' } },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteBySKLowerThan(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 3: Delete by SK lowerThan ===');

  // Verify items exist (partition=2, H < 50)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    skCondition: { lowerThan: { E: 'e', F: 'f', G: 'g', H: '050' } },
  });

  const expectedCount = 49; // H values 001-049
  if (beforeSearch.length < expectedCount) {
    throw new Error(
      `Expected at least ${expectedCount} items before deletion, got ${beforeSearch.length}`
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=2 and H < 50
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    skCondition: { lowerThan: { E: 'e', F: 'f', G: 'g', H: '050' } },
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    skCondition: { lowerThan: { E: 'e', F: 'f', G: 'g', H: '050' } },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteBySKBetween(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 4: Delete by SK between ===');

  // Verify items exist (partition=3, H between 100 and 150)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: {
      between: {
        from: { E: 'e', F: 'f', G: 'g', H: '100' },
        to: { E: 'e', F: 'f', G: 'g', H: '150' },
      },
    },
  });

  const expectedCount = 51; // H values 100-150
  if (beforeSearch.length < expectedCount) {
    throw new Error(
      `Expected at least ${expectedCount} items before deletion, got ${beforeSearch.length}`
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=3 and H between 100 and 150
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: {
      between: {
        from: { E: 'e', F: 'f', G: 'g', H: '100' },
        to: { E: 'e', F: 'f', G: 'g', H: '150' },
      },
    },
    limit: 100,
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: {
      between: {
        from: { E: 'e', F: 'f', G: 'g', H: '100' },
        to: { E: 'e', F: 'f', G: 'g', H: '150' },
      },
    },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteByFilter(tablaDePrueba: Table<pkDto, skDto, dataDto>) {
  console.log('\n=== Test 5: Delete by filter (status) ===');

  // Verify items exist (partition=4, status='inactive')
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'inactive' },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      'Expected to find items with status=inactive before deletion'
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=4 and status='inactive'
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'inactive' },
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'inactive' },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteByFilterWithOperator(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log(
    '\n=== Test 6: Delete by filter with operator (priority >= 8) ==='
  );

  // Verify items exist (partition=1, priority >= 8)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: { priority: { '>=': 8 } },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      'Expected to find items with priority >= 8 before deletion'
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=1 and priority >= 8
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: { priority: { '>=': 8 } },
    limit: 200,
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: { priority: { '>=': 8 } },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteByFilterBoolean(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 7: Delete by filter (boolean active=false) ===');

  // Verify items exist (partition=2, active=false)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { active: false },
  });

  if (beforeSearch.length === 0) {
    throw new Error('Expected to find items with active=false before deletion');
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=2 and active=false
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { active: false },
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { active: false },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteBySKAndFilter(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 8: Delete by SK condition AND filter ===');

  // Verify items exist (partition=3, H >= 200, status='pending')
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThanOrEqual: { E: 'e', F: 'f', G: 'g', H: '200' } },
    filter: { status: 'pending' },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      'Expected to find items with H >= 200 and status=pending before deletion'
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=3, H >= 200, and status='pending'
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThanOrEqual: { E: 'e', F: 'f', G: 'g', H: '200' } },
    filter: { status: 'pending' },
    limit: 100,
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '3', B: 'b', C: 'c', D: 'd' },
    skCondition: { greaterThanOrEqual: { E: 'e', F: 'f', G: 'g', H: '200' } },
    filter: { status: 'pending' },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteBySKBeginsWith(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 9: Delete by SK beginsWith ===');

  // Verify items exist (partition=4, H begins with '2')
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    skCondition: { beginsWith: { E: 'e', F: 'f', G: 'g', H: '002' } },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      'Expected to find items with H beginning with "2" before deletion'
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=4 and H begins with '2'
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    skCondition: { beginsWith: { E: 'e', F: 'f', G: 'g', H: '002' } },
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '4', B: 'b', C: 'c', D: 'd' },
    skCondition: { beginsWith: { E: 'e', F: 'f', G: 'g', H: '002' } },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteByMultipleFilters(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log(
    '\n=== Test 10: Delete by multiple filters (status AND priority) ==='
  );

  // Verify items exist (partition=1, status='active', priority < 5)
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: {
      status: 'active',
      priority: { '<': 5 },
    },
  });

  if (beforeSearch.length === 0) {
    throw new Error(
      'Expected to find items with status=active and priority < 5 before deletion'
    );
  }
  console.log(`Found ${beforeSearch.length} items before deletion`);

  // Delete items with partition=1, status='active', and priority < 5
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: {
      status: 'active',
      priority: { '<': 5 },
    },
    limit: 200,
  });

  console.log(`Deleted ${deleted} items`);

  // Verify items are gone
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '1', B: 'b', C: 'c', D: 'd' },
    filter: {
      status: 'active',
      priority: { '<': 5 },
    },
    limit: 100,
  });

  if (afterSearch.length !== 0) {
    throw new Error(
      `Expected 0 items after deletion, got ${afterSearch.length}`
    );
  }
  console.log('✓ Items successfully deleted and verified');
}

async function testDeleteWithLimit(
  tablaDePrueba: Table<pkDto, skDto, dataDto>
) {
  console.log('\n=== Test 11: Delete with limit (partial deletion) ===');

  // Verify items exist (partition=2, status='pending')
  const beforeSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'pending' },
  });

  const totalBefore = beforeSearch.length;
  if (totalBefore === 0) {
    throw new Error(
      'Expected to find items with status=pending before deletion'
    );
  }
  console.log(`Found ${totalBefore} items before deletion`);

  // Delete only 10 items with partition=2 and status='pending'
  const deleted = await tablaDePrueba.deleteWithCondition({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'pending' },
    limit: 10,
  });

  if (deleted !== 10) {
    throw new Error(`Expected to delete 10 items, deleted ${deleted}`);
  }
  console.log(`Deleted ${deleted} items`);

  // Verify only 10 items were deleted (should have totalBefore - 10 remaining)
  const afterSearch = await tablaDePrueba.search({
    pk: { A: '2', B: 'b', C: 'c', D: 'd' },
    filter: { status: 'pending' },
  });

  const expectedRemaining = totalBefore - 10;
  if (afterSearch.length !== expectedRemaining) {
    throw new Error(
      `Expected ${expectedRemaining} items remaining, got ${afterSearch.length}`
    );
  }
  console.log(`✓ Correctly deleted 10 items, ${afterSearch.length} remaining`);
}

async function main() {
  const client = new DynamoClient(config);
  const nombreTabla = 'tabla2';
  const tablaDePrueba = client.table<pkDto, skDto, dataDto>(
    nombreTabla,
    keySchema
  );

  try {
    await tablaDePrueba.flush();
    const scanResult4 = await tablaDePrueba.scan({ limit: 10 }).run();
    if (scanResult4.items.length !== 0) {
      throw new Error('Scan result is not correct');
    }
    // Insert 1000 records
    await insert1000Records(tablaDePrueba);

    // Test different deletion scenarios
    await testDeleteBySKEqual(tablaDePrueba);
    await testDeleteBySKAndFilter(tablaDePrueba);
    await testDeleteBySKBeginsWith(tablaDePrueba);
    await testDeleteBySKGreaterThan(tablaDePrueba);
    await testDeleteBySKLowerThan(tablaDePrueba);
    await testDeleteBySKBetween(tablaDePrueba);
    await testDeleteByFilter(tablaDePrueba);
    await testDeleteByFilterWithOperator(tablaDePrueba);
    await testDeleteByFilterBoolean(tablaDePrueba);
    await testDeleteByMultipleFilters(tablaDePrueba);
    await testDeleteWithLimit(tablaDePrueba);
    await tablaDePrueba.flush();
    const scanResult5 = await tablaDePrueba.scan({ limit: 10 }).run();
    if (scanResult5.items.length !== 0) {
      throw new Error('Scan result is not correct');
    }
    console.log('\n✅ All delete tests completed successfully!');
  } catch (error) {
    console.error('❌ Test failed:', error);
    throw error;
  }
}

main().catch(console.error);
