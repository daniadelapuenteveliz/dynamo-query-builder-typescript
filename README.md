# dynamo-query-builder

This library designed for managing objects stored in Amazon DynamoDB, laveragin partition key (PK) and sort key (SK) architecture for efficient data organization and querying.

## Connect DynamoDB

First instance a `DynamoClient`. AWS credentials, can be provided in two ways:

### 1: Explicit IAM Credentials
Pass credentials directly in the configuration
```typescript
import { DynamoClient, Config } from 'dynamo-query-builder';

const config: Config = {
  region: 'YOUR_REGION',
  credentials: {
    accessKeyId: 'YOUR_ACCESS_KEY_ID',
    secretAccessKey: 'YOUR_SECRET_ACCESS_KEY',
  },
};

const client = new DynamoClient(config);
```

### 2: Inherited Credentials
Use the default AWS credential provider chain.

```typescript
const config: Config = {};
const client = new DynamoClient(config);
//or
const client2 = new DynamoClient();
```

## DynamoDB key in dynamo-query-builder

DynamoDB uses two types of keys (must be strings in the current version of the library):

- PK (Partition Key) — a hash key
- SK (Sort Key) — a range key

A common pattern in Dynamo design is to build these keys by concatenating multiple attributes that share a logical relationship or hierarchy.

Example:

```typescript
type message = {
  sender_id: string;
  channel: 'whatsapp' | 'mail' | 'sms';
  receiver_id: string;
  timestamp: string;
  metadata: {
    [key: string]: any;
  };
  attachment_urls: string[];
};
```

- A message always belongs to a sender and a channel → good candidates for the PK.
- The receiver gets the message at a particular timestamp → good candidates for the SK.

Then, a consistent key structure could be:
```typescript
PK = sender_id#channel
SK = receiver_id#timestamp
```

Important technicla notes: 
- Any key can be written as A#B#C....
- It is recommended to separate only the SK, since PK queries are not efficient (you would rely on scans with filters, which should be avoided unless strictly necessary).
- SK queries are always prefix-based. You cannot query only C; you must include A#B#C. This is why maintaining a clear hierarchy (A > B > C…) is important.



## KeySchema
A KeySchema, defines how your PK and SK (optional) are constructed and represented in DynamoDB. Both PK and SK share the same structure:

- name: Actual key name on DynamoDB
- keys: Ordered string list representing the A#B#C... notation.
- separator: string used to join the key components (# as default).

Example:
```typescript
import { KeySchema } from 'dynamo-query-builder';

const keySchema: KeySchema = {
  pk: {
    name: 'sender_channel',
    keys: ['sender_id', 'channel'],
    separator: '#',
  },
  sk: {
    name: 'receiver_timestamp',
    keys: ['receiver_id', 'timestamp'],
    separator: '#',
  },
  preserve: ['sender_id', 'channel', 'receiver_id'], 
};
```
Note: preserve lets you store parts of a composite key as separate attributes.
If SK = A#B#C and you set preserve: ['B'], then B is also stored as its own DynamoDB attribute.

## Typing

Defining PK, SK, and data types ensures the library can store, infer, and validate your items correctly. This also helps maintain strong typing across your application.

Example:
```typescript
// PK as DTO
type MessagePK = {
  sender_id: string;
  channel: 'whatsapp' | 'mail' | 'sms';
};

// SK as DTO
type MessageSK = {
  receiver_id: string;
  timestamp: string;
};

// Rest of the data
type MessageData = {
  message_text: string;
  metadata: {
    [key: string]: any;
  };
  attachment_urls: string[];
};

// Complete Item DTO
type MessageDto = MessagePK & MessageSK & MessageData;
```

## Complete Example

```typescript
import { DynamoClient, Config, KeySchema, Table } from 'dynamo-query-builder';

const config: Config = {
  region: 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
};

const client = new DynamoClient(config);

type MessagePK = {
  sender_id: string;
  channel: 'whatsapp' | 'mail' | 'sms';
};

type MessageSK = {
  receiver_id: string;
  timestamp: string;
};

type MessageData = {
  message_text: string;
  metadata: {
    [key: string]: any;
  };
  attachment_urls: string[];
};

type MessageDto = MessagePK & MessageSK & MessageData;

const keySchema: KeySchema = {
  pk: {
    name: 'sender_channel',
    keys: ['sender_id', 'channel'],
    separator: '#',
    type: 'string',
  },
  sk: {
    name: 'receiver_timestamp',
    keys: ['receiver_id', 'timestamp'],
    separator: '#',
    type: 'string',
  },
  preserve: ['sender_id', 'channel', 'receiver_id'],
};

const messageTable: Table<MessagePK, MessageSK, MessageData> = 
  client.table<MessagePK, MessageSK, MessageData>('messages', keySchema);
// Now the table is ready to be used. 
```

## How to use it

### Create Operations

#### `put(item, override?)`

Inserts single item. Fails if item already exist unless `override=true`. inserted item must contain PK and SK attributes.

**Example:**
```typescript
await messageTable.put({
  sender_id: 'user123',
  channel: 'whatsapp',
  receiver_id: 'user456',
  timestamp: '2024-01-01T00:00:00Z',
  message_text: 'Hello!',
  metadata: {},
  attachment_urls: []
});
```
#### `putBatch(items, override?)`

Performs an atomic batch insert of multiple items (up to 25) If any item fails, the entire transaction is rolled back. inserted items must contain PK and SK attributes.

**Example:**
```typescript
const messages = [m1,m2];
await messageTable.putBatch(messages);
```

---

### Update Operations

#### `update(pk, sk, newData)`

Updates specific attributes of an existing item. Only the fields provided in `newData` will be updated; other attributes remain unchanged. The item must exist. updated item must contain PK and SK attributes. Key attributes can't be updated.

**Example:**
```typescript
await messageTable.update(
  { sender_id: 'user123', channel: 'whatsapp' },
  { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' },
  { message_text: 'Updated message text' }
);
```

#### `updateBatch(updates)`

Performs an atomic batch update of multiple items (up to 25). updated items must contain PK and SK attributes.

**Example:**
```typescript
await messageTable.updateBatch([
  {
    pk1,
    sk1,
    newData: { message_text: 'Updated 1' }
  },
  {
    pk2,
    sk2,
    newData: { message_text: 'Updated 2' }
  }
]);
```
---

### Delete Operations

#### `delete(pk, sk)`
Deletes a single item by its partition key and sort key.

**Example:**
```typescript
await messageTable.delete(
  { sender_id: 'user123', channel: 'whatsapp' },
  { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' }
);
```

#### `deletePartition(pk)`

Deletes all items that share the same partition key. This method queries the partition in batches of 25 and deletes items until the partition is empty.

**Warning:** This operation may take a while for large partitions.

**Example:**
```typescript
await messageTable.deletePartition({ 
  sender_id: 'user123', 
  channel: 'whatsapp' 
});
```

**Limitations:**
- Can be slow for partitions with many items.
- No progress tracking or cancellation support.
- Not atomic - partial deletions possible if operation is interrupted.

#### `deleteBatch(deletes)`

Performs an atomic batch delete of multiple items (up to 25).

**Example:**
```typescript
await messageTable.deleteBatch([
  {pk1,sk1},
  {pk2,sk2}
]);
```
---

#### `deleteWithCondition(deleteParams)`

Deletes items matching specific conditions. This method queries items by PK (and optional SK conditions), applies filters, and deletes matching items in batches until a limit is reached or all matching items are deleted.

**Parameters:**
- `deleteParams: SearchParams<PK, SK, DataDto>` - Search parameters:
  - `pk: PK` - Partition key (required)
  - `skCondition?: SKCondition<SK>` - Optional SK condition (equal, greaterThan, beginsWith, between, etc.)
  - `filter?: FilterObject<DataDto>` - Optional filter on data attributes
  - `limit?: number` - Maximum number of items to delete
  - `IndexName?: string` - Optional GSI/LSI to query

**Example:**
```typescript
// Delete all messages from a sender in a date range
const deleted = await messageTable.deleteWithCondition({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  skCondition: {
    between: {
      from: { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' },
      to: { receiver_id: 'user456', timestamp: '2024-01-31T23:59:59Z' }
    }
  },
  filter: { message_text: 'spam' }, // Only delete spam messages
  limit: 100 // Delete at most 100 items
});
```

**Limitations:**
- Processes items in batches of 25 internally
- Can be slow for large result sets
- Not atomic across batches - partial deletions possible if interrupted


#### `flush()`

Deletes **all items** from the table. This method scans the entire table and deletes items in batches. 

**Warning:** This is a destructive operation that cannot be undone!

**Example:**
```typescript
const deletedCount = await messageTable.flush();
```

**Limitations:**
- **Extremely slow** for large tables (scans entire table)
- Not atomic - partial deletions possible if operation is interrupted
- **No confirmation or safety checks** - use with extreme caution
- Processes in batches of 25 internally

---

### Read Operations

#### `getOne(pk, sk, IndexName?)`

Retrieves a single by PK and SK. Throws an error if the item is not found.

**Example:**
```typescript
const message = await messageTable.getOne(
  { sender_id: 'user123', channel: 'whatsapp' },
  { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' }
);

// Using a Global Secondary Index
const message = await messageTable.getOne(
  { sender_id: 'user123', channel: 'whatsapp' },
  { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' },
  'GSI1'
);
```

#### `getPartitionBatch(qparams)`

Retrieves all items in a partition (items sharing the same PK). Supports pagination, sorting, and projection.

**Parameters:**
- `qparams: QueryParams<PK, DataDto>` - Query parameters:
  - `pk: PK` - Partition key (required)
  - `limit: number` - Maximum number of items to return
  - `pagination?: Pagination` - Pagination options:
    - `pivot?: KeyRec` - Last evaluated key from previous query
    - `direction?: 'forward' | 'backward'` - Sort direction
  - `project?: (keyof DataDto)[]` - Array of attribute names to return
  - `IndexName?: string` - Optional GSI/LSI name

**Returns:** `Promise<paginationResult>` - Object containing:
  - `items: ItemOf<PK, SK, DataDto>[]` - Array of items
  - `lastEvaluatedKey?: KeyRec` - Key for pagination
  - `hasNext: boolean` - Whether more items exist
  - `count: number` - Number of items returned

**Example:**
```typescript
// Get first 50 items in a partition
const result = await messageTable.getPartitionBatch({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  limit: 50
});

// Paginate through results
let lastKey = result.lastEvaluatedKey;
while (result.hasNext) {
  const nextResult = await messageTable.getPartitionBatch({
    pk: { sender_id: 'user123', channel: 'whatsapp' },
    limit: 50,
    pagination: { pivot: lastKey, direction: 'forward' }
  });
  lastKey = nextResult.lastEvaluatedKey;
}

// Project only specific attributes
const result = await messageTable.getPartitionBatch({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  limit: 50,
  project: ['message_text', 'timestamp'] // Only return these fields
});
```

**Limitations:**
- Default limit is 50 if not specified
- Returns items sorted ascending by default
- Maximum 1MB of data per query (DynamoDB limit)
- Projection reduces returned data but doesn't reduce read capacity units consumed


#### `search(searchParams)`

Searches for items matching specific conditions. This method queries by PK (and optional SK conditions), applies filters, and automatically paginates through all matching results up to an optional limit.

**Parameters:**
- `searchParams: SearchParams<PK, SK, DataDto>` - Search parameters:
  - `pk: PK` - Partition key (required)
  - `skCondition?: SKCondition<SK>` - Optional SK condition (equal, greaterThan, beginsWith, between, etc.)
  - `filter?: FilterObject<DataDto>` - Optional filter on data attributes
  - `limit?: number` - Maximum number of items to return
  - `project?: (keyof DataDto)[]` - Array of attribute names to return
  - `IndexName?: string` - Optional GSI/LSI name
  - `pagination?: Pagination` - Pagination options

**Returns:** `Promise<ItemOf<PK, SK, DataDto>[]>` - Array of all matching items (up to limit)

**Example:**
```typescript
// Search for messages in a date range
const messages = await messageTable.search({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  skCondition: {
    beginsWith: { receiver_id: 'user456' }
  },
  filter: { message_text: 'urgent' },
  limit: 100
});

// Search with SK between condition
const messages = await messageTable.search({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  skCondition: {
    between: {
      from: { receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' },
      to: { receiver_id: 'user456', timestamp: '2024-01-31T23:59:59Z' }
    }
  }
});
```

**Limitations:**
- Processes items in batches of 25 internally
- Can be slow for large result sets
- Filter expressions are applied after query (less efficient than key conditions)
- If limit is not provided, returns all matching items (may be slow/expensive)
- Maximum 1MB of data per query batch (DynamoDB limit)

### Query

Creates a Query builder instance for constructing complex queries. Returns a `Query` object that supports method chaining for filtering, sorting, and pagination.

**Parameters:**
- `qparams: QueryParams<PK, DataDto>` - Query parameters:
  - `pk: PK` - Partition key (required)
  - `limit: number` - Maximum number of items per query
  - `project?: (keyof DataDto)[]` - Array of attribute names to return
  - `IndexName?: string` - Optional GSI/LSI name

**Returns:** `Query<PK, SK, DataDto>` - Query builder instance

**Example:**
```typescript
// Basic query
const query = messageTable.query({
  pk: { sender_id: 'user123', channel: 'whatsapp' },
  limit: 50
});

// Chain query methods
const result = await messageTable
  .query({ pk: { sender_id: 'user123', channel: 'whatsapp' }, limit: 50 })
  .whereSKequal({ receiver_id: 'user456', timestamp: '2024-01-01T00:00:00Z' })
  .filter({ message_text: 'hello' })
  .sortAscending()
  .run();

// Query with SK begins with
const result = await messageTable
  .query({ pk: { sender_id: 'user123', channel: 'whatsapp' }, limit: 50 })
  .whereSKBeginsWith({ receiver_id: 'user456' })
  .run();
```

### Scan

Creates a Scan builder instance for scanning the entire table. Returns a `Scan` object that supports method chaining for filtering and pagination.

**Parameters:**
- `sparams: ScanParams<DataDto>` - Scan parameters:
  - `limit: number` - Maximum number of items per scan
  - `project?: (keyof DataDto)[]` - Array of attribute names to return
  - `IndexName?: string` - Optional GSI/LSI name

**Example:**
```typescript
// Basic scan
const scan = messageTable.scan({ limit: 100 });

// Chain scan methods
const result = await messageTable
  .scan({ limit: 100 })
  .filter({ message_text: 'spam' })
  .run();

// Scan with pagination
let lastKey;
do {
  const result = await messageTable
    .scan({ limit: 100 })
    .pivot(lastKey)
    .run();
  lastKey = result.lastEvaluatedKey;
} while (result.hasNext);
```

**Limitations:**
- **Expensive operation** - scans entire table (or index)
- **Slow** for large tables
- Returns a Scan builder, not results (must call `.run()` to execute)
- Filter expressions are applied after scan (consumes full read capacity)
- Maximum 1MB of data per scan (DynamoDB limit)
- **Avoid scans when possible** - use queries with PK instead

### Raw methods

Raw methods provide direct access to AWS SDK command inputs, bypassing the library's automatic formatting, validation, and type safety features. 
- `putRaw()` - Accepts `PutItemCommandInput` from `@aws-sdk/client-dynamodb`
- `updateRaw()` - Accepts `UpdateItemCommandInput` from `@aws-sdk/client-dynamodb`
- `deleteRaw()` - Accepts `DeleteItemCommandInput` from `@aws-sdk/client-dynamodb`
- `queryRaw()` - Accepts `CommandInput` (custom type for Query operations)
- `scanRaw()` - Accepts `CommandInput` (custom type for Scan operations)

---
### Metadata and Utility Operations

- `getTableName()` - Returns the table name used by this table instance.
- `getClient()` - Returns the underlying AWS DynamoDB client instance.
- `getTableNameInDynamo()`- Retrieves the actual table name from DynamoDB by calling `DescribeTable`.
- `getItemCount()` - Retrieves the approximate item count for the table from DynamoDB metadata.
- `getDynamoKeySchema()` - Retrieves the DynamoDB key schema (PK and SK definitions) from the table metadata.
- `getAttributeDefinitions()` - Retrieves the attribute definitions from the table metadata.
- `getGlobalSecondaryIndexes()` - Retrieves the Global Secondary Indexes (GSI) definitions from the table metadata.
- `describe()` - Retrieves the complete table description from DynamoDB (includes all metadata: schema, indexes, throughput, etc.).
- `getKeySchema()` -Returns the KeySchema configuration used by this table instance (the schema provided during table creation).

## What You CANNOT Do (for now...)
- Change Metadata: Indexes (GSI, LSI), Payment method and Throughput settings.
- Cannot Delete Tables
- Non string Keys: Numeric or binary for the PK and SK

## Future Improvements
- Download table to CSV
- Bulk import from CSV
- locking support (mutex)
- Enhanced connection options