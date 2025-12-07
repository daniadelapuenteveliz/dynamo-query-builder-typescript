import { Table } from '../../table';
import { KeySchema } from '../../types/types';
import {
  DescribeTableCommand,
  DescribeTableCommandOutput,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { ErrorCode } from '../../types/errors';

type PK = { pkPart: string };
type dummySk = {};
type dummyData = {};
const schema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['pkPart'],
  },
};

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

const baseDescribe: DescribeTableCommandOutput = {
  $metadata: {},
  Table: {
    TableName: 'Messages',
    ItemCount: 10,
    KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
    AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'gsi1',
        KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
        Projection: { ProjectionType: 'ALL' },
      },
    ],
  },
};

describe('Table metadata methods', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('returns basic metadata without calling DynamoDB', () => {
    const table = new Table<PK, dummySk, dummyData>(
      createClient(),
      'Messages',
      schema
    );
    expect(table.getTableName()).toBe('Messages');
    expect(table.getClient()).toBeDefined();
  });

  it('describes the table using DescribeTableCommand', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValue(baseDescribe);

    const result = await table.describe();

    expect(result).toBe(baseDescribe);
    const command = sendMock.mock.calls[0][0] as DescribeTableCommand;
    expect(command).toBeInstanceOf(DescribeTableCommand);
    expect((command as any).input.TableName).toBe('Messages');
  });

  it('retrieves the table name from DynamoDB metadata', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue(baseDescribe);

    await expect(table.getTableNameInDynamo()).resolves.toBe('Messages');
  });

  it('throws TABLE_INFO_NOT_FOUND when DescribeTable has no Table', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({ $metadata: {} });

    await expect(table.getTableNameInDynamo()).rejects.toMatchObject({
      code: ErrorCode.TABLE_INFO_NOT_FOUND,
    });
  });

  it('throws TABLE_NAME_NOT_FOUND when TableName is missing', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({
      $metadata: {},
      Table: {},
    });

    await expect(table.getTableNameInDynamo()).rejects.toMatchObject({
      code: ErrorCode.TABLE_NAME_NOT_FOUND,
    });
  });

  it('returns item count and throws when missing', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValueOnce(baseDescribe);
    await expect(table.getItemCount()).resolves.toBe(10);

    sendMock.mockResolvedValueOnce({ $metadata: {}, Table: {} });
    await expect(table.getItemCount()).rejects.toMatchObject({
      code: ErrorCode.ITEM_COUNT_NOT_FOUND,
    });
  });

  it('throws TABLE_INFO_NOT_FOUND when item count metadata lacks Table', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({ $metadata: {} });

    await expect(table.getItemCount()).rejects.toMatchObject({
      code: ErrorCode.TABLE_INFO_NOT_FOUND,
    });
  });

  it('returns key schema or throws when missing', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValueOnce(baseDescribe);
    await expect(table.getDynamoKeySchema()).resolves.toEqual(
      baseDescribe.Table?.KeySchema
    );

    sendMock.mockResolvedValueOnce({ $metadata: {}, Table: {} });
    await expect(table.getDynamoKeySchema()).rejects.toMatchObject({
      code: ErrorCode.KEY_SCHEMA_NOT_FOUND,
    });
  });

  it('throws TABLE_INFO_NOT_FOUND when key schema metadata lacks Table', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({ $metadata: {} });

    await expect(table.getDynamoKeySchema()).rejects.toMatchObject({
      code: ErrorCode.TABLE_INFO_NOT_FOUND,
    });
  });

  it('returns attribute definitions or throws when missing', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValueOnce(baseDescribe);
    await expect(table.getAttributeDefinitions()).resolves.toEqual(
      baseDescribe.Table?.AttributeDefinitions
    );

    sendMock.mockResolvedValueOnce({ $metadata: {}, Table: {} });
    await expect(table.getAttributeDefinitions()).rejects.toMatchObject({
      code: ErrorCode.ATTRIBUTE_DEFINITIONS_NOT_FOUND,
    });
  });

  it('throws TABLE_INFO_NOT_FOUND when attribute definitions metadata lacks Table', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({ $metadata: {} });

    await expect(table.getAttributeDefinitions()).rejects.toMatchObject({
      code: ErrorCode.TABLE_INFO_NOT_FOUND,
    });
  });

  it('returns global secondary indexes or throws when missing', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValueOnce(baseDescribe);
    await expect(table.getGlobalSecondaryIndexes()).resolves.toEqual(
      baseDescribe.Table?.GlobalSecondaryIndexes
    );

    sendMock.mockResolvedValueOnce({ $metadata: {}, Table: {} });
    await expect(table.getGlobalSecondaryIndexes()).rejects.toMatchObject({
      code: ErrorCode.GLOBAL_SECONDARY_INDEXES_NOT_FOUND,
    });
  });

  it('throws TABLE_INFO_NOT_FOUND when GSI metadata lacks Table', async () => {
    const client = createClient();
    const table = new Table<PK, dummySk, dummyData>(client, 'Messages', schema);
    (client.send as jest.Mock).mockResolvedValue({ $metadata: {} });

    await expect(table.getGlobalSecondaryIndexes()).rejects.toMatchObject({
      code: ErrorCode.TABLE_INFO_NOT_FOUND,
    });
  });
});
