import {
  AttributeValue,
  DynamoDBClient,
  UpdateItemCommand,
  UpdateItemCommandInput,
  UpdateItemCommandOutput,
  TransactWriteItemsCommand,
  TransactWriteItemsCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { Table } from '../../table';
import { KeySchema } from '../../types/types';
import { ErrorCode } from '../../types/errors';

jest.mock('@aws-sdk/client-dynamodb', () => {
  const actual = jest.requireActual('@aws-sdk/client-dynamodb');
  return {
    ...actual,
    PutItemCommand: jest.fn().mockImplementation(input => ({ input })),
    TransactWriteItemsCommand: jest
      .fn()
      .mockImplementation(input => ({ input })),
    UpdateItemCommand: jest.fn().mockImplementation(input => ({ input })),
  };
});

type PK = { tenantId: string; userId: string };
type SK = { sort: string };
type Data = { status: string; attempts: number };

type PKOnly = { id: string };
type EmptySK = Record<string, never>;
type DataOnly = { score: number };

const schema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['tenantId', 'userId'],
    separator: '#',
  },
  sk: {
    name: 'sk',
    keys: ['sort'],
  },
};

const schemaWithoutSk: KeySchema = {
  pk: {
    name: 'PK',
    keys: ['id'],
  },
};

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

describe('Table update helpers', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('builds UpdateItemCommandInput with pk and sk metadata', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValue({ $metadata: {} });

    await table.update(
      { tenantId: 'tenant', userId: 'user' },
      { sort: 'MSG#1' },
      { status: 'sent', attempts: 1 }
    );

    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as UpdateItemCommand;
    const input = command.input;

    expect(input).toEqual({
      TableName: 'Messages',
      ReturnValues: 'ALL_NEW',
      Key: {
        pk: { S: 'tenant#user' },
        sk: { S: 'MSG#1' },
      },
      ConditionExpression: 'attribute_exists(#pk) AND attribute_exists(#sk)',
      UpdateExpression: 'SET #status = :status, #attempts = :attempts',
      ExpressionAttributeNames: {
        '#pk': 'pk',
        '#sk': 'sk',
        '#status': 'status',
        '#attempts': 'attempts',
      },
      ExpressionAttributeValues: {
        ':status': { S: 'sent' },
        ':attempts': { N: '1' },
      },
    });
  });

  it('builds UpdateItemCommandInput for tables without sort key', async () => {
    const client = createClient();
    const table = new Table<PKOnly, EmptySK, DataOnly>(
      client,
      'Users',
      schemaWithoutSk
    );
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValue({ $metadata: {} });

    await table.update({ id: 'id#1' }, undefined as any, { score: 10 });

    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as UpdateItemCommand;
    const input = command.input;

    expect(input.Key).toEqual({ PK: { S: 'id#1' } });
    expect(input.ConditionExpression).toBe('attribute_exists(#pk)');
    expect(input.UpdateExpression).toBe('SET #score = :score');
    expect(input.ExpressionAttributeNames).toEqual({ 
      '#pk': 'PK',
      '#score': 'score' 
    });
    expect(input.ExpressionAttributeValues).toEqual({ ':score': { N: '10' } });
  });

  it('createUpdateItemInputFromRecord returns internal UpdateItemInput shape', () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const tableAny = table as any;

    const itemAsRecord: Record<string, AttributeValue> = {
      pk: { S: 'tenant#user' },
      sk: { S: 'MSG#1' },
      status: { S: 'pending' },
      attempts: { N: '0' },
    };

    const input = tableAny.createUpdateItemInputFromRecord(itemAsRecord);

    expect(input).toMatchObject({
      TableName: 'Messages',
      ReturnValues: 'ALL_NEW',
      Key: { pk: { S: 'tenant#user' }, sk: { S: 'MSG#1' } },
      ConditionExpression: 'attribute_exists(#pk) AND attribute_exists(#sk)',
      UpdateExpression: 'SET #status = :status, #attempts = :attempts',
    });
  });
});

describe('Table.update', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('sends UpdateItemCommand and returns the response', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const response = {
      $metadata: {},
      Attributes: {
        pk: { S: 'tenant#user' },
        status: { S: 'delivered' },
      },
    } as UpdateItemCommandOutput;
    sendMock.mockResolvedValue(response);

    const result = await table.update(
      { tenantId: 'tenant', userId: 'user' },
      { sort: 'MSG#1' },
      { status: 'delivered' }
    );

    expect(result).toBe(response);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as UpdateItemCommand;
    expect(command.input.UpdateExpression).toBe('SET #status = :status');
    expect(command.input.ExpressionAttributeNames).toEqual({
      '#pk': 'pk',
      '#sk': 'sk',
      '#status': 'status',
    });
    expect(command.input.ExpressionAttributeValues).toEqual({
      ':status': { S: 'delivered' },
    });
  });

  it('throws when pk is missing', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.update(
        undefined as unknown as PK,
        { sort: 'MSG#1' },
        { status: 'sent' }
      )
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  it('throws when sk is missing but schema requires it', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.update(
        { tenantId: 'tenant', userId: 'user' },
        undefined as unknown as SK,
        {
          status: 'sent',
        }
      )
    ).rejects.toMatchObject({ code: ErrorCode.SK_REQUIRED });
  });

  it('throws when new data is empty', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.update(
        { tenantId: 'tenant', userId: 'user' },
        { sort: 'MSG#1' },
        {}
      )
    ).rejects.toMatchObject({ code: ErrorCode.NEW_DATA_REQUIRED });
  });

  it('wraps conditional failures as ITEM_DOES_NOT_EXIST', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockRejectedValue(new Error('The conditional request failed'));

    await expect(
      table.update(
        { tenantId: 'tenant', userId: 'user' },
        { sort: 'MSG#1' },
        {
          status: 'sent',
        }
      )
    ).rejects.toMatchObject({ code: ErrorCode.ITEM_DOES_NOT_EXIST });
  });

  it('rethrows unexpected client errors', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const originalError = new Error('Network failure');
    (client.send as jest.Mock).mockRejectedValue(originalError);

    await expect(
      table.update(
        { tenantId: 'tenant', userId: 'user' },
        { sort: 'MSG#1' },
        {
          status: 'sent',
        }
      )
    ).rejects.toBe(originalError);
  });
});

describe('Table.updateRaw', () => {
  it('sends the provided UpdateItemCommandInput without alterations', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockResolvedValue({ $metadata: {} });

    const input: UpdateItemCommandInput = {
      TableName: 'Messages',
      Key: { pk: { S: 'key' }, sk: { S: 'sort' } },
      UpdateExpression: 'SET #name = :name',
    };

    await table.updateRaw(input);

    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as UpdateItemCommand;
    expect(command.input).toBe(input);
  });
});

describe('Table.updateBatch', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('sends TransactWriteItemsCommand with formatted update items', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const mockResponse: TransactWriteItemsCommandOutput = {
      $metadata: {},
    };
    sendMock.mockResolvedValue(mockResponse);

    const updates = [
      {
        pk: { tenantId: 'tenant1', userId: 'user1' } as PK,
        sk: { sort: 'MSG#1' } as SK,
        newData: { status: 'read' } as Partial<Data>,
      },
      {
        pk: { tenantId: 'tenant2', userId: 'user2' } as PK,
        sk: { sort: 'MSG#2' } as SK,
        newData: { attempts: 2 } as Partial<Data>,
      },
    ];

    const response = await table.updateBatch(updates);

    expect(response).toBe(mockResponse);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
    expect(command.input.TransactItems).toHaveLength(2);
    const firstUpdate = command.input.TransactItems?.[0]?.Update;
    expect((firstUpdate as any)?.TableName).toBe('Messages');
    expect((firstUpdate as any)?.ReturnValues).toBe('ALL_NEW');
  });

  it('returns null when the batch is empty', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;

    const response = await table.updateBatch([]);

    expect(response).toBeNull();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('throws when batch size exceeds 25 items', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const updates = Array.from({ length: 26 }, (_, idx) => ({
      pk: { tenantId: `tenant${idx}`, userId: `user${idx}` } as PK,
      sk: { sort: `MSG#${idx}` } as SK,
      newData: { status: 'sent' } as Partial<Data>,
    }));

    await expect(table.updateBatch(updates)).rejects.toMatchObject({
      code: ErrorCode.BATCH_LIMIT_EXCEEDED,
    });
  });

  it('throws when an item lacks pk', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.updateBatch([
        {
          pk: undefined as unknown as PK,
          sk: { sort: 'MSG#1' } as SK,
          newData: { status: 'sent' } as Partial<Data>,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  it('throws when an item lacks sk but schema requires it', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.updateBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: undefined as unknown as SK,
          newData: { status: 'sent' } as Partial<Data>,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.SK_REQUIRED });
  });

  it('throws when an item lacks newData', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.updateBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
          newData: {} as Partial<Data>,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.NEW_DATA_REQUIRED });
  });

  it('wraps conditional failures as CONDITIONAL_CHECK_FAILED', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockRejectedValue(new Error('ConditionalCheckFailed: missing'));

    await expect(
      table.updateBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
          newData: { status: 'sent' } as Partial<Data>,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.CONDITIONAL_CHECK_FAILED });
  });

  it('wraps duplicate item errors as DUPLICATE_ITEMS_IN_BATCH', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockRejectedValue(
      new Error(
        'Transaction request cannot include multiple operations on one item'
      )
    );

    await expect(
      table.updateBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
          newData: { status: 'sent' } as Partial<Data>,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.DUPLICATE_ITEMS_IN_BATCH });
  });

  it('rethrows unexpected errors from the client', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const unexpected = new Error('Generic failure');
    (client.send as jest.Mock).mockRejectedValue(unexpected);

    await expect(
      table.updateBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
          newData: { status: 'sent' } as Partial<Data>,
        },
      ])
    ).rejects.toBe(unexpected);
  });
});
