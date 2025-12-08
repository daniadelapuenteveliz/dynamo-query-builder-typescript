import {
  AttributeValue,
  DynamoDBClient,
  DeleteItemCommand,
  DeleteItemCommandInput,
  DeleteItemCommandOutput,
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
    DeleteItemCommand: jest.fn().mockImplementation(input => ({ input })),
    TransactWriteItemsCommand: jest
      .fn()
      .mockImplementation(input => ({ input })),
  };
});

type PK = { tenantId: string; userId: string };
type SK = { sort: string };
type Data = { message: string };

type PKOnly = { id: string };
type EmptySK = Record<string, never>;

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

describe('Table delete helpers', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('createDeleteItemInputFromRecord', () => {
    it('builds DeleteItemInput with pk and sk metadata', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const tableAny = table as any;

      const itemAsRecord: Record<string, AttributeValue> = {
        pk: { S: 'tenant#user' },
        sk: { S: 'MSG#1' },
        message: { S: 'hello' },
      };

      const input = tableAny.createDeleteItemInputFromRecord(itemAsRecord);

      expect(input).toEqual({
        TableName: 'Messages',
        Key: {
          pk: { S: 'tenant#user' },
          sk: { S: 'MSG#1' },
        },
      });
    });

    it('builds DeleteItemInput for tables without sort key', () => {
      const client = createClient();
      const table = new Table<PKOnly, EmptySK, Data>(
        client,
        'Users',
        schemaWithoutSk
      );
      const tableAny = table as any;

      const itemAsRecord: Record<string, AttributeValue> = {
        PK: { S: 'id#1' },
        message: { S: 'test' },
      };

      const input = tableAny.createDeleteItemInputFromRecord(itemAsRecord);

      expect(input.Key).toEqual({ PK: { S: 'id#1' } });
      expect(input.TableName).toBe('Users');
      expect(input.Key).not.toHaveProperty('sk');
    });
  });

  describe('createDeleteItemCommandInputFromRecord', () => {
    it('builds DeleteItemCommandInput from DeleteItemInput', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockResolvedValue({ $metadata: {} });

      await table.delete(
        { tenantId: 'tenant', userId: 'user' },
        { sort: 'MSG#1' }
      );

      expect(sendMock).toHaveBeenCalledTimes(1);
      const command = sendMock.mock.calls[0][0] as DeleteItemCommand;
      const input = command.input;

      expect(input).toEqual({
        TableName: 'Messages',
        Key: {
          pk: { S: 'tenant#user' },
          sk: { S: 'MSG#1' },
        },
      });
    });

    it('builds DeleteItemCommandInput for tables without sort key', async () => {
      const client = createClient();
      const table = new Table<PKOnly, EmptySK, Data>(
        client,
        'Users',
        schemaWithoutSk
      );
      const sendMock = client.send as jest.Mock;
      sendMock.mockResolvedValue({ $metadata: {} });

      await table.delete({ id: 'id#1' }, undefined as any);

      expect(sendMock).toHaveBeenCalledTimes(1);
      const command = sendMock.mock.calls[0][0] as DeleteItemCommand;
      const input = command.input;

      expect(input.Key).toEqual({ PK: { S: 'id#1' } });
      expect(input.TableName).toBe('Users');
    });
  });
});

describe('Table.delete', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('sends DeleteItemCommand and returns the response', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const response = {
      $metadata: {},
      Attributes: {
        pk: { S: 'tenant#user' },
        sk: { S: 'MSG#1' },
      },
    } as DeleteItemCommandOutput;
    sendMock.mockResolvedValue(response);

    const result = await table.delete(
      { tenantId: 'tenant', userId: 'user' },
      { sort: 'MSG#1' }
    );

    expect(result).toBe(response);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as DeleteItemCommand;
    expect(command.input.TableName).toBe('Messages');
    expect(command.input.Key).toEqual({
      pk: { S: 'tenant#user' },
      sk: { S: 'MSG#1' },
    });
  });

  it('throws when pk is missing', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.delete(undefined as unknown as PK, { sort: 'MSG#1' })
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  it('throws when sk is missing but schema requires it', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.delete(
        { tenantId: 'tenant', userId: 'user' },
        undefined as unknown as SK
      )
    ).rejects.toMatchObject({ code: ErrorCode.SK_REQUIRED });
  });

  it('wraps conditional failures as ITEM_DOES_NOT_EXIST', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    sendMock.mockRejectedValue(new Error('The conditional request failed'));

    await expect(
      table.delete({ tenantId: 'tenant', userId: 'user' }, { sort: 'MSG#1' })
    ).rejects.toMatchObject({ code: ErrorCode.ITEM_DOES_NOT_EXIST });
  });

  it('rethrows unexpected client errors', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const originalError = new Error('Network failure');
    (client.send as jest.Mock).mockRejectedValue(originalError);

    await expect(
      table.delete({ tenantId: 'tenant', userId: 'user' }, { sort: 'MSG#1' })
    ).rejects.toBe(originalError);
  });

  it('works correctly for tables without sort key', async () => {
    const client = createClient();
    const table = new Table<PKOnly, EmptySK, Data>(
      client,
      'Users',
      schemaWithoutSk
    );
    const sendMock = client.send as jest.Mock;
    const response = {
      $metadata: {},
      Attributes: { PK: { S: 'id#1' } },
    } as DeleteItemCommandOutput;
    sendMock.mockResolvedValue(response);

    const result = await table.delete(
      { id: 'id#1' },
      undefined as unknown as EmptySK
    );

    expect(result).toBe(response);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as DeleteItemCommand;
    expect(command.input.TableName).toBe('Users');
    expect(command.input.Key).toEqual({ PK: { S: 'id#1' } });
    expect(command.input.Key).not.toHaveProperty('sk');
  });
});

describe('Table.deleteRaw', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('sends the provided DeleteItemCommandInput without alterations', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const response = {
      $metadata: {},
      Attributes: { pk: { S: 'key' } },
    } as DeleteItemCommandOutput;
    sendMock.mockResolvedValue(response);

    const input: DeleteItemCommandInput = {
      TableName: 'Messages',
      Key: { pk: { S: 'tenant#user' }, sk: { S: 'MSG#1' } },
    };

    const result = await table.deleteRaw(input);

    expect(result).toBe(response);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as DeleteItemCommand;
    expect(command.input).toBe(input);
  });
});

describe('Table.deleteBatch', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('sends TransactWriteItemsCommand with formatted delete items', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const mockResponse: TransactWriteItemsCommandOutput = {
      $metadata: {},
    };
    sendMock.mockResolvedValue(mockResponse);

    const deletes = [
      {
        pk: { tenantId: 'tenant1', userId: 'user1' } as PK,
        sk: { sort: 'MSG#1' } as SK,
      },
      {
        pk: { tenantId: 'tenant2', userId: 'user2' } as PK,
        sk: { sort: 'MSG#2' } as SK,
      },
    ];

    const response = await table.deleteBatch(deletes);

    expect(response).toBe(mockResponse);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
    expect(command.input.TransactItems).toHaveLength(2);
    const firstDelete = command.input.TransactItems?.[0]?.Delete;
    expect((firstDelete as any)?.TableName).toBe('Messages');
    expect((firstDelete as any)?.Key).toEqual({
      pk: { S: 'tenant1#user1' },
      sk: { S: 'MSG#1' },
    });
  });

  it('returns null when the batch is empty', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;

    const response = await table.deleteBatch([]);

    expect(response).toBeNull();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('throws when batch size exceeds 25 items', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const deletes = Array.from({ length: 26 }, (_, idx) => ({
      pk: { tenantId: `tenant${idx}`, userId: `user${idx}` } as PK,
      sk: { sort: `MSG#${idx}` } as SK,
    }));

    await expect(table.deleteBatch(deletes)).rejects.toMatchObject({
      code: ErrorCode.BATCH_LIMIT_EXCEEDED,
    });
  });

  it('throws when an item lacks pk', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.deleteBatch([
        {
          pk: undefined as unknown as PK,
          sk: { sort: 'MSG#1' } as SK,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  it('throws when an item lacks sk but schema requires it', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.deleteBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: undefined as unknown as SK,
        },
      ])
    ).rejects.toMatchObject({ code: ErrorCode.SK_REQUIRED });
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
      table.deleteBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
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
      table.deleteBatch([
        {
          pk: { tenantId: 'tenant', userId: 'user' } as PK,
          sk: { sort: 'MSG#1' } as SK,
        },
      ])
    ).rejects.toBe(unexpected);
  });

  it('handles batch with single item', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const mockResponse: TransactWriteItemsCommandOutput = {
      $metadata: {},
    };
    sendMock.mockResolvedValue(mockResponse);

    const deletes = [
      {
        pk: { tenantId: 'tenant', userId: 'user' } as PK,
        sk: { sort: 'MSG#1' } as SK,
      },
    ];

    const response = await table.deleteBatch(deletes);

    expect(response).toBe(mockResponse);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
    expect(command.input.TransactItems).toHaveLength(1);
  });

  it('handles batch with maximum allowed items (25)', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);
    const sendMock = client.send as jest.Mock;
    const mockResponse: TransactWriteItemsCommandOutput = {
      $metadata: {},
    };
    sendMock.mockResolvedValue(mockResponse);

    const deletes = Array.from({ length: 25 }, (_, idx) => ({
      pk: { tenantId: `tenant${idx}`, userId: `user${idx}` } as PK,
      sk: { sort: `MSG#${idx}` } as SK,
    }));

    const response = await table.deleteBatch(deletes);

    expect(response).toBe(mockResponse);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
    expect(command.input.TransactItems).toHaveLength(25);
  });

  it('works correctly for tables without sort key', async () => {
    const client = createClient();
    const table = new Table<PKOnly, EmptySK, Data>(
      client,
      'Users',
      schemaWithoutSk
    );
    const sendMock = client.send as jest.Mock;
    const mockResponse: TransactWriteItemsCommandOutput = {
      $metadata: {},
    };
    sendMock.mockResolvedValue(mockResponse);

    const deletes = [
      {
        pk: { id: 'id#1' } as PKOnly,
        sk: undefined as unknown as EmptySK,
      },
      {
        pk: { id: 'id#2' } as PKOnly,
        sk: undefined as unknown as EmptySK,
      },
    ];

    const response = await table.deleteBatch(deletes);

    expect(response).toBe(mockResponse);
    expect(sendMock).toHaveBeenCalledTimes(1);
    const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
    expect(command.input.TransactItems).toHaveLength(2);
    const firstDelete = command.input.TransactItems?.[0]?.Delete;
    expect((firstDelete as any)?.TableName).toBe('Users');
    expect((firstDelete as any)?.Key).toEqual({ PK: { S: 'id#1' } });
    expect((firstDelete as any)?.Key).not.toHaveProperty('sk');
  });
});

describe('Error handling in delete operations', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('handleDeleteItemError', () => {
    it('converts conditional request failure to ITEM_DOES_NOT_EXIST', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockRejectedValue(new Error('The conditional request failed'));

      await expect(
        table.delete({ tenantId: 'tenant', userId: 'user' }, { sort: 'MSG#1' })
      ).rejects.toMatchObject({
        code: ErrorCode.ITEM_DOES_NOT_EXIST,
      });
    });

    it('rethrows non-conditional errors', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      const networkError = new Error('Network timeout');
      sendMock.mockRejectedValue(networkError);

      await expect(
        table.delete({ tenantId: 'tenant', userId: 'user' }, { sort: 'MSG#1' })
      ).rejects.toBe(networkError);
    });
  });

  describe('handleDeleteItemBatchError', () => {
    it('converts duplicate items error to DUPLICATE_ITEMS_IN_BATCH', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockRejectedValue(
        new Error(
          'Transaction request cannot include multiple operations on one item'
        )
      );

      await expect(
        table.deleteBatch([
          {
            pk: { tenantId: 'tenant', userId: 'user' } as PK,
            sk: { sort: 'MSG#1' } as SK,
          },
        ])
      ).rejects.toMatchObject({
        code: ErrorCode.DUPLICATE_ITEMS_IN_BATCH,
      });
    });

    it('rethrows non-duplicate errors', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      const networkError = new Error('Network timeout');
      sendMock.mockRejectedValue(networkError);

      await expect(
        table.deleteBatch([
          {
            pk: { tenantId: 'tenant', userId: 'user' } as PK,
            sk: { sort: 'MSG#1' } as SK,
          },
        ])
      ).rejects.toBe(networkError);
    });
  });
});

describe('Table.deletePartition', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('deletes all items in a partition with multiple items', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    // Mock getPartitionBatch to return items in batches
    const mockItems1 = [
      {
        tenantId: 'tenant1',
        userId: 'user1',
        sort: 'MSG#1',
        message: 'message1',
      },
      {
        tenantId: 'tenant1',
        userId: 'user1',
        sort: 'MSG#2',
        message: 'message2',
      },
    ];
    const mockItems2 = [
      {
        tenantId: 'tenant1',
        userId: 'user1',
        sort: 'MSG#3',
        message: 'message3',
      },
    ];

    let getPartitionBatchCallCount = 0;
    jest.spyOn(table, 'getPartitionBatch').mockImplementation(async () => {
      getPartitionBatchCallCount++;
      if (getPartitionBatchCallCount === 1) {
        return {
          items: mockItems1 as any,
          lastEvaluatedKey: { sort: 'MSG#2' },
          firstEvaluatedKey: undefined,
          count: 2,
          hasNext: true,
          hasPrevious: false,
          direction: 'forward' as const,
        };
      } else {
        return {
          items: mockItems2 as any,
          lastEvaluatedKey: undefined,
          firstEvaluatedKey: undefined,
          count: 1,
          hasNext: false,
          hasPrevious: false,
          direction: 'forward' as const,
        };
      }
    });

    const deleteBatchSpy = jest
      .spyOn(table, 'deleteBatch')
      .mockResolvedValue({ $metadata: {} } as any);

    await table.deletePartition({ tenantId: 'tenant1', userId: 'user1' });

    expect(getPartitionBatchCallCount).toBe(2);
    expect(deleteBatchSpy).toHaveBeenCalledTimes(2);
    expect(deleteBatchSpy).toHaveBeenNthCalledWith(1, [
      {
        pk: { tenantId: 'tenant1', userId: 'user1' },
        sk: { sort: 'MSG#1' },
      },
      {
        pk: { tenantId: 'tenant1', userId: 'user1' },
        sk: { sort: 'MSG#2' },
      },
    ]);
    expect(deleteBatchSpy).toHaveBeenNthCalledWith(2, [
      {
        pk: { tenantId: 'tenant1', userId: 'user1' },
        sk: { sort: 'MSG#3' },
      },
    ]);
  });

  it('deletes partition with single item', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    jest.spyOn(table, 'getPartitionBatch').mockResolvedValue({
      items: [
        {
          tenantId: 'tenant1',
          userId: 'user1',
          sort: 'MSG#1',
          message: 'message1',
        },
      ] as any,
      lastEvaluatedKey: undefined,
      firstEvaluatedKey: undefined,
      count: 1,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });

    const deleteBatchSpy = jest
      .spyOn(table, 'deleteBatch')
      .mockResolvedValue({ $metadata: {} } as any);

    await table.deletePartition({ tenantId: 'tenant1', userId: 'user1' });

    expect(deleteBatchSpy).toHaveBeenCalledTimes(1);
    expect(deleteBatchSpy).toHaveBeenCalledWith([
      {
        pk: { tenantId: 'tenant1', userId: 'user1' },
        sk: { sort: 'MSG#1' },
      },
    ]);
  });

  it('handles empty partition', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    jest.spyOn(table, 'getPartitionBatch').mockResolvedValue({
      items: [],
      lastEvaluatedKey: undefined,
      firstEvaluatedKey: undefined,
      count: 0,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });

    const deleteBatchSpy = jest
      .spyOn(table, 'deleteBatch')
      .mockResolvedValue({ $metadata: {} } as any);

    await table.deletePartition({ tenantId: 'tenant1', userId: 'user1' });

    expect(deleteBatchSpy).not.toHaveBeenCalled();
  });

  it('deletes partition with more than 25 items (multiple batches)', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    const allItems = Array.from({ length: 60 }, (_, i) => ({
      tenantId: 'tenant1',
      userId: 'user1',
      sort: `MSG#${i + 1}`,
      message: `message${i + 1}`,
    }));

    let callCount = 0;
    jest.spyOn(table, 'getPartitionBatch').mockImplementation(async () => {
      callCount++;
      const limit = 25;
      const startIndex = (callCount - 1) * limit;
      const endIndex = Math.min(startIndex + limit, allItems.length);
      const items = allItems.slice(startIndex, endIndex);
      const hasNext = endIndex < allItems.length;

      return {
        items: items as any,
        lastEvaluatedKey: hasNext ? { sort: `MSG#${endIndex}` } : undefined,
        firstEvaluatedKey: undefined,
        count: items.length,
        hasNext,
        hasPrevious: false,
        direction: 'forward' as const,
      };
    });

    const deleteBatchSpy = jest
      .spyOn(table, 'deleteBatch')
      .mockResolvedValue({ $metadata: {} } as any);

    await table.deletePartition({ tenantId: 'tenant1', userId: 'user1' });

    expect(callCount).toBe(3); // 25 + 25 + 10
    expect(deleteBatchSpy).toHaveBeenCalledTimes(3);
    expect(deleteBatchSpy.mock.calls[0][0]).toHaveLength(25);
    expect(deleteBatchSpy.mock.calls[1][0]).toHaveLength(25);
    expect(deleteBatchSpy.mock.calls[2][0]).toHaveLength(10);
  });

  it('works for tables without sort key', async () => {
    const client = createClient();
    const table = new Table<PKOnly, EmptySK, Data>(
      client,
      'Users',
      schemaWithoutSk
    );

    jest.spyOn(table, 'getPartitionBatch').mockResolvedValue({
      items: [
        {
          id: 'user1',
          message: 'message1',
        },
      ] as any,
      lastEvaluatedKey: undefined,
      firstEvaluatedKey: undefined,
      count: 1,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });

    const deleteBatchSpy = jest
      .spyOn(table, 'deleteBatch')
      .mockResolvedValue({ $metadata: {} } as any);

    await table.deletePartition({ id: 'user1' });

    expect(deleteBatchSpy).toHaveBeenCalledTimes(1);
    expect(deleteBatchSpy).toHaveBeenCalledWith([
      {
        pk: { id: 'user1' },
        sk: undefined,
      },
    ]);
  });

  it('throws when pk is missing', async () => {
    const client = createClient();
    const table = new Table<PK, SK, Data>(client, 'Messages', schema);

    await expect(
      table.deletePartition(undefined as unknown as PK)
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  describe('deleteWithCondition', () => {
    it('deletes items matching SK condition and filter', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;

      // Mock search results
      const mockItems = [
        {
          pk: { S: 'tenant1#user1' },
          sk: { S: 'sort1' },
          message: { S: 'test1' },
        },
        {
          pk: { S: 'tenant1#user1' },
          sk: { S: 'sort2' },
          message: { S: 'test2' },
        },
      ];

      const mockSearchResponse = {
        Items: mockItems,
        LastEvaluatedKey: undefined,
      };

      sendMock
        .mockResolvedValueOnce(mockSearchResponse) // First search
        .mockResolvedValueOnce({}) // deleteBatch transaction
        .mockResolvedValueOnce({ Items: [] }); // Verify search after deletion

      jest.spyOn(table, 'query').mockReturnValue({
        whereSKequal: jest.fn().mockReturnThis(),
        filter: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        run: jest.fn().mockResolvedValue({
          items: [
            {
              tenantId: 'tenant1',
              userId: 'user1',
              sort: 'sort1',
              message: 'test1',
            },
            {
              tenantId: 'tenant1',
              userId: 'user1',
              sort: 'sort2',
              message: 'test2',
            },
          ],
          hasNext: false,
          lastEvaluatedKey: undefined,
        }),
      } as any);

      jest.spyOn(table, 'deleteBatch').mockResolvedValue({} as any);

      const deleted = await table.deleteWithCondition({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        skCondition: { equal: { sort: 'sort1' } },
        filter: { message: 'test1' },
        limit: 10,
      });

      expect(deleted).toBe(2);
      expect(table.deleteBatch).toHaveBeenCalled();
    });

    it('throws error when pk is missing', async () => {
      const table = new Table<PK, SK, Data>(createClient(), 'Messages', schema);

      await expect(
        table.deleteWithCondition({
          pk: undefined as any,
          limit: 10,
        })
      ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
    });

    it('handles deletion with pagination', async () => {
      const table = new Table<PK, SK, Data>(createClient(), 'Messages', schema);

      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
        filter: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        pivot: jest.fn().mockReturnThis(),
        run: jest
          .fn()
          .mockResolvedValueOnce({
            items: Array(25)
              .fill(null)
              .map((_, i) => ({
                tenantId: 'tenant1',
                userId: 'user1',
                sort: `sort${i}`,
                message: `test${i}`,
              })),
            hasNext: true,
            lastEvaluatedKey: {
              tenantId: 'tenant1',
              userId: 'user1',
              sort: 'sort24',
            },
          })
          .mockResolvedValueOnce({
            items: Array(5)
              .fill(null)
              .map((_, i) => ({
                tenantId: 'tenant1',
                userId: 'user1',
                sort: `sort${i + 25}`,
                message: `test${i + 25}`,
              })),
            hasNext: false,
            lastEvaluatedKey: undefined,
          }),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);
      jest.spyOn(table, 'deleteBatch').mockResolvedValue({} as any);

      const deleted = await table.deleteWithCondition({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        skCondition: { equal: { sort: 'sort1' } },
        limit: 30,
      });

      expect(deleted).toBe(30);
      expect(queryMock.pivot).toHaveBeenCalled();
    });

    it('handles deletion error', async () => {
      const table = new Table<PK, SK, Data>(createClient(), 'Messages', schema);

      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        run: jest.fn().mockResolvedValue({
          items: [
            {
              tenantId: 'tenant1',
              userId: 'user1',
              sort: 'sort1',
              message: 'test1',
            },
          ],
          hasNext: false,
          lastEvaluatedKey: undefined,
        }),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);
      jest
        .spyOn(table, 'deleteBatch')
        .mockRejectedValue(new Error('Delete failed'));

      await expect(
        table.deleteWithCondition({
          pk: { tenantId: 'tenant1', userId: 'user1' },
          limit: 10,
        })
      ).rejects.toThrow('Failed to delete batch: Delete failed');
    });

    it('handles deletion error with non-Error object', async () => {
      const table = new Table<PK, SK, Data>(createClient(), 'Messages', schema);

      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        run: jest.fn().mockResolvedValue({
          items: [
            {
              tenantId: 'tenant1',
              userId: 'user1',
              sort: 'sort1',
              message: 'test1',
            },
          ],
          hasNext: false,
          lastEvaluatedKey: undefined,
        }),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);
      // Mock a non-Error object (e.g., a string)
      jest.spyOn(table, 'deleteBatch').mockRejectedValue('String error');

      await expect(
        table.deleteWithCondition({
          pk: { tenantId: 'tenant1', userId: 'user1' },
          limit: 10,
        })
      ).rejects.toThrow('Failed to delete batch: String error');
    });

    it('stops when no items are found', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const deleteBatchSpy = jest.spyOn(table, 'deleteBatch');

      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        run: jest.fn().mockResolvedValue({
          items: [],
          hasNext: false,
          lastEvaluatedKey: undefined,
        }),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const deleted = await table.deleteWithCondition({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        limit: 10,
      });

      expect(deleted).toBe(0);
      expect(deleteBatchSpy).not.toHaveBeenCalled();
    });

    it('respects limit when items exceed limit', async () => {
      const table = new Table<PK, SK, Data>(createClient(), 'Messages', schema);

      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
        sortAscending: jest.fn().mockReturnThis(),
        run: jest.fn().mockResolvedValue({
          items: Array(30)
            .fill(null)
            .map((_, i) => ({
              tenantId: 'tenant1',
              userId: 'user1',
              sort: `sort${i}`,
              message: `test${i}`,
            })),
          hasNext: false,
          lastEvaluatedKey: undefined,
        }),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);
      jest.spyOn(table, 'deleteBatch').mockResolvedValue({} as any);

      const deleted = await table.deleteWithCondition({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        limit: 10,
      });

      expect(deleted).toBe(10);
      expect(table.deleteBatch).toHaveBeenCalledTimes(1);
    });
  });
});
