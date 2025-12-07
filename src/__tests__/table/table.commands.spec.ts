import {
  PutItemCommand,
  PutItemCommandInput,
  TransactWriteItemsCommand,
  TransactWriteItemsCommandOutput,
  DynamoDBClient,
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
  };
});

type PK = { tenantId: string; userId: string };
type SK = { sort: string };
type Data = { message: string };

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

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

const buildItem = (overrides: Partial<PK & SK & Data> = {}) => ({
  tenantId: 'tenant',
  userId: 'user',
  sort: 'MSG',
  message: 'hello world',
  ...overrides,
});

describe('Table command methods', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('put', () => {
    it('sends a PutItemCommand with conditional expression by default', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      const response = { $metadata: { httpStatusCode: 200 } };
      sendMock.mockResolvedValue(response);

      const result = await table.put(buildItem());

      expect(result).toBe(response);
      expect(sendMock).toHaveBeenCalledTimes(1);
      const command = sendMock.mock.calls[0][0] as PutItemCommand;
      expect(command.input.TableName).toBe('Messages');
      expect(command.input.ConditionExpression).toBe(
        'attribute_not_exists(#pk) AND attribute_not_exists(#sk)'
      );
      expect(command.input.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': 'sk',
      });
    });

    it('omits conditional expression when override is true', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockResolvedValue({});

      await table.put(buildItem(), true);

      const command = sendMock.mock.calls[0][0] as PutItemCommand;
      expect(command.input.ConditionExpression).toBeUndefined();
    });

    it('wraps conditional request failure as ValidationError', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockRejectedValue(new Error('The conditional request failed'));

      await expect(table.put(buildItem())).rejects.toMatchObject({
        code: ErrorCode.ITEM_ALREADY_EXISTS,
      });
    });

    it('rethrows unexpected errors', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const originalError = new Error('Network crash');
      (client.send as jest.Mock).mockRejectedValue(originalError);

      await expect(table.put(buildItem())).rejects.toBe(originalError);
    });
  });

  describe('putRaw', () => {
    it('sends the provided PutItemCommandInput without alterations', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockResolvedValue({ ok: true });

      const input: PutItemCommandInput = {
        TableName: 'Messages',
        Item: { pk: { S: 'tenant#user' } },
      };

      await table.putRaw(input);

      expect(sendMock).toHaveBeenCalledTimes(1);
      const command = sendMock.mock.calls[0][0] as PutItemCommand;
      expect(command.input).toBe(input);
    });
  });

  describe('putBatch', () => {
    it('sends a TransactWriteItemsCommand with formatted items', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      const mockResponse: TransactWriteItemsCommandOutput = {
        $metadata: {},
      } as TransactWriteItemsCommandOutput;
      sendMock.mockResolvedValue(mockResponse);

      const items = [buildItem(), buildItem({ sort: 'MSG#2' })];
      const response = await table.putBatch(items);

      expect(response).toBe(mockResponse);
      expect(sendMock).toHaveBeenCalledTimes(1);
      const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
      expect(command.input.TransactItems).toHaveLength(2);
      const firstPut = command.input.TransactItems?.[0]?.Put;
      expect(firstPut?.ConditionExpression).toBe(
        'attribute_not_exists(#pk) AND attribute_not_exists(#sk)'
      );
    });

    it('omits condition expression for override=true', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockResolvedValue({ $metadata: {} });

      const items = [buildItem(), buildItem({ sort: 'SECOND' })];
      await table.putBatch(items, true);

      const command = sendMock.mock.calls[0][0] as TransactWriteItemsCommand;
      const firstPut = command.input.TransactItems?.[0]?.Put;
      expect(firstPut?.ConditionExpression).toBeUndefined();
    });

    it('returns null and avoids sending a command when the batch is empty', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;

      const response = await table.putBatch([]);

      expect(response).toBeNull();
      expect(sendMock).not.toHaveBeenCalled();
    });

    it('throws when batch size exceeds 25 items', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const items = Array.from({ length: 26 }, (_, idx) =>
        buildItem({ sort: `msg#${idx}` })
      );

      await expect(table.putBatch(items)).rejects.toMatchObject({
        code: ErrorCode.BATCH_LIMIT_EXCEEDED,
      });
    });

    it('wraps conditional failures as ValidationError', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockRejectedValue(
        new Error('ConditionalCheckFailed: item exists')
      );

      await expect(table.putBatch([buildItem()])).rejects.toMatchObject({
        code: ErrorCode.CONDITIONAL_CHECK_FAILED,
      });
    });

    it('wraps duplicate item errors with specific code', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const sendMock = client.send as jest.Mock;
      sendMock.mockRejectedValue(
        new Error(
          'Transaction request cannot include multiple operations on one item'
        )
      );

      await expect(
        table.putBatch([buildItem(), buildItem()])
      ).rejects.toMatchObject({
        code: ErrorCode.DUPLICATE_ITEMS_IN_BATCH,
      });
    });

    it('rethrows unexpected errors from the client', async () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const unexpected = new Error('Boom');
      (client.send as jest.Mock).mockRejectedValue(unexpected);

      await expect(table.putBatch([buildItem()])).rejects.toBe(unexpected);
    });
  });

  describe('query method', () => {
    it('creates query with string type PK', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      
      const query = table.query({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        limit: 10,
      });

      expect(query).toBeDefined();
      const queryParams = (query as any).params;
      expect(queryParams.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1#user1' },
      });
    });

    it('creates query with IndexName', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      
      const query = table.query({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        limit: 10,
        IndexName: 'GSI1',
      });

      const queryParams = (query as any).params;
      expect(queryParams.IndexName).toBe('GSI1');
    });

    it('creates query with project', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      
      const query = table.query({
        pk: { tenantId: 'tenant1', userId: 'user1' },
        limit: 10,
        project: ['message'],
      });

      const queryParams = (query as any).params;
      expect(queryParams.ProjectionExpression).toBeDefined();
      expect(queryParams.ExpressionAttributeNames).toBeDefined();
    });
  });

  describe('applySKCondition', () => {
    it('handles backward compatible SK (direct SK object)', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { sort: 'sort1' });

      expect(queryMock.whereSKequal).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with equal', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { equal: { sort: 'sort1' } });

      expect(queryMock.whereSKequal).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with greaterThan', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKGreaterThan: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { greaterThan: { sort: 'sort1' } });

      expect(queryMock.whereSKGreaterThan).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with lowerThan', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKLowerThan: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { lowerThan: { sort: 'sort1' } });

      expect(queryMock.whereSKLowerThan).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with greaterThanOrEqual', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKGreaterThanOrEqual: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { greaterThanOrEqual: { sort: 'sort1' } });

      expect(queryMock.whereSKGreaterThanOrEqual).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with lowerThanOrEqual', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKLowerThanOrEqual: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { lowerThanOrEqual: { sort: 'sort1' } });

      expect(queryMock.whereSKLowerThanOrEqual).toHaveBeenCalledWith({ sort: 'sort1' });
    });

    it('handles SK condition with beginsWith', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKBeginsWith: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { beginsWith: { sort: 'sort' } });

      expect(queryMock.whereSKBeginsWith).toHaveBeenCalledWith({ sort: 'sort' });
    });

    it('handles SK condition with between', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKBetween: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
     tableAny.applySKCondition(query, { 
        between: { from: { sort: 'sort1' }, to: { sort: 'sort2' } } 
      });

      expect(queryMock.whereSKBetween).toHaveBeenCalledWith({ sort: 'sort1' }, { sort: 'sort2' });
    });

    it('falls back to whereSKequal for unknown condition', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
      tableAny.applySKCondition(query, { unknown: { sort: 'sort1' } } as any);

      expect(queryMock.whereSKequal).toHaveBeenCalled();
    });

    it('handles backward compatible SK with non-object value (defensive code)', () => {
      const client = createClient();
      const table = new Table<PK, SK, Data>(client, 'Messages', schema);
      const queryMock = {
        whereSKequal: jest.fn().mockReturnThis(),
      } as any;

      jest.spyOn(table, 'query').mockReturnValue(queryMock);

      const query = table.query({ pk: { tenantId: 'tenant1', userId: 'user1' }, limit: 10 });
      const tableAny = table as any;
      
      // Pass a non-object value to trigger line 1045 (backward compatible path)
      // This tests defensive code for runtime type checking
      tableAny.applySKCondition(query, null as any);

      expect(queryMock.whereSKequal).toHaveBeenCalledWith(null);
    });
  });
});
