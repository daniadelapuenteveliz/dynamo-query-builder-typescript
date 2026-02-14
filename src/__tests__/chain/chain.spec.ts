import {
  DynamoDBClient,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb';
import { Chain } from '../../query/chain';
import { Query } from '../../query/query';
import { SchemaFormatter } from '../../formatting/schemaFormatter';
import { CommandInput, KeySchema, rawFilterParams } from '../../types/types';
import { ErrorCode } from '../../types/errors';

jest.mock('@aws-sdk/client-dynamodb', () => {
  const actual = jest.requireActual('@aws-sdk/client-dynamodb');
  return {
    ...actual,
    QueryCommand: jest.fn().mockImplementation(input => ({ input })),
    ScanCommand: jest.fn().mockImplementation(input => ({ input })),
  };
});

jest.mock('@aws-sdk/util-dynamodb', () => ({
  unmarshall: jest.fn((item: any) => item),
}));

type PK = { tenantId: string };
type SK = { category: string };
type Data = {
  status?: string;
  age?: number;
  'address.city'?: string;
  isActive?: boolean;
  count?: number;
  description?: null;
};

const keySchema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['tenantId'],
  },
  sk: {
    name: 'sk',
    keys: ['category'],
  },
};

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

describe('Chain', () => {
  let client: DynamoDBClient;
  let schemaFormatter: SchemaFormatter<PK, SK, Data>;
  let params: CommandInput;

  beforeEach(() => {
    client = createClient();
    schemaFormatter = new SchemaFormatter<PK, SK, Data>(keySchema);
    params = {
      TableName: 'TestTable',
      Limit: 10,
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
    };
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('getClient', () => {
    it('returns the DynamoDB client instance', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      expect(chain.getClient()).toBe(client);
    });
  });

  describe('run', () => {
    it('executes query and returns formatted items', async () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      const mockItems = [
        {
          pk: { S: 'tenant1' },
          sk: { S: 'category1' },
          status: { S: 'active' },
        },
        {
          pk: { S: 'tenant1' },
          sk: { S: 'category2' },
          status: { S: 'inactive' },
        },
      ];

      const mockResponse = {
        Items: mockItems,
      };

      sendMock.mockResolvedValue(mockResponse);

      // Mock formatRecordAsItemDto
      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation((record: any) => ({
          tenantId: record.pk?.S || 'tenant1',
          category: record.sk?.S || 'category1',
          status: record.status?.S || 'active',
        }));

      const result = await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(QueryCommand).toHaveBeenCalledWith({
        ...params,
        ScanIndexForward: false,
      });
      expect(result.items).toHaveLength(2);
      expect(result).toHaveProperty('items');
      expect(result).toHaveProperty('hasNext');
      expect(result).toHaveProperty('count');
      expect(schemaFormatter.formatRecordAsItemDto).toHaveBeenCalledTimes(2);
    });

    it('returns empty array when Items is undefined', async () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      const mockResponse = {
        Items: undefined,
      };

      sendMock.mockResolvedValue(mockResponse);

      const result = await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(result.items).toEqual([]);
      expect(result).toHaveProperty('items');
      expect(result).toHaveProperty('hasNext', false);
      expect(result).toHaveProperty('count', 0);
    });

    it('returns empty array when Items is empty', async () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      const mockResponse = {
        Items: [],
      };

      sendMock.mockResolvedValue(mockResponse);

      const result = await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(result.items).toEqual([]);
      expect(result).toHaveProperty('items');
      expect(result).toHaveProperty('hasNext', false);
      expect(result).toHaveProperty('count', 0);
    });

    it('executes scan and returns formatted items', async () => {
      const scanParams: CommandInput = {
        TableName: 'TestTable',
        Limit: 10,
      };
      const chain = new Chain(scanParams, 'scan', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      const mockItems = [
        {
          pk: { S: 'tenant1' },
          sk: { S: 'category1' },
          status: { S: 'active' },
        },
      ];

      const mockResponse = {
        Items: mockItems,
      };

      sendMock.mockResolvedValue(mockResponse);

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation((record: any) => ({
          tenantId: record.pk?.S || 'tenant1',
          category: record.sk?.S || 'category1',
          status: record.status?.S || 'active',
        }));

      const result = await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(ScanCommand).toHaveBeenCalledWith(scanParams);
      expect(result.items).toHaveLength(1);
      expect(result).toHaveProperty('items');
      expect(result).toHaveProperty('hasNext');
      expect(result).toHaveProperty('count');
      expect(schemaFormatter.formatRecordAsItemDto).toHaveBeenCalledTimes(1);
    });

    it('throws error for invalid query type', async () => {
      const chain = new Chain(
        params,
        'invalid' as any,
        schemaFormatter,
        client
      );

      await expect(chain.run()).rejects.toMatchObject({
        code: ErrorCode.INVALID_QUERY_TYPE,
      });
    });

    it('handles run with all optional params set', async () => {
      const fullParams: CommandInput = {
        TableName: 'TestTable',
        Limit: 10,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
        FilterExpression: '#status = :status',
        ProjectionExpression: 'pk, sk, status',
        ExclusiveStartKey: { pk: { S: 'tenant0' } },
        IndexName: 'GSI1',
      };
      const chain = new Chain(fullParams, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      sendMock.mockResolvedValue({ Items: [] });

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation(
          () => ({ tenantId: 'tenant1', category: 'cat1' }) as any
        );

      await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      const callArgs = (QueryCommand as unknown as jest.Mock).mock.calls[0][0];
      expect(callArgs.IndexName).toBe('GSI1');
      expect(callArgs.ExclusiveStartKey).toEqual({ pk: { S: 'tenant0' } });
      expect(callArgs.ProjectionExpression).toBe('pk, sk, status');
      expect(callArgs.FilterExpression).toBe('#status = :status');
    });

    it('handles run with LastEvaluatedKey', async () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      const mockResponse = {
        Items: [
          {
            pk: { S: 'tenant1' },
            sk: { S: 'category1' },
            status: { S: 'active' },
          },
        ],
        LastEvaluatedKey: {
          pk: { S: 'tenant1' },
          sk: { S: 'category2' },
        },
      };

      sendMock.mockResolvedValue(mockResponse);

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation((record: any) => ({
          tenantId: record.pk?.S || 'tenant1',
          category: record.sk?.S || 'category1',
        }));

      const result = await chain.run();

      expect(result.lastEvaluatedKey).toBeDefined();
      expect(schemaFormatter.formatRecordAsItemDto).toHaveBeenCalledWith(
        expect.objectContaining({
          pk: { S: 'tenant1' },
          sk: { S: 'category2' },
        })
      );
    });

    it('handles run with ExclusiveStartKey', async () => {
      const testParams: CommandInput = {
        ...params,
        ExclusiveStartKey: {
          pk: { S: 'tenant0' },
          sk: { S: 'category0' },
        },
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      sendMock.mockResolvedValue({ Items: [] });

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation((record: any) => ({
          tenantId: record.pk?.S || 'tenant0',
          category: record.sk?.S || 'category0',
        }));

      await chain.run();

      expect(schemaFormatter.formatRecordAsItemDto).toHaveBeenCalledWith(
        expect.objectContaining({
          pk: { S: 'tenant0' },
          sk: { S: 'category0' },
        })
      );
    });

    it('handles run with ScanIndexForward set to false', async () => {
      const testParams: CommandInput = {
        ...params,
        ScanIndexForward: false,
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      sendMock.mockResolvedValue({ Items: [] });

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation(
          () => ({ tenantId: 'tenant1', category: 'cat1' }) as any
        );

      jest.spyOn(schemaFormatter, 'formatPaginationResult').mockReturnValue({
        items: [],
        hasNext: false,
        hasPrevious: false,
        count: 0,
        lastEvaluatedKey: undefined,
        firstEvaluatedKey: undefined,
        direction: 'backward',
      });

      await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      const callArgs = (QueryCommand as unknown as jest.Mock).mock.calls[0][0];
      expect(callArgs.ScanIndexForward).toBe(false);
      expect(schemaFormatter.formatPaginationResult).toHaveBeenCalledWith(
        expect.any(Array),
        expect.any(Number),
        'backward',
        undefined,
        undefined
      );
    });

    it('handles run with ScanIndexForward undefined (defaults to false)', async () => {
      const testParams: CommandInput = {
        TableName: 'TestTable',
        Limit: 10,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
        // Explicitly omitting ScanIndexForward to test undefined case
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      sendMock.mockResolvedValue({ Items: [] });

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation(
          () => ({ tenantId: 'tenant1', category: 'cat1' }) as any
        );

      jest.spyOn(schemaFormatter, 'formatPaginationResult').mockReturnValue({
        items: [],
        hasNext: false,
        hasPrevious: false,
        count: 0,
        lastEvaluatedKey: undefined,
        firstEvaluatedKey: undefined,
        direction: 'backward',
      });

      await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      const callArgs = (QueryCommand as unknown as jest.Mock).mock.calls[0][0];
      expect(callArgs.ScanIndexForward).toBe(false);
      expect(schemaFormatter.formatPaginationResult).toHaveBeenCalledWith(
        expect.any(Array),
        expect.any(Number),
        'backward',
        undefined,
        undefined
      );
    });

    it('handles run with ScanIndexForward set to true', async () => {
      const testParams: CommandInput = {
        ...params,
        ScanIndexForward: true,
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const sendMock = client.send as jest.Mock;

      sendMock.mockResolvedValue({ Items: [] });

      jest
        .spyOn(schemaFormatter, 'formatRecordAsItemDto')
        .mockImplementation(
          () => ({ tenantId: 'tenant1', category: 'cat1' }) as any
        );

      jest.spyOn(schemaFormatter, 'formatPaginationResult').mockReturnValue({
        items: [],
        hasNext: false,
        hasPrevious: false,
        count: 0,
        lastEvaluatedKey: undefined,
        firstEvaluatedKey: undefined,
        direction: 'forward',
      });

      await chain.run();

      expect(sendMock).toHaveBeenCalledTimes(1);
      const callArgs = (QueryCommand as unknown as jest.Mock).mock.calls[0][0];
      expect(callArgs.ScanIndexForward).toBe(true);
      expect(schemaFormatter.formatPaginationResult).toHaveBeenCalledWith(
        expect.any(Array),
        expect.any(Number),
        'forward',
        undefined,
        undefined
      );
    });
  });

  describe('sortAscending', () => {
    it('sets ScanIndexForward to true and returns the chain instance', () => {
      const chain = new Query(params, schemaFormatter, client);
      const result = chain.sortAscending();

      expect(params.ScanIndexForward).toBe(true);
      expect(result).toBe(chain);
    });

    it('overwrites existing ScanIndexForward value', () => {
      const testParams = { ...params, ScanIndexForward: false };
      const chain = new Query(testParams, schemaFormatter, client);
      chain.sortAscending();

      expect(testParams.ScanIndexForward).toBe(true);
    });
  });

  describe('sortDescending', () => {
    it('sets ScanIndexForward to false and returns the chain instance', () => {
      const chain = new Query(params, schemaFormatter, client);
      const result = chain.sortDescending();

      expect(params.ScanIndexForward).toBe(false);
      expect(result).toBe(chain);
    });

    it('overwrites existing ScanIndexForward value', () => {
      const testParams = { ...params, ScanIndexForward: true };
      const chain = new Query(testParams, schemaFormatter, client);
      chain.sortDescending();

      expect(testParams.ScanIndexForward).toBe(false);
    });
  });

  describe('project', () => {
    it('sets ProjectionExpression with PK, SK, and provided attributes', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category'] });

      const result = chain.project(['status']);

      expect(params.ProjectionExpression).toMatch(
        /^#attr_pk_\d+, #attr_sk_\d+, #attr_status_\d+$/
      );
      expect(params.ExpressionAttributeNames).toEqual(
        expect.objectContaining({
          '#pk': 'pk',
        })
      );
      // Verify the attribute name mappings exist
      const nameKeys = Object.keys(params.ExpressionAttributeNames || {});
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'pk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'sk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'status')
      ).toBe(true);
      expect(result).toBe(chain);
    });

    it('includes only PK when SK is not defined', () => {
      const schemaWithoutSk: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId'],
        },
      };
      const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
        schemaWithoutSk
      );
      const chain = new Chain(params, 'query', formatterWithoutSk, client);
      jest
        .spyOn(formatterWithoutSk, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest.spyOn(formatterWithoutSk, 'getSK').mockReturnValue(undefined);

      const result = chain.project(['status']);

      expect(params.ProjectionExpression).toMatch(
        /^#attr_pk_\d+, #attr_status_\d+$/
      );
      expect(params.ExpressionAttributeNames).toBeDefined();
      const nameKeys = Object.keys(params.ExpressionAttributeNames || {});
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'pk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'status')
      ).toBe(true);
      expect(result).toBe(chain);
    });

    it('includes only PK and SK when projectDto is empty', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category'] });

      const result = chain.project([]);

      expect(params.ProjectionExpression).toMatch(
        /^#attr_pk_\d+, #attr_sk_\d+$/
      );
      expect(params.ExpressionAttributeNames).toBeDefined();
      const nameKeys = Object.keys(params.ExpressionAttributeNames || {});
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'pk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'sk')
      ).toBe(true);
      expect(result).toBe(chain);
    });

    it('includes only PK and SK when projectDto is undefined', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category'] });

      const result = chain.project(undefined as any);

      expect(params.ProjectionExpression).toMatch(
        /^#attr_pk_\d+, #attr_sk_\d+$/
      );
      expect(params.ExpressionAttributeNames).toBeDefined();
      const nameKeys = Object.keys(params.ExpressionAttributeNames || {});
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'pk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'sk')
      ).toBe(true);
      expect(result).toBe(chain);
    });

    it('includes multiple attributes in ProjectionExpression', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category'] });

      const result = chain.project(['status']);

      expect(params.ProjectionExpression).toMatch(
        /^#attr_pk_\d+, #attr_sk_\d+, #attr_status_\d+$/
      );
      expect(params.ExpressionAttributeNames).toBeDefined();
      const nameKeys = Object.keys(params.ExpressionAttributeNames || {});
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'pk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'sk')
      ).toBe(true);
      expect(
        nameKeys.some(k => params.ExpressionAttributeNames![k] === 'status')
      ).toBe(true);
      expect(result).toBe(chain);
    });

    it('initializes ExpressionAttributeNames when it does not exist', () => {
      const testParams: CommandInput = {
        TableName: 'TestTable',
        Limit: 10,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'getPK')
        .mockReturnValue({ name: 'pk', keys: ['tenantId'] });
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category'] });

      const result = chain.project(['status']);

      expect(testParams.ExpressionAttributeNames).toBeDefined();
      expect(
        Object.keys(testParams.ExpressionAttributeNames || {}).length
      ).toBeGreaterThan(0);
      expect(result).toBe(chain);
    });
  });

  describe('pivot', () => {
    it('sets ExclusiveStartKey from formatted item keys', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const pivotItem = { tenantId: 'tenant1', category: 'category1' };

      jest.spyOn(schemaFormatter, 'formatItemKeysDtoAsRecord').mockReturnValue({
        pk: { S: 'tenant1' },
        sk: { S: 'category1' },
      });

      const result = chain.pivot(pivotItem);

      expect(schemaFormatter.formatItemKeysDtoAsRecord).toHaveBeenCalledWith(
        pivotItem
      );
      expect(params.ExclusiveStartKey).toEqual({
        pk: { S: 'tenant1' },
        sk: { S: 'category1' },
      });
      expect(result).toBe(chain);
    });

    it('when IndexName is set, uses formatItemKeysWithIndexDtoAsRecord and sets ExclusiveStartKey', () => {
      const paramsWithIndex = {
        ...params,
        IndexName: 'team_user',
      };
      const chain = new Chain(
        paramsWithIndex,
        'query',
        schemaFormatter,
        client
      );
      const pivotItem = {
        tenantId: 'tenant1',
        category: 'category1',
      };

      jest
        .spyOn(schemaFormatter, 'formatItemKeysWithIndexDtoAsRecord')
        .mockReturnValue({
          pk: { S: 'tenant1' },
          sk: { S: 'idx_sk_value' },
        });
      const formatItemKeysDtoAsRecordSpy = jest.spyOn(
        schemaFormatter,
        'formatItemKeysDtoAsRecord'
      );

      const result = chain.pivot(pivotItem);

      expect(
        schemaFormatter.formatItemKeysWithIndexDtoAsRecord
      ).toHaveBeenCalledWith(pivotItem, 'team_user');
      expect(formatItemKeysDtoAsRecordSpy).not.toHaveBeenCalled();
      expect(paramsWithIndex.ExclusiveStartKey).toEqual({
        pk: { S: 'tenant1' },
        sk: { S: 'idx_sk_value' },
      });
      expect(result).toBe(chain);
    });
  });

  describe('filterRaw', () => {
    it('sets FilterExpression when it does not exist', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      expect(params.FilterExpression).toBe('attribute_exists(#status)');
      // ExpressionAttributeNames already has #pk from params initialization
      expect(params.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#status': 'status',
      });
      // ExpressionAttributeValues already has :pk from params initialization
      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':val': { S: 'active' },
      });
      expect(result).toBe(chain);
    });

    it('appends to existing FilterExpression with AND', () => {
      const testParams = {
        ...params,
        FilterExpression: 'attribute_exists(#pk)',
        ExpressionAttributeNames: { '#pk': 'pk' },
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      expect(testParams.FilterExpression).toBe(
        'attribute_exists(#pk) AND (attribute_exists(#status))'
      );
      expect(result).toBe(chain);
    });

    it('merges ExpressionAttributeNames', () => {
      const testParams = {
        ...params,
        ExpressionAttributeNames: { '#pk': 'pk', '#existing': 'existing' },
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status', '#new': 'new' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      expect(testParams.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#existing': 'existing',
        '#status': 'status',
        '#new': 'new',
      });
      expect(result).toBe(chain);
    });

    it('uses else branch when ExpressionAttributeNames already exists', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const chain = new Query(testParams, schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      // Should merge, not replace
      expect(testParams.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#status': 'status',
      });
      expect(result).toBe(chain);
    });

    it('initializes ExpressionAttributeNames when missing (does not merge filter names)', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: undefined as any,
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const chain = new Query(testParams, schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      // According to current implementation, it only initializes to {},
      // and does not merge provided names in this branch
      expect(testParams.ExpressionAttributeNames).toEqual({});
      expect(result).toBe(chain);
    });

    it('sets ExpressionAttributeValues when they do not exist', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      // ExpressionAttributeValues already has :pk from params initialization
      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':val': { S: 'active' },
      });
      expect(result).toBe(chain);
    });

    it('merges ExpressionAttributeValues', () => {
      const testParams = {
        ...params,
        ExpressionAttributeValues: {
          ':pk': { S: 'tenant1' },
          ':existing': { S: 'existing' },
        },
      };
      const chain = new Chain(testParams, 'query', schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: {
          ':val': { S: 'active' },
          ':new': { N: '123' },
        },
      };

      const result = chain.filterRaw(filter);

      expect(testParams.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':existing': { S: 'existing' },
        ':val': { S: 'active' },
        ':new': { N: '123' },
      });
      expect(result).toBe(chain);
    });

    it('sets ExpressionAttributeValues when params does not have ExpressionAttributeValues', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
      };
      const chain = new Query(testParams, schemaFormatter, client);
      const filter: rawFilterParams = {
        FilterExpression: 'attribute_exists(#status)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      };

      const result = chain.filterRaw(filter);

      expect(testParams.ExpressionAttributeValues).toEqual({
        ':val': { S: 'active' },
      });
      expect(result).toBe(chain);
    });
  });

  describe('filter', () => {
    it('creates simple equality filter', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = { status: 'active' };

      chain.filter(filterObject);

      expect(params.FilterExpression).toMatch(/#status_\d+ = :val_status_\d+/);
      expect(params.ExpressionAttributeNames).toEqual(
        expect.objectContaining({
          '#pk': 'pk',
        })
      );
      // We can't predict the exact keys for names/values because of the counter,
      // but we can check if they exist and have correct values
      const nameKey = Object.keys(params.ExpressionAttributeNames!).find(
        k => params.ExpressionAttributeNames![k] === 'status'
      );
      expect(nameKey).toBeDefined();

      const valueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].S === 'active'
      );
      expect(valueKey).toBeDefined();
    });

    it('creates filter with comparison operators', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = { age: { '>=': 18 } };

      chain.filter(filterObject);

      expect(params.FilterExpression).toMatch(/#age_\d+ >= :val_age_\d+_/);

      const nameKey = Object.keys(params.ExpressionAttributeNames!).find(
        k => params.ExpressionAttributeNames![k] === 'age'
      );
      expect(nameKey).toBeDefined();

      const valueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].N === '18'
      );
      expect(valueKey).toBeDefined();
    });

    it('creates filter with multiple conditions (AND)', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = { status: 'active', age: { '>': 21 } };

      chain.filter(filterObject);

      expect(params.FilterExpression).toMatch(
        /#status_\d+ = :val_status_\d+ AND #age_\d+ > :val_age_\d+_/
      );
    });

    it('handles dot notation for nested attributes', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = { 'address.city': 'New York' };

      chain.filter(filterObject);

      const nameKey = Object.keys(params.ExpressionAttributeNames!).find(
        k => params.ExpressionAttributeNames![k] === 'address.city'
      );
      expect(nameKey).toBeDefined();
      expect(nameKey).toMatch(/#address_city_\d+/);
    });

    it('handles different value types', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = {
        isActive: true,
        count: 10,
        description: null,
      };

      chain.filter(filterObject);

      const boolValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].BOOL === true
      );
      expect(boolValueKey).toBeDefined();

      const numValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].N === '10'
      );
      expect(numValueKey).toBeDefined();

      const nullValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].NULL === true
      );
      expect(nullValueKey).toBeDefined();
    });

    it('handles string values in filter', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = {
        status: 'active',
        name: 'test',
      };

      chain.filter(filterObject);

      const statusValueKey = Object.keys(
        params.ExpressionAttributeValues!
      ).find(k => params.ExpressionAttributeValues![k].S === 'active');
      expect(statusValueKey).toBeDefined();

      const nameValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].S === 'test'
      );
      expect(nameValueKey).toBeDefined();
    });

    it('handles filter with operators and different value types', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = {
        age: { '>=': 18 },
        isActive: { '=': true },
        description: { '<>': null },
        name: { '>': 'A' },
      };

      chain.filter(filterObject);

      // Check number value
      const numValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].N === '18'
      );
      expect(numValueKey).toBeDefined();

      // Check boolean value
      const boolValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].BOOL === true
      );
      expect(boolValueKey).toBeDefined();

      // Check null value
      const nullValueKey = Object.keys(params.ExpressionAttributeValues!).find(
        k => params.ExpressionAttributeValues![k].NULL === true
      );
      expect(nullValueKey).toBeDefined();

      // Check string value
      const stringValueKey = Object.keys(
        params.ExpressionAttributeValues!
      ).find(k => params.ExpressionAttributeValues![k].S === 'A');
      expect(stringValueKey).toBeDefined();
    });

    it('does nothing for empty filter object', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      chain.filter({});

      expect(params.FilterExpression).toBeUndefined();
    });

    it('skips undefined values in filter object', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = { status: 'active', age: undefined as any };

      chain.filter(filterObject);

      expect(params.FilterExpression).toMatch(/#status_\d+ = :val_status_\d+/);
      expect(params.FilterExpression).not.toMatch(/age/);
    });

    it('handles filter with multiple operators on same field', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = {
        age: { '>': 18, '<': 65 } as any,
      };

      chain.filter(filterObject);

      // Operators get sanitized (non-alphanumeric removed), so '>' becomes empty string
      // Should create two conditions for the same field
      expect(params.FilterExpression).toMatch(/#age_\d+ > :val_age_\d+_/);
      expect(params.FilterExpression).toMatch(/#age_\d+ < :val_age_\d+_/);
      expect(params.FilterExpression).toMatch(/AND/);
    });

    it('handles filter with operator value as undefined', () => {
      const chain = new Chain(params, 'query', schemaFormatter, client);
      const filterObject = {
        age: { '>=': undefined as any },
      };

      chain.filter(filterObject);

      // Should not add condition for undefined values
      expect(params.FilterExpression).toBeUndefined();
    });
  });

  describe('Query methods', () => {
    describe('whereSKequal', () => {
      it('adds equality condition to KeyConditionExpression', () => {
        const query = new Query(params, schemaFormatter, client);
        const sk = { category: 'test' };

        const result = query.whereSKequal(sk);

        expect(params.KeyConditionExpression).toContain('AND #sk = :sk');
        expect(params.ExpressionAttributeNames!['#sk']).toBe('sk');
        expect(params.ExpressionAttributeValues![':sk']).toEqual({ S: 'test' });
        expect(result).toBeInstanceOf(Query);
        expect(result).not.toBe(query); // Should return new instance
      });

      it('throws error when SK is not defined in schema', () => {
        const schemaWithoutSk: KeySchema = {
          pk: {
            name: 'pk',
            keys: ['tenantId'],
          },
        };
        const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
          schemaWithoutSk
        );
        const query = new Query(params, formatterWithoutSk, client);

        expect(() => {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKequal({ category: 'test' });
        }).toThrow();
        try {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKequal({ category: 'test' });
        } catch (error: any) {
          expect(error.code).toBe(ErrorCode.SK_NOT_DEFINED_IN_SCHEMA);
        }
      });

      it('handles number type SK', () => {
        const numberSchema: KeySchema = {
          pk: {
            name: 'pk',
            keys: ['tenantId'],
          },
          sk: {
            name: 'sk',
            keys: ['value'],
          },
        };
        type NumberSK = { value: string };
        const numberFormatter = new SchemaFormatter<PK, NumberSK, Data>(
          numberSchema
        );
        const numberParams: CommandInput = {
          TableName: 'TestTable',
          Limit: 10,
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: { '#pk': 'pk' },
          ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
        };
        const query = new Query(numberParams, numberFormatter, client);
        jest.spyOn(numberFormatter, 'formatSK').mockReturnValue('123');
        // The compare method checks typeof skparams.value === 'number'
        // Since KeyRec is Record<string, string>, there's no 'value' property
        // So it will default to string type (S)
        const sk = { value: '123' } as any;
        // Mock the value to be a number for the type check
        Object.defineProperty(sk, 'value', {
          value: 123,
          writable: true,
          enumerable: true,
          configurable: true,
        });

        const result = query.whereSKequal(sk);

        // The compare method checks typeof skparams.value === 'number'
        expect(numberParams.ExpressionAttributeValues![':sk']).toEqual({
          N: '123',
        });
        expect(result).toBeInstanceOf(Query);
      });
    });

    describe('whereSKGreaterThan', () => {
      it('adds greater than condition', () => {
        const query = new Query(params, schemaFormatter, client);
        const sk = { category: 'test' };

        const result = query.whereSKGreaterThan(sk);

        expect(params.KeyConditionExpression).toContain('AND #sk > :sk');
        expect(result).toBeInstanceOf(Query);
      });
    });

    describe('whereSKLowerThan', () => {
      it('adds lower than condition', () => {
        const query = new Query(params, schemaFormatter, client);
        const sk = { category: 'test' };

        const result = query.whereSKLowerThan(sk);

        expect(params.KeyConditionExpression).toContain('AND #sk < :sk');
        expect(result).toBeInstanceOf(Query);
      });
    });

    describe('whereSKGreaterThanOrEqual', () => {
      it('adds greater than or equal condition', () => {
        const query = new Query(params, schemaFormatter, client);
        const sk = { category: 'test' };

        const result = query.whereSKGreaterThanOrEqual(sk);

        expect(params.KeyConditionExpression).toContain('AND #sk >= :sk');
        expect(result).toBeInstanceOf(Query);
      });
    });

    describe('whereSKLowerThanOrEqual', () => {
      it('adds lower than or equal condition', () => {
        const query = new Query(params, schemaFormatter, client);
        const sk = { category: 'test' };

        const result = query.whereSKLowerThanOrEqual(sk);

        expect(params.KeyConditionExpression).toContain('AND #sk <= :sk');
        expect(result).toBeInstanceOf(Query);
      });
    });

    describe('whereSKBeginsWith', () => {
      it('adds begins_with condition for string type', () => {
        const query = new Query(params, schemaFormatter, client);
        const partialSk = { category: 'test' };

        jest
          .spyOn(schemaFormatter, 'formatPartialOrderedSK')
          .mockReturnValue('test');

        const result = query.whereSKBeginsWith(partialSk);

        expect(params.KeyConditionExpression).toContain(
          'AND begins_with(#sk, :sk)'
        );
        expect(params.ExpressionAttributeValues![':sk']).toEqual({ S: 'test' });
        expect(result).toBeInstanceOf(Query);
      });

      it('throws error when SK is not defined in schema', () => {
        const schemaWithoutSk: KeySchema = {
          pk: {
            name: 'pk',
            keys: ['tenantId'],
          },
        };
        const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
          schemaWithoutSk
        );
        const query = new Query(params, formatterWithoutSk, client);

        expect(() => {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKBeginsWith({ category: 'test' });
        }).toThrow();
        try {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKBeginsWith({ category: 'test' });
        } catch (error: any) {
          expect(error.code).toBe(ErrorCode.SK_NOT_DEFINED_IN_SCHEMA);
        }
      });
    });

    describe('whereSKBetween', () => {
      it('adds between condition for string type', () => {
        // Create a fresh query with fresh params to avoid interference from other tests
        const testParams: CommandInput = {
          TableName: 'TestTable',
          Limit: 10,
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: { '#pk': 'pk' },
          ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
        };
        const query = new Query(testParams, schemaFormatter, client);
        const sk1 = { category: 'test1' };
        const sk2 = { category: 'test2' };

        jest
          .spyOn(schemaFormatter, 'formatPartialOrderedSK')
          .mockReturnValueOnce('test1')
          .mockReturnValueOnce('test2');

        const result = query.whereSKBetween(sk1, sk2);

        expect(testParams.KeyConditionExpression).toContain(
          'AND #sk between :low and :high'
        );
        expect(testParams.ExpressionAttributeValues![':low']).toEqual({
          S: 'test1',
        });
        expect(testParams.ExpressionAttributeValues![':high']).toEqual({
          S: 'test2',
        });
        expect(result).toBeInstanceOf(Query);
      });

      it('throws error when SK is not defined in schema', () => {
        const schemaWithoutSk: KeySchema = {
          pk: {
            name: 'pk',
            keys: ['tenantId'],
          },
        };
        const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
          schemaWithoutSk
        );
        const query = new Query(params, formatterWithoutSk, client);

        expect(() => {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKBetween({ category: 'test1' }, { category: 'test2' });
        }).toThrow();
        try {
          // @ts-expect-error - Testing error case with invalid SK
          query.whereSKBetween({ category: 'test1' }, { category: 'test2' });
        } catch (error: any) {
          expect(error.code).toBe(ErrorCode.SK_NOT_DEFINED_IN_SCHEMA);
        }
      });
    });
  });
});
