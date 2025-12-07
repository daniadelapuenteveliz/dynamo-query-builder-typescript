import { AttributeValue, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Table } from '../../table';
import { KeySchema, QueryParams, ScanParams, CommandInput, FilterObject, ProjectDto } from '../../types/types';
import { ErrorCode } from '../../types/errors';

jest.mock('@aws-sdk/client-dynamodb', () => {
  const actual = jest.requireActual('@aws-sdk/client-dynamodb');
  return {
    ...actual,
  };
});

type PK = { tenantId: string; userId: string };
type SK = { category: string; orderId: string };
type Data = { status: string };

const compositeSchema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['tenantId', 'userId'],
    separator: '#',
  },
  sk: {
    name: 'sk',
    keys: ['category', 'orderId'],
    separator: '#',
  },
};

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

const attr = (value: string): AttributeValue =>
  ({
    toString: () => value,
  }) as unknown as AttributeValue;

describe('Table.getOne', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  const buildQueryMocks = (result: any[]) => {
    const runMock = jest.fn().mockResolvedValue({
      items: result,
      lastEvaluatedKey: undefined,
      oldLastEvaluatedKey: undefined,
      count: result.length,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });
    return { runMock };
  };

  it('retrieves and formats a record when found', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );
    const { runMock } = buildQueryMocks([
      {
        tenantId: 'tenant',
        userId: 'user',
        category: 'purchase',
        orderId: '1234',
        status: 'shipped',
      },
    ]);

    const whereSKequalMock = jest.fn().mockReturnValue({
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    const item = await table.getOne(
      { tenantId: 'tenant', userId: 'user' },
      { category: 'purchase', orderId: '1234' }
    );

    expect(table.query).toHaveBeenCalledWith({
      pk: {
        tenantId: 'tenant',
        userId: 'user',
      },
      limit: 1,
    });
    expect(whereSKequalMock).toHaveBeenCalledWith({
      category: 'purchase',
      orderId: '1234',
    });
    expect(runMock).toHaveBeenCalled();
    expect(item).toEqual({
      tenantId: 'tenant',
      userId: 'user',
      category: 'purchase',
      orderId: '1234',
      status: 'shipped',
    });
  });

  it('passes IndexName when provided', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );
    const mocks = buildQueryMocks([
      {
        pk: attr('tenant#user'),
        sk: attr('purchase#1234'),
        status: attr('shipped'),
      },
    ]);

    const whereSKequalMock = jest.fn().mockReturnValue({ run: mocks.runMock });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    await table.getOne(
      { tenantId: 'tenant', userId: 'user' },
      { category: 'purchase', orderId: '1234' },
      'GSI1'
    );

    expect(table.query).toHaveBeenCalledWith({
      pk: {
        tenantId: 'tenant',
        userId: 'user',
      },
      limit: 1,
      IndexName: 'GSI1',
    });
    expect(mocks.runMock).toHaveBeenCalled();
  });

  it('throws when pk is missing', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    await expect(
      table.getOne(undefined as unknown as PK, {
        category: 'purchase',
        orderId: '1',
      })
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });

  it('throws when sk is missing but schema requires it', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    await expect(
      table.getOne(
        { tenantId: 'tenant', userId: 'user' },
        undefined as unknown as SK
      )
    ).rejects.toMatchObject({ code: ErrorCode.SK_REQUIRED });
  });

  it('throws item not found when query returns empty', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );
    const { runMock } = buildQueryMocks([]);

    const whereSKequalMock = jest.fn().mockReturnValue({ run: runMock });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    await expect(
      table.getOne(
        { tenantId: 'tenant', userId: 'user' },
        { category: 'purchase', orderId: '1' }
      )
    ).rejects.toMatchObject({ code: ErrorCode.ITEM_DOES_NOT_EXIST });

    expect(runMock).toHaveBeenCalled();
  });
});

describe('Table.getPartitionBatch', () => {
  const buildQueryMock = (items: Record<string, AttributeValue>[]) => {
    const runMock = jest.fn().mockResolvedValue({
      items: items,
      lastEvaluatedKey: undefined,
      oldLastEvaluatedKey: undefined,
      count: items.length,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });
    const queryMock: any = {
      sortAscending: jest.fn().mockReturnThis(),
      sortDescending: jest.fn().mockReturnThis(),
      pivot: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      run: runMock,
    };
    return { queryMock, runMock };
  };

  const createFormatterMock = () => {
    const formatRecordAsItemDto = jest.fn((record: any) => ({
      formatted: record,
    }));
    const formatPaginationResult = jest
      .fn()
      .mockReturnValue({ paginated: true });
    return { formatRecordAsItemDto, formatPaginationResult };
  };

  const baseSchema: KeySchema = {
    pk: {
      name: 'pk',
      keys: ['tenantId', 'userId'],
      separator: '#',
    },
    sk: {
      name: 'sk',
      keys: ['category', 'orderId'],
      separator: '#',
    },
  };

  it('formats query results and delegates pagination formatting', async () => {
    const table = new Table<PK, SK, Data>(createClient(), 'Orders', baseSchema);
    const formatter = createFormatterMock();
    (table as any).schemaFormatter = formatter;

    const rawItems = [
      { pk: attr('tenant#user'), sk: attr('category#1') },
      { pk: attr('tenant#user'), sk: attr('category#2') },
    ];
    const { queryMock, runMock } = buildQueryMock(rawItems);

    jest.spyOn(table, 'query').mockReturnValue(queryMock as any);

    const qparams: QueryParams<PK, Data> = {
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 2,
      project: ['status'],
    };

    const result = await table.getPartitionBatch(qparams);

    expect(table.query).toHaveBeenCalledTimes(1);
    expect(table.query).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 2 })
    );
    expect(qparams.limit).toBe(2);
    expect(queryMock.sortAscending).toHaveBeenCalledTimes(1);
    expect(queryMock.sortDescending).not.toHaveBeenCalled();
    expect(queryMock.pivot).not.toHaveBeenCalled();
    expect(runMock).toHaveBeenCalled();
    expect(result).toEqual({
      items: rawItems,
      lastEvaluatedKey: undefined,
      oldLastEvaluatedKey: undefined,
      count: rawItems.length,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward',
    });
  });

  it('sorts descending when direction is backward', async () => {
    const table = new Table<PK, SK, Data>(createClient(), 'Orders', baseSchema);
    const formatter = createFormatterMock();
    (table as any).schemaFormatter = formatter;

    const { queryMock } = buildQueryMock([]);

    jest.spyOn(table, 'query').mockReturnValue(queryMock as any);

    await table.getPartitionBatch({
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 1,
      pagination: {
        direction: 'backward',
      },
    });

    expect(queryMock.sortDescending).toHaveBeenCalledTimes(1);
    expect(queryMock.sortAscending).not.toHaveBeenCalled();
  });

  it('applies pivot when provided', async () => {
    const table = new Table<PK, SK, Data>(createClient(), 'Orders', baseSchema);
    const formatter = createFormatterMock();
    (table as any).schemaFormatter = formatter;

    const pivot = { sk: 'category#5' };
    const { queryMock } = buildQueryMock([]);

    jest.spyOn(table, 'query').mockReturnValue(queryMock as any);

    await table.getPartitionBatch({
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 1,
      pagination: {
        pivot,
      },
    });

    expect(queryMock.pivot).toHaveBeenCalledWith(pivot);
  });

  it('defaults to ascending when pagination is provided without a direction', async () => {
    const table = new Table<PK, SK, Data>(createClient(), 'Orders', baseSchema);
    const formatter = createFormatterMock();
    (table as any).schemaFormatter = formatter;

    const { queryMock } = buildQueryMock([]);

    jest.spyOn(table, 'query').mockReturnValue(queryMock as any);

    await table.getPartitionBatch({
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 5,
      pagination: {},
    });

    expect(queryMock.sortAscending).toHaveBeenCalledTimes(1);
    expect(queryMock.sortDescending).not.toHaveBeenCalled();
  });
});

describe('Table.query', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('creates a Query instance with string PK', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const qparams: QueryParams<PK, Data> = {
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 10,
    };

    const query = table.query(qparams);

    expect(query).toBeDefined();
    expect((query as any).params.TableName).toBe('Orders');
    expect((query as any).params.KeyConditionExpression).toBe('#pk = :pk');
    expect((query as any).params.ExpressionAttributeNames).toEqual({
      '#pk': 'pk',
    });
    expect((query as any).params.ExpressionAttributeValues).toEqual({
      ':pk': { S: 'tenant#user' },
    });
    expect((query as any).params.Limit).toBe(10);
  });

  it('includes IndexName when provided', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const qparams: QueryParams<PK, Data> = {
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 10,
      IndexName: 'GSI1',
    };

    const query = table.query(qparams);

    expect((query as any).params.IndexName).toBe('GSI1');
  });

});

describe('Table.queryRaw', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('creates a Query instance from raw QueryCommandInput', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: any = {
      TableName: 'Orders',
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'tenant#user' } },
      Limit: 10,
    };

    const query = table.queryRaw(commandInput);

    expect(query).toBeDefined();
    expect((query as any).params).toBe(commandInput);
    expect((query as any).schemaFormatter).toBeDefined();
    expect((query as any).getClient()).toBe(table.getClient());
  });

  it('creates a Query instance when TableName is not provided in commandInput', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: any = {
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'tenant#user' } },
      Limit: 10,
    };

    const query = table.queryRaw(commandInput);

    expect(query).toBeDefined();
    expect((query as any).params).toBe(commandInput);
  });

  it('throws when TableName in commandInput does not match table name', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: any = {
      TableName: 'DifferentTable',
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'tenant#user' } },
    };

    try {
      table.queryRaw(commandInput);
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.code).toBe(ErrorCode.TABLE_NAME_MISMATCH);
      expect(error.message).toContain('Table name mismatch');
    }
  });
});

describe('Table.scan', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('creates a Scan instance with basic params', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const sparams: ScanParams<Data> = {
      limit: 10,
    };

    const scan = table.scan(sparams);

    expect(scan).toBeDefined();
    expect((scan as any).params.TableName).toBe('Orders');
    expect((scan as any).params.Limit).toBe(10);
    expect((scan as any).params).not.toHaveProperty('IndexName');
  });

  it('includes IndexName when provided', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const sparams: ScanParams<Data> = {
      limit: 10,
      IndexName: 'GSI1',
    };

    const scan = table.scan(sparams);

    expect((scan as any).params.IndexName).toBe('GSI1');
  });

  it('applies project when provided', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const Scan = require('../../query/scan').Scan;
    const projectSpy = jest.spyOn(Scan.prototype, 'project').mockReturnThis();

    const sparams: ScanParams<Data> = {
      limit: 10,
      project: ['status'],
    };

    const scan = table.scan(sparams);

    expect(projectSpy).toHaveBeenCalledWith(['status']);
    expect(scan).toBeDefined();
    projectSpy.mockRestore();
  });

  it('does not apply project when not provided', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const Scan = require('../../query/scan').Scan;
    const projectSpy = jest.spyOn(Scan.prototype, 'project').mockReturnThis();

    const sparams: ScanParams<Data> = {
      limit: 10,
    };

    table.scan(sparams);

    expect(projectSpy).not.toHaveBeenCalled();
    projectSpy.mockRestore();
  });

  it('creates Scan instance with all optional params', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const Scan = require('../../query/scan').Scan;
    const projectSpy = jest.spyOn(Scan.prototype, 'project').mockReturnThis();

    const sparams: ScanParams<Data> = {
      limit: 20,
      IndexName: 'GSI2',
      project: ['status'],
    };

    const scan = table.scan(sparams);

    expect((scan as any).params.TableName).toBe('Orders');
    expect((scan as any).params.Limit).toBe(20);
    expect((scan as any).params.IndexName).toBe('GSI2');
    expect(projectSpy).toHaveBeenCalledWith(['status']);
    projectSpy.mockRestore();
  });
});

describe('Table.scanRaw', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('creates a Scan instance from raw CommandInput', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: CommandInput = {
      TableName: 'Orders',
      Limit: 10,
      FilterExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': { S: 'active' } },
    };

    const scan = table.scanRaw(commandInput);

    expect(scan).toBeDefined();
    expect((scan as any).params).toBe(commandInput);
    expect((scan as any).schemaFormatter).toBeDefined();
    expect((scan as any).getClient()).toBe(table.getClient());
  });

  it('creates a Scan instance when TableName is not provided in commandInput', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: any = {
      Limit: 10,
      FilterExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': { S: 'active' } },
    };

    const scan = table.scanRaw(commandInput);

    expect(scan).toBeDefined();
    expect((scan as any).params).toBe(commandInput);
  });

  it('throws when TableName in commandInput does not match table name', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: CommandInput = {
      TableName: 'DifferentTable',
      Limit: 10,
      FilterExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': { S: 'active' } },
    };

    expect(() => table.scanRaw(commandInput)).toThrow();
    try {
      table.scanRaw(commandInput);
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.code).toBe(ErrorCode.TABLE_NAME_MISMATCH);
      expect(error.message).toContain('Table name mismatch');
    }
  });

  it('creates Scan instance with IndexName in commandInput', () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const commandInput: CommandInput = {
      TableName: 'Orders',
      IndexName: 'GSI1',
      Limit: 10,
    };

    const scan = table.scanRaw(commandInput);

    expect(scan).toBeDefined();
    expect((scan as any).params.IndexName).toBe('GSI1');
  });
});

describe('Table.search', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  const buildQueryMock = (items: any[], hasNext: boolean = false, lastEvaluatedKey?: any) => {
    const runMock = jest.fn().mockResolvedValue({
      items,
      lastEvaluatedKey,
      oldLastEvaluatedKey: undefined,
      count: items.length,
      hasNext,
      hasPrevious: false,
      direction: 'forward' as const,
    });
    const formatPaginationResultMock = jest.fn().mockReturnValue({
      items,
      hasNext,
      lastEvaluatedKey,
      count: items.length,
      firstEvaluatedKey: items[0],
      direction: 'forward' as const,
    });
    return { runMock, formatPaginationResultMock };
  };

  it('searches with direct SK (backward compatible - equal)', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'pending' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKequalMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { category: 'purchase', orderId: '1234' },
      limit: 10,
    });

    expect(table.query).toHaveBeenCalledWith({
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 25,
    });
    expect(whereSKequalMock).toHaveBeenCalledWith({
      category: 'purchase',
      orderId: '1234',
    });
    expect(runMock).toHaveBeenCalled();
    expect(result).toEqual(mockItems);
  });

  it('searches with explicit equal condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKequalMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { equal: { category: 'purchase', orderId: '1234' } },
      limit: 10,
    });

    expect(whereSKequalMock).toHaveBeenCalledWith({ category: 'purchase', orderId: '1234' });
    expect(result).toEqual(mockItems);
  });

  it('searches with greaterThan condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKGreaterThanMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKGreaterThan: whereSKGreaterThanMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { greaterThan: { category: 'purchase', orderId: '1234' } },
      limit: 10,
    });

    expect(whereSKGreaterThanMock).toHaveBeenCalledWith({ category: 'purchase', orderId: '1234' });
    expect(result).toEqual(mockItems);
  });

  it('searches with lowerThan condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1233', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKLowerThanMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKLowerThan: whereSKLowerThanMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { lowerThan: { category: 'purchase', orderId: '1234' } },
      limit: 10,
    });

    expect(whereSKLowerThanMock).toHaveBeenCalledWith({ category: 'purchase', orderId: '1234' });
    expect(result).toEqual(mockItems);
  });

  it('searches with greaterThanOrEqual condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKGreaterThanOrEqualMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKGreaterThanOrEqual: whereSKGreaterThanOrEqualMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { greaterThanOrEqual: { category: 'purchase', orderId: '1234' } },
      limit: 10,
    });

    expect(whereSKGreaterThanOrEqualMock).toHaveBeenCalledWith({ category: 'purchase', orderId: '1234' });
    expect(result).toEqual(mockItems);
  });

  it('searches with lowerThanOrEqual condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKLowerThanOrEqualMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKLowerThanOrEqual: whereSKLowerThanOrEqualMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { lowerThanOrEqual: { category: 'purchase', orderId: '1234' } },
      limit: 10,
    });

    expect(whereSKLowerThanOrEqualMock).toHaveBeenCalledWith({ category: 'purchase', orderId: '1234' });
    expect(result).toEqual(mockItems);
  });

  it('searches with beginsWith condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'pending' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKBeginsWithMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKBeginsWith: whereSKBeginsWithMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { beginsWith: { category: 'purchase' } },
      limit: 10,
    });

    expect(whereSKBeginsWithMock).toHaveBeenCalledWith({ category: 'purchase' });
    expect(result).toEqual(mockItems);
  });

  it('searches with between condition', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'pending' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const whereSKBetweenMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKBetween: whereSKBetweenMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { between: { from: { category: 'purchase', orderId: '1234' }, to: { category: 'purchase', orderId: '1236' } } },
      limit: 10,
    });

    expect(whereSKBetweenMock).toHaveBeenCalledWith(
      { category: 'purchase', orderId: '1234' },
      { category: 'purchase', orderId: '1236' }
    );
    expect(result).toEqual(mockItems);
  });

  it('searches without SK when schema has no SK', async () => {
    const schemaWithoutSK: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['tenantId'],
      },
    };

    const table = new Table<PK, never, Data>(
      createClient(),
      'Orders',
      schemaWithoutSK
    );

    const mockItems = [
      { tenantId: 'tenant', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const queryMock = {
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    };

    jest.spyOn(table, 'query').mockReturnValue(queryMock as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      limit: 10,
    });

    expect(table.query).toHaveBeenCalled();
    expect(result).toEqual(mockItems);
  });

  it('searches with filter and project', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
    ];

    const { runMock, formatPaginationResultMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = formatPaginationResultMock;

    const filterMock = jest.fn().mockReturnThis();
    const projectMock = jest.fn().mockReturnThis();
    const whereSKequalMock = jest.fn().mockReturnValue({
      filter: filterMock,
      project: projectMock,
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    const filter: FilterObject<Data> = { status: 'shipped' };
    const project: ProjectDto<Data> = ['status'];

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { category: 'purchase', orderId: '1234' },
      limit: 10,
      filter,
      project,
    });

    expect(filterMock).toHaveBeenCalledWith(filter);
    expect(projectMock).toHaveBeenCalledWith(project);
    expect(result).toEqual(mockItems);
  });

  it('handles pagination when hasNext is true', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const firstBatch = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'pending' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1236', status: 'delivered' },
    ];

    const secondBatch = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1237', status: 'cancelled' },
    ];

    const firstRunMock = jest.fn().mockResolvedValueOnce({
      items: firstBatch,
      lastEvaluatedKey: { category: 'purchase', orderId: '1236' },
      oldLastEvaluatedKey: undefined,
      count: firstBatch.length,
      hasNext: true,
      hasPrevious: false,
      direction: 'forward' as const,
    });
    const secondRunMock = jest.fn().mockResolvedValueOnce({
      items: secondBatch,
      lastEvaluatedKey: undefined,
      oldLastEvaluatedKey: undefined,
      count: secondBatch.length,
      hasNext: false,
      hasPrevious: false,
      direction: 'forward' as const,
    });

    const whereSKequalMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: firstRunMock,
    });

    const whereSKequalMock2 = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      pivot: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: secondRunMock,
    });

    const queryMock = jest.spyOn(table, 'query');
    queryMock
      .mockReturnValueOnce({
        whereSKequal: whereSKequalMock,
      } as any)
      .mockReturnValueOnce({
        whereSKequal: whereSKequalMock2,
      } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { category: 'purchase', orderId: '1234' },
      limit: 4,
    });

    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(result).toHaveLength(4);
    expect(result).toEqual([...firstBatch, ...secondBatch]);
  });

  it('respects limit when pagination returns more items than needed', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    const mockItems = [
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1234', status: 'shipped' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1235', status: 'pending' },
      { tenantId: 'tenant', userId: 'user', category: 'purchase', orderId: '1236', status: 'delivered' },
    ];

    const { runMock } = buildQueryMock(mockItems, false);
    (table as any).schemaFormatter.formatPaginationResult = jest.fn().mockReturnValue({
      items: mockItems,
      hasNext: false,
      lastEvaluatedKey: undefined,
      count: mockItems.length,
      firstEvaluatedKey: mockItems[0],
      direction: 'forward' as const,
    });

    const whereSKequalMock = jest.fn().mockReturnValue({
      filter: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sortAscending: jest.fn().mockReturnThis(),
      run: runMock,
    });

    jest.spyOn(table, 'query').mockReturnValue({
      whereSKequal: whereSKequalMock,
    } as any);

    const result = await table.search({
      pk: { tenantId: 'tenant', userId: 'user' },
      skCondition: { category: 'purchase', orderId: '1234' },
      limit: 2,
    });

    expect(result).toHaveLength(2);
    expect(result).toEqual(mockItems.slice(0, 2));
  });

  it('throws error when pk is missing', async () => {
    const table = new Table<PK, SK, Data>(
      createClient(),
      'Orders',
      compositeSchema
    );

    await expect(
      table.search({
        pk: undefined as unknown as PK,
        limit: 10,
      })
    ).rejects.toMatchObject({ code: ErrorCode.PK_REQUIRED });
  });
});
