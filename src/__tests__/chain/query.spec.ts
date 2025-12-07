import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Query } from '../../query/query';
import { SchemaFormatter } from '../../formatting/schemaFormatter';
import { CommandInput, KeySchema } from '../../types/types';

type PK = { tenantId: string };
type SK = { category: string; orderId: string };
type Data = { status: string };

const keySchema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['tenantId'],
  },
  sk: {
    name: 'sk',
    keys: ['category', 'orderId'],
    separator: '#',
  },
};

const keySchemaWithoutSk: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['tenantId'],
  },
};

const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

describe('Query', () => {
  let client: DynamoDBClient;
  let schemaFormatter: SchemaFormatter<PK, SK, Data>;
  let params: CommandInput;

  beforeEach(() => {
    client = createClient();
    schemaFormatter = new SchemaFormatter<PK, SK, Data>(keySchema);
    params = {
      Limit: 10,
      TableName: 'TestTable',
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
    };
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('whereSKGreaterThan', () => {
    it('adds greater than condition for string SK', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      const result = query.whereSKGreaterThan(sk);

      expect(params.KeyConditionExpression).toBe('#pk = :pk AND #sk > :sk');
      expect(params.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': 'sk',
      });
      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':sk': { S: 'category1#order1' },
      });
      expect(result).toBeInstanceOf(Query);
      expect(result).not.toBe(query);
    });

    it('uses string type when skparams.value is not a number', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = {
        category: 'category1',
        orderId: 'order1',
      };
      const result = query.whereSKGreaterThan(sk);

      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':sk': { S: 'category1#order1' },
      });
      expect(result).toBeInstanceOf(Query);
    });

    it('throws when table does not have SK', () => {
      const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
        keySchemaWithoutSk
      );
      const query = new Query(params, formatterWithoutSk, client);

      expect(() => (query as any).whereSKGreaterThan({})).toThrow(
        'SK is not defined in the schema'
      );
    });
  });

  describe('whereSKLowerThan', () => {
    it('adds lower than condition', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      const result = query.whereSKLowerThan(sk);

      expect(params.KeyConditionExpression).toBe('#pk = :pk AND #sk < :sk');
      expect(result).toBeInstanceOf(Query);
    });
  });

  describe('whereSKGreaterThanOrEqual', () => {
    it('adds greater than or equal condition', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      const result = query.whereSKGreaterThanOrEqual(sk);

      expect(params.KeyConditionExpression).toBe('#pk = :pk AND #sk >= :sk');
      expect(result).toBeInstanceOf(Query);
    });
  });

  describe('whereSKLowerThanOrEqual', () => {
    it('adds lower than or equal condition', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      const result = query.whereSKLowerThanOrEqual(sk);

      expect(params.KeyConditionExpression).toBe('#pk = :pk AND #sk <= :sk');
      expect(result).toBeInstanceOf(Query);
    });
  });

  describe('whereSKequal', () => {
    it('adds equal condition', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      const result = query.whereSKequal(sk);

      expect(params.KeyConditionExpression).toBe('#pk = :pk AND #sk = :sk');
      expect(result).toBeInstanceOf(Query);
    });
  });

  describe('whereSKBeginsWith', () => {
    it('adds begins_with condition for string SK', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValue('category1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const partialSk: Partial<SK> = { category: 'category1' };
      const result = query.whereSKBeginsWith(partialSk);

      expect(params.KeyConditionExpression).toBe(
        '#pk = :pk AND begins_with(#sk, :sk)'
      );
      expect(params.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': 'sk',
      });
      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':sk': { S: 'category1' },
      });
      expect(result).toBeInstanceOf(Query);
      expect(result).not.toBe(query);
    });

    it('initializes ExpressionAttributeNames when not present in whereSKBeginsWith', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValue('category1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const partialSk: Partial<SK> = { category: 'category1' };
      query.whereSKBeginsWith(partialSk);

      expect(testParams.ExpressionAttributeNames).toEqual({ '#sk': 'sk' });
    });

    it('initializes ExpressionAttributeValues when not present in whereSKBeginsWith', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValue('category1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const partialSk: Partial<SK> = { category: 'category1' };
      query.whereSKBeginsWith(partialSk);

      expect(testParams.ExpressionAttributeValues).toEqual({
        ':sk': { S: 'category1' },
      });
    });

    it('handles getSK returning undefined for name in whereSKBeginsWith', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest.spyOn(schemaFormatter, 'getSK').mockReturnValue(undefined as any);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValue('category1');

      const partialSk: Partial<SK> = { category: 'category1' };
      query.whereSKBeginsWith(partialSk);

      expect(testParams.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': '',
      });
      expect(testParams.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':sk': { S: 'category1' },
      });
    });

    it('throws when table does not have SK', () => {
      const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
        keySchemaWithoutSk
      );
      const query = new Query(params, formatterWithoutSk, client);

      expect(() => (query as any).whereSKBeginsWith({})).toThrow(
        'SK is not defined in the schema'
      );
    });

  });

  describe('whereSKBetween', () => {
    it('adds between condition for string SK', () => {
      const query = new Query(params, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValueOnce('category1#order1')
        .mockReturnValueOnce('category1#order2');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk1: Partial<SK> = { category: 'category1', orderId: 'order1' };
      const sk2: Partial<SK> = { category: 'category1', orderId: 'order2' };
      const result = query.whereSKBetween(sk1, sk2);

      expect(params.KeyConditionExpression).toBe(
        '#pk = :pk AND #sk between :low and :high'
      );
      expect(params.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': 'sk',
      });
      expect(params.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':low': { S: 'category1#order1' },
        ':high': { S: 'category1#order2' },
      });
      expect(result).toBeInstanceOf(Query);
      expect(result).not.toBe(query);
    });

    it('handles getSK returning undefined for name in whereSKBetween', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest.spyOn(schemaFormatter, 'getSK').mockReturnValue(undefined as any);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValueOnce('category1#order1')
        .mockReturnValueOnce('category1#order2');

      const sk1: Partial<SK> = { category: 'category1', orderId: 'order1' };
      const sk2: Partial<SK> = { category: 'category1', orderId: 'order2' };
      query.whereSKBetween(sk1, sk2);

      expect(testParams.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': '',
      });
      expect(testParams.ExpressionAttributeValues).toEqual({
        ':pk': { S: 'tenant1' },
        ':low': { S: 'category1#order1' },
        ':high': { S: 'category1#order2' },
      });
    });

    it('initializes ExpressionAttributeNames when not present in whereSKBetween', () => {
      const testParams: CommandInput = {
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
        Limit: 10,
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValueOnce('category1#order1')
        .mockReturnValueOnce('category1#order2');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk1: Partial<SK> = { category: 'category1', orderId: 'order1' };
      const sk2: Partial<SK> = { category: 'category1', orderId: 'order2' };
      query.whereSKBetween(sk1, sk2);

      expect(testParams.ExpressionAttributeNames).toEqual({ '#sk': 'sk' });
    });

    it('initializes ExpressionAttributeValues when not present in whereSKBetween', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatPartialOrderedSK')
        .mockReturnValueOnce('category1#order1')
        .mockReturnValueOnce('category1#order2');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk1: Partial<SK> = { category: 'category1', orderId: 'order1' };
      const sk2: Partial<SK> = { category: 'category1', orderId: 'order2' };
      query.whereSKBetween(sk1, sk2);

      expect(testParams.ExpressionAttributeValues).toEqual({
        ':low': { S: 'category1#order1' },
        ':high': { S: 'category1#order2' },
      });
    });

    it('throws when table does not have SK', () => {
      const formatterWithoutSk = new SchemaFormatter<PK, never, Data>(
        keySchemaWithoutSk
      );
      const query = new Query(params, formatterWithoutSk, client);

      expect(() => (query as any).whereSKBetween({}, {})).toThrow(
        'SK is not defined in the schema'
      );
    });
  });

  describe('compare method edge cases', () => {
    it('handles getSK returning undefined for name', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest.spyOn(schemaFormatter, 'getSK').mockReturnValue(undefined);

      const sk: SK = { category: 'category1', orderId: 'order1' };
      query.whereSKGreaterThan(sk);

      expect(testParams.ExpressionAttributeNames).toEqual({
        '#pk': 'pk',
        '#sk': '',
      });
    });

    it('initializes ExpressionAttributeNames when not present', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'tenant1' } },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      query.whereSKGreaterThan(sk);

      expect(testParams.ExpressionAttributeNames).toEqual({ '#sk': 'sk' });
    });

    it('initializes ExpressionAttributeValues when not present', () => {
      const testParams: CommandInput = {
        Limit: 10,
        TableName: 'TestTable',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
      };
      const query = new Query(testParams, schemaFormatter, client);
      jest
        .spyOn(schemaFormatter, 'formatSK')
        .mockReturnValue('category1#order1');
      jest
        .spyOn(schemaFormatter, 'getSK')
        .mockReturnValue({ name: 'sk', keys: ['category', 'orderId'] });

      const sk: SK = { category: 'category1', orderId: 'order1' };
      query.whereSKGreaterThan(sk);

      expect(testParams.ExpressionAttributeValues).toEqual({
        ':sk': { S: 'category1#order1' },
      });
    });
  });
});
