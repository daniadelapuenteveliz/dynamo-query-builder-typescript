import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoClient } from '../../clients/dynamo-client';
import { Config, KeySchema } from '../../types/types';

jest.mock('@aws-sdk/client-dynamodb', () => {
  const actual = jest.requireActual('@aws-sdk/client-dynamodb');
  return {
    ...actual,
    DynamoDBClient: jest.fn().mockImplementation((config) => ({
      config,
    })),
  };
});

type PK = { id: string };
type SK = { sort: string };
type Data = { message: string };

const config: Config = {
  region: 'us-east-1',
  credentials: {
    accessKeyId: 'test-access-key',
    secretAccessKey: 'test-secret-key',
  },
};

const schema: KeySchema = {
  pk: {
    name: 'pk',
    keys: ['id'],
  },
  sk: {
    name: 'sk',
    keys: ['sort'],
  },
};

describe('DynamoClient', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create a DynamoDBClient with correct config', () => {
      new DynamoClient(config);

      expect(DynamoDBClient).toHaveBeenCalledWith({
        region: 'us-east-1',
        credentials: {
          accessKeyId: 'test-access-key',
          secretAccessKey: 'test-secret-key',
        },
      });
    });
  });

  describe('table', () => {
    it('should create a Table instance with correct types', () => {
      const client = new DynamoClient(config);
      const table = client.table<PK, SK, Data>('TestTable', schema);

      expect(table).toBeDefined();
      expect(table.getTableName()).toBe('TestTable');
    });

    it('should create a Table instance without SK', () => {
      const dynamoClient = new DynamoClient(config);
      const tableSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const table = dynamoClient.table<PK, never, Data>('TestTable', tableSchema);

      expect(table).toBeDefined();
      expect(table.getTableName()).toBe('TestTable');
    });

    it('should create a Table instance without Data', () => {
      const client = new DynamoClient(config);
      const table = client.table<PK, SK, never>('TestTable', schema);

      expect(table).toBeDefined();
      expect(table.getTableName()).toBe('TestTable');
    });
  });

  describe('getClient', () => {
    it('should return the DynamoDBClient instance', () => {
      const client = new DynamoClient(config);
      const dynamoClient = client.getClient();

      expect(dynamoClient).toBeDefined();
      expect(dynamoClient.config).toBeDefined();
    });
  });
});

