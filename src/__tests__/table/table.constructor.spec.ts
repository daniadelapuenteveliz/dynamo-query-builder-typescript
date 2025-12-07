import { Table } from '../../table';
import { ConfigurationError, ErrorCode } from '../../types/errors';
import { KeySchema } from '../../types/types';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';

describe('Table constructor validations', () => {
  const createClient = () => ({ send: jest.fn() }) as unknown as DynamoDBClient;

  it('throws when keySchema is missing', () => {
    const client = createClient();
    expect(
      () => new Table(client, 'TestTable', undefined as unknown as KeySchema)
    ).toThrowError('Key schema is required');

    try {
      new Table(client, 'TestTable', undefined as unknown as KeySchema);
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as ConfigurationError).code).toBe(
        ErrorCode.KEY_SCHEMA_REQUIRED
      );
    }
  });

  it('throws when primary key definition is missing', () => {
    const client = createClient();
    expect(() => new Table(client, 'TestTable', {} as KeySchema)).toThrowError(
      'Primary key (pk) is required'
    );

    try {
      new Table(client, 'TestTable', {} as KeySchema);
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as ConfigurationError).code).toBe(ErrorCode.PK_REQUIRED);
    }
  });

  it('throws when composite pk lacks a separator', () => {
    const client = createClient();
    const schema: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['tenantId', 'userId'],
      },
    };

    expect(() => new Table(client, 'TestTable', schema)).toThrowError(
      'Separator is required when pk.keys.length is greater than 1 (current: 2)'
    );

    try {
      new Table(client, 'TestTable', schema);
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as ConfigurationError).code).toBe(
        ErrorCode.SEPARATOR_REQUIRED
      );
    }
  });

  it('throws when composite sk lacks a separator', () => {
    const client = createClient();
    const schema: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['tenantId'],
      },
      sk: {
        name: 'sk',
        keys: ['timestamp', 'cat'],
      },
    };

    expect(() => new Table(client, 'TestTable', schema)).toThrowError(
      'Separator is required when sk.keys.length is greater than 1 (current: 2)'
    );

    try {
      new Table(client, 'TestTable', schema);
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as ConfigurationError).code).toBe(
        ErrorCode.SEPARATOR_REQUIRED
      );
    }
  });

  it('creates an instance when the schema is valid', () => {
    const client = createClient();
    const schema: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['tenantId', 'userId'],
        separator: '#',
      },
      sk: {
        name: 'sk',
        keys: ['timestamp'],
      },
      preserve: ['userId'],
    };

    const table = new Table(client, 'TestTable', schema);

    expect(table.getKeySchema()).toBe(schema);
    expect(table.getClient()).toBe(client);
    expect(table.getTableName()).toBe('TestTable');
  });
});
