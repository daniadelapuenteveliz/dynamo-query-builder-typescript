import {
  ErrorCode,
  DynamoError,
  ConfigurationError,
  ValidationError,
  MetadataError,
  OperationError,
  DynamoErrorFactory,
  isDynamoError,
  isConfigurationError,
  isValidationError,
  isMetadataError,
  isOperationError,
  categorizeAwsError,
} from '../types/errors';

describe('DynamoError base class', () => {
  class TestError extends DynamoError {
    constructor(contextOverrides = {}) {
      super(ErrorCode.DYNAMODB_ERROR, 'Base failure', {
        tableName: 'TestTable',
        operation: 'write',
        originalError: 'Upstream',
        ...contextOverrides,
      });
    }
  }

  it('attaches metadata and extends Error correctly', () => {
    const error = new TestError();

    expect(error).toBeInstanceOf(Error);
    expect(error.code).toBe(ErrorCode.DYNAMODB_ERROR);
    expect(error.name).toBe('TestError');
    expect(error.context.tableName).toBe('TestTable');
    expect(error.timestamp).toBeInstanceOf(Date);
  });

  it('builds detailed messages with optional context fields', () => {
    const error = new TestError();
    const detailed = error.getDetailedMessage();

    expect(detailed).toContain('Base failure');
    expect(detailed).toContain('(Table: TestTable)');
    expect(detailed).toContain('(Operation: write)');
    expect(detailed).toContain('(Original: Upstream)');
  });

  it('serializes to JSON for logging', () => {
    const error = new TestError();
    const json = error.toJSON() as Record<string, unknown>;

    expect(json).toMatchObject({
      name: 'TestError',
      code: ErrorCode.DYNAMODB_ERROR,
      message: 'Base failure',
      context: expect.objectContaining({ tableName: 'TestTable' }),
    });
    expect(typeof json.timestamp).toBe('string');
  });

  it('constructs safely when captureStackTrace is unavailable', () => {
    const original = (Error as unknown as { captureStackTrace?: any })
      .captureStackTrace;
    (Error as unknown as { captureStackTrace?: any }).captureStackTrace =
      undefined;

    class LegacyEnvError extends DynamoError {
      constructor() {
        super(ErrorCode.DYNAMODB_ERROR, 'Legacy');
      }
    }

    const error = new LegacyEnvError();

    expect(error.message).toBe('Legacy');
    expect(error.stack).toBeDefined();

    (Error as unknown as { captureStackTrace?: any }).captureStackTrace =
      original;
  });
});

describe('DynamoErrorFactory handler helpers', () => {
  it('handlePutItemError wraps conditional failures and rethrows others', () => {
    expect(() =>
      DynamoErrorFactory.handlePutItemError(
        new Error('The conditional request failed for PutItem')
      )
    ).toThrowErrorMatchingInlineSnapshot('"Item already exists"');

    const unexpected = new Error('Other failure');
    expect(() => DynamoErrorFactory.handlePutItemError(unexpected)).toThrow(
      unexpected
    );
  });

  it('handlePutItemBatchError maps known messages to validation errors', () => {
    expect(() =>
      DynamoErrorFactory.handlePutItemBatchError(
        new Error('ConditionalCheckFailed: duplicate')
      )
    ).toThrowError(ValidationError);

    try {
      DynamoErrorFactory.handlePutItemBatchError(
        new Error(
          'Transaction request cannot include multiple operations on one item'
        )
      );
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      expect((error as ValidationError).code).toBe(
        ErrorCode.DUPLICATE_ITEMS_IN_BATCH
      );
    }

    const unexpected = new Error('Unexpected batch failure');
    expect(() => DynamoErrorFactory.handlePutItemBatchError(unexpected)).toThrow(
      unexpected
    );
  });

  it('handleUpdate helpers map conditional failures and rethrow otherwise', () => {
    expect(() =>
      DynamoErrorFactory.handleUpdateItemError(
        new Error('The conditional request failed for UpdateItem')
      )
    ).toThrowErrorMatchingInlineSnapshot('"Item does not exist"');

    const unexpectedItem = new Error('Other update failure');
    expect(() =>
      DynamoErrorFactory.handleUpdateItemError(unexpectedItem)
    ).toThrow(unexpectedItem);

    expect(() =>
      DynamoErrorFactory.handleUpdateItemBatchError(
        new Error('ConditionalCheckFailed: update batch')
      )
    ).toThrowError(ValidationError);

    try {
      DynamoErrorFactory.handleUpdateItemBatchError(
        new Error(
          'Transaction request cannot include multiple operations on one item'
        )
      );
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      expect((error as ValidationError).code).toBe(
        ErrorCode.DUPLICATE_ITEMS_IN_BATCH
      );
    }

    const unexpectedBatch = new Error('Other update batch failure');
    expect(() =>
      DynamoErrorFactory.handleUpdateItemBatchError(unexpectedBatch)
    ).toThrow(unexpectedBatch);
  });
});

describe('DynamoErrorFactory constructors', () => {
  it('creates configuration errors with proper metadata', () => {
    const keySchema = DynamoErrorFactory.keySchemaRequired();
    const pk = DynamoErrorFactory.pkRequired();
    const separator = DynamoErrorFactory.separatorRequired('pk', 2);
    const mismatch = DynamoErrorFactory.tableNameMismatch('expected', 'actual');

    expect(keySchema).toBeInstanceOf(ConfigurationError);
    expect(pk.code).toBe(ErrorCode.PK_REQUIRED);
    expect(separator.context.operation).toBe('key_validation');
    expect(mismatch.context.tableName).toBe('actual');
  });

  it('creates validation errors with batch context', () => {
    const batchLimit = DynamoErrorFactory.batchLimitExceeded(30);
    const exists = DynamoErrorFactory.itemAlreadyExists();
    const missing = DynamoErrorFactory.itemDoesNotExist();
    const duplicate = DynamoErrorFactory.duplicateItemsInBatch('details');
    const conditional = DynamoErrorFactory.conditionalCheckFailed('details');

    expect(batchLimit.context.batchSize).toBe(30);
    expect(exists.code).toBe(ErrorCode.ITEM_ALREADY_EXISTS);
    expect(missing.code).toBe(ErrorCode.ITEM_DOES_NOT_EXIST);
    expect(duplicate.context.originalError).toBe('details');
    expect(conditional.context.originalError).toBe('details');
  });

  it('creates metadata errors with table names in context', () => {
    const tableInfo = DynamoErrorFactory.tableInfoNotFound('T');
    const tableName = DynamoErrorFactory.tableNameNotFound('T');
    const itemCount = DynamoErrorFactory.itemCountNotFound('T');
    const keySchema = DynamoErrorFactory.keySchemaNotFound('T');
    const attributeDefs = DynamoErrorFactory.attributeDefinitionsNotFound('T');
    const gsi = DynamoErrorFactory.globalSecondaryIndexesNotFound('T');

    const all = [
      tableInfo,
      tableName,
      itemCount,
      keySchema,
      attributeDefs,
      gsi,
    ];

    for (const error of all) {
      expect(error).toBeInstanceOf(MetadataError);
      expect(error.context.tableName).toBe('T');
    }
  });

  it('supports metadata and operation errors without explicit context', () => {
    const metadataError = new MetadataError(
      ErrorCode.TABLE_INFO_NOT_FOUND,
      'Missing table metadata'
    );
    const operationError = new OperationError(
      ErrorCode.DYNAMODB_ERROR,
      'Operation failure'
    );

    expect(metadataError.context).toEqual({});
    expect(operationError.context).toEqual({});
  });

  it('creates operation errors with decorated context', () => {
    const dynamo = DynamoErrorFactory.dynamoDbError('boom', 'orig', 'Table');
    const transaction = DynamoErrorFactory.transactionError('t', 'orig');
    const network = DynamoErrorFactory.networkError('n', 'orig');
    const auth = DynamoErrorFactory.authenticationError('a', 'orig');
    const throttling = DynamoErrorFactory.throttlingError('th', 'orig');
    const validation = DynamoErrorFactory.validationError('val', 'orig');

    expect(dynamo).toBeInstanceOf(OperationError);
    expect(dynamo.context.tableName).toBe('Table');
    expect(transaction.context.operation).toBe('transaction');
    expect(network.context.operation).toBe('network');
    expect(auth.context.operation).toBe('authentication');
    expect(throttling.context.operation).toBe('throttling');
    expect(validation.context.operation).toBe('validation');
  });
});

describe('Type guard helpers', () => {
  const configError = DynamoErrorFactory.keySchemaRequired();
  const validationError = DynamoErrorFactory.itemAlreadyExists();
  const metadataError = DynamoErrorFactory.tableInfoNotFound('T');
  const operationError = DynamoErrorFactory.dynamoDbError('boom');
  const plainError = new Error('plain');

  it('identifies DynamoError derivatives', () => {
    expect(isDynamoError(configError)).toBe(true);
    expect(isConfigurationError(configError)).toBe(true);
    expect(isValidationError(validationError)).toBe(true);
    expect(isMetadataError(metadataError)).toBe(true);
    expect(isOperationError(operationError)).toBe(true);
  });

  it('returns false for non custom errors', () => {
    expect(isDynamoError(plainError)).toBe(false);
    expect(isConfigurationError(plainError)).toBe(false);
    expect(isValidationError(plainError)).toBe(false);
    expect(isMetadataError(plainError)).toBe(false);
    expect(isOperationError(plainError)).toBe(false);
  });
});

describe('categorizeAwsError', () => {
  it('maps various AWS error messages to error codes', () => {
    expect(categorizeAwsError(new Error('Conditional request failed.'))).toBe(
      ErrorCode.CONDITIONAL_CHECK_FAILED
    );
    expect(
      categorizeAwsError(
        new Error(
          'Transaction request cannot include multiple operations on one item'
        )
      )
    ).toBe(ErrorCode.DUPLICATE_ITEMS_IN_BATCH);
    expect(
      categorizeAwsError(new Error('Limit exceeded: throttling exception'))
    ).toBe(ErrorCode.THROTTLING_ERROR);
    expect(
      categorizeAwsError(new Error('Validation error: invalid input'))
    ).toBe(ErrorCode.VALIDATION_ERROR);
    expect(categorizeAwsError(new Error('Access denied by IAM policy'))).toBe(
      ErrorCode.AUTHENTICATION_ERROR
    );
    expect(categorizeAwsError(new Error('Network timeout occurred'))).toBe(
      ErrorCode.NETWORK_ERROR
    );
    expect(categorizeAwsError(new Error('Some other unexpected error'))).toBe(
      ErrorCode.DYNAMODB_ERROR
    );
  });
});
