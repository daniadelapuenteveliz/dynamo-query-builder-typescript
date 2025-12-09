/**
 * Custom error classes for DynamoDB operations
 * Provides standardized error handling with proper error codes and context
 */

export enum ErrorCode {
  // Configuration errors
  FIRST_KEY_NOT_INCLUDED_IN_SK = 'FIRST_KEY_NOT_INCLUDED_IN_SK',
  KEY_NOT_INCLUDED_IN_SK = 'KEY_NOT_INCLUDED_IN_SK',
  SK_NOT_DEFINED_IN_SCHEMA = 'SK_NOT_DEFINED_IN_SCHEMA',
  SK_EMPTY = 'SK_EMPTY',
  KEY_PART_REQUIRED = 'KEY_PART_REQUIRED',
  KEY_SCHEMA_REQUIRED = 'KEY_SCHEMA_REQUIRED',
  PK_REQUIRED = 'PK_REQUIRED',
  SK_REQUIRED = 'SK_REQUIRED',
  NEW_DATA_REQUIRED = 'NEW_DATA_REQUIRED',
  SEPARATOR_REQUIRED = 'SEPARATOR_REQUIRED',
  TABLE_NAME_MISMATCH = 'TABLE_NAME_MISMATCH',
  KEY_TYPE_NOT_SUPPORTED = 'KEY_TYPE_NOT_SUPPORTED',
  INVALID_QUERY_TYPE = 'INVALID_QUERY_TYPE',
  // Validation errors
  BATCH_LIMIT_EXCEEDED = 'BATCH_LIMIT_EXCEEDED',
  ITEM_ALREADY_EXISTS = 'ITEM_ALREADY_EXISTS',
  ITEM_DOES_NOT_EXIST = 'ITEM_DOES_NOT_EXIST',
  DUPLICATE_ITEMS_IN_BATCH = 'DUPLICATE_ITEMS_IN_BATCH',
  CONDITIONAL_CHECK_FAILED = 'CONDITIONAL_CHECK_FAILED',

  // DynamoDB metadata errors
  TABLE_INFO_NOT_FOUND = 'TABLE_INFO_NOT_FOUND',
  TABLE_NAME_NOT_FOUND = 'TABLE_NAME_NOT_FOUND',
  ITEM_COUNT_NOT_FOUND = 'ITEM_COUNT_NOT_FOUND',
  KEY_SCHEMA_NOT_FOUND = 'KEY_SCHEMA_NOT_FOUND',
  ATTRIBUTE_DEFINITIONS_NOT_FOUND = 'ATTRIBUTE_DEFINITIONS_NOT_FOUND',
  GLOBAL_SECONDARY_INDEXES_NOT_FOUND = 'GLOBAL_SECONDARY_INDEXES_NOT_FOUND',

  // DynamoDB operation errors
  DYNAMODB_ERROR = 'DYNAMODB_ERROR',
  TRANSACTION_ERROR = 'TRANSACTION_ERROR',
  NETWORK_ERROR = 'NETWORK_ERROR',
  AUTHENTICATION_ERROR = 'AUTHENTICATION_ERROR',
  THROTTLING_ERROR = 'THROTTLING_ERROR',
  VALIDATION_ERROR = 'VALIDATION_ERROR',
}

export interface ErrorContext {
  tableName?: string | undefined;
  pkName?: string | undefined;
  pkValue?: string | undefined;
  skName?: string | undefined;
  skValue?: string | undefined;
  itemCount?: number | undefined;
  batchSize?: number | undefined;
  originalError?: string | undefined;
  operation?: string | undefined;
}

/**
 * Base error class for all DynamoDB operations
 */
export abstract class DynamoError extends Error {
  public readonly code: ErrorCode;
  public readonly context: ErrorContext;
  public readonly timestamp: Date;

  constructor(code: ErrorCode, message: string, context: ErrorContext = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.context = context;
    this.timestamp = new Date();

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }

  /**
   * Returns a detailed error message with context
   */
  public getDetailedMessage(): string {
    let message = this.message;

    if (this.context.tableName) {
      message += ` (Table: ${this.context.tableName})`;
    }

    if (this.context.operation) {
      message += ` (Operation: ${this.context.operation})`;
    }

    if (this.context.originalError) {
      message += ` (Original: ${this.context.originalError})`;
    }

    return message;
  }

  /**
   * Converts error to a plain object for logging/serialization
   */
  public toJSON(): object {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      timestamp: this.timestamp.toISOString(),
      stack: this.stack,
    };
  }
}

/**
 * Configuration-related errors
 */
export class ConfigurationError extends DynamoError {
  constructor(code: ErrorCode, message: string, context: ErrorContext = {}) {
    super(code, message, context);
  }
}

/**
 * Validation-related errors
 */
export class ValidationError extends DynamoError {
  constructor(code: ErrorCode, message: string, context: ErrorContext = {}) {
    super(code, message, context);
  }
}

/**
 * DynamoDB metadata-related errors
 */
export class MetadataError extends DynamoError {
  constructor(code: ErrorCode, message: string, context: ErrorContext = {}) {
    super(code, message, context);
  }
}

/**
 * DynamoDB operation-related errors
 */
export class OperationError extends DynamoError {
  constructor(code: ErrorCode, message: string, context: ErrorContext = {}) {
    super(code, message, context);
  }
}

/**
 * Factory functions for common error scenarios
 */
export class DynamoErrorFactory {
  static invalidQueryType(queryType: string): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.INVALID_QUERY_TYPE,
      `Invalid query type: ${queryType}`
    );
  }
  static keyTypeNotSupported(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.KEY_TYPE_NOT_SUPPORTED,
      'Key type not supported'
    );
  }
  static SKNotDefinedInSchema(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.SK_NOT_DEFINED_IN_SCHEMA,
      'SK is not defined in the schema'
    );
  }
  static keyNotIncludedInSK(key: string): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.KEY_NOT_INCLUDED_IN_SK,
      `Key "${key}" is not included in the SK`
    );
  }
  static firstKeyNotIncludedInSK(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.FIRST_KEY_NOT_INCLUDED_IN_SK,
      'The first key is not included in the SK'
    );
  }
  static SKEmpty(): ConfigurationError {
    return new ConfigurationError(ErrorCode.SK_EMPTY, 'SK is empty');
  }
  static keyPartRequired(keyPart: string): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.KEY_PART_REQUIRED,
      `Key part "${keyPart}" is required`
    );
  }
  static itemNotFound(tableName?: string): MetadataError {
    return new MetadataError(ErrorCode.ITEM_DOES_NOT_EXIST, 'Item not found', {
      tableName,
    });
  }
  static handleDeleteItemError(error: any): void {
    if (error.message.includes('The conditional request failed')) {
      throw DynamoErrorFactory.itemDoesNotExist();
    }
    throw error;
  }

  static handlePutItemError(error: any): void {
    if (error.message.includes('The conditional request failed')) {
      throw DynamoErrorFactory.itemAlreadyExists();
    }
    throw error;
  }

  static handlePutItemBatchError(error: any): void {
    if (error.message.includes('ConditionalCheckFailed')) {
      throw DynamoErrorFactory.conditionalCheckFailed(error.message);
    }
    if (
      error.message ===
      'Transaction request cannot include multiple operations on one item'
    ) {
      throw DynamoErrorFactory.duplicateItemsInBatch(error.message);
    }
    throw error;
  }

  static handleUpdateItemError(error: any): void {
    if (error.message.includes('The conditional request failed')) {
      throw DynamoErrorFactory.itemDoesNotExist();
    }
    throw error;
  }

  static handleDeleteItemBatchError(error: any): void {
    if (
      error.message ===
      'Transaction request cannot include multiple operations on one item'
    ) {
      throw DynamoErrorFactory.duplicateItemsInBatch(error.message);
    }
    throw error;
  }

  static handleUpdateItemBatchError(error: any): void {
    if (error.message.includes('ConditionalCheckFailed')) {
      throw DynamoErrorFactory.conditionalCheckFailed(error.message);
    }
    if (
      error.message ===
      'Transaction request cannot include multiple operations on one item'
    ) {
      throw DynamoErrorFactory.duplicateItemsInBatch(error.message);
    }
    throw error;
  }
  /**
   * Creates a key schema required error
   */
  static keySchemaRequired(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.KEY_SCHEMA_REQUIRED,
      'Key schema is required'
    );
  }

  /**
   * Creates a primary key required error
   */
  static pkRequired(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.PK_REQUIRED,
      'Primary key (pk) is required'
    );
  }

  /**
   * Creates a sort key required error
   */
  static skRequired(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.SK_REQUIRED,
      'Sort key (sk) is required'
    );
  }

  static newDataRequired(): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.NEW_DATA_REQUIRED,
      'data to update should be provided'
    );
  }

  /**
   * Creates a separator required error
   */
  static separatorRequired(
    keyType: 'pk' | 'sk',
    keysLength: number
  ): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.SEPARATOR_REQUIRED,
      `Separator is required when ${keyType}.keys.length is greater than 1 (current: ${keysLength})`,
      { operation: 'key_validation' }
    );
  }

  /**
   * Creates a table name mismatch error
   */
  static tableNameMismatch(
    expected: string,
    actual: string
  ): ConfigurationError {
    return new ConfigurationError(
      ErrorCode.TABLE_NAME_MISMATCH,
      `Table name mismatch. Expected: ${expected}, Actual: ${actual}`,
      { tableName: actual }
    );
  }

  /**
   * Creates a batch limit exceeded error
   */
  static batchLimitExceeded(batchSize: number): ValidationError {
    return new ValidationError(
      ErrorCode.BATCH_LIMIT_EXCEEDED,
      `Batch write items limit is 25, but ${batchSize} items were provided`,
      { batchSize }
    );
  }

  /**
   * Creates an item already exists error
   */
  static itemAlreadyExists(): ValidationError {
    return new ValidationError(
      ErrorCode.ITEM_ALREADY_EXISTS,
      'Item already exists'
    );
  }

  /**
   * Creates an item does not exist error
   */
  static itemDoesNotExist(): ValidationError {
    return new ValidationError(
      ErrorCode.ITEM_DOES_NOT_EXIST,
      'Item does not exist'
    );
  }

  /**
   * Creates a duplicate items in batch error
   */
  static duplicateItemsInBatch(originalError: string): ValidationError {
    return new ValidationError(
      ErrorCode.DUPLICATE_ITEMS_IN_BATCH,
      'Batch contains items with the same partition key and sort key',
      { originalError }
    );
  }

  /**
   * Creates a conditional check failed error
   */
  static conditionalCheckFailed(originalError: string): ValidationError {
    return new ValidationError(
      ErrorCode.CONDITIONAL_CHECK_FAILED,
      'Some of the items already exist in the table',
      { originalError }
    );
  }

  /**
   * Creates a table info not found error
   */
  static tableInfoNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.TABLE_INFO_NOT_FOUND,
      'Table information not found',
      { tableName }
    );
  }

  /**
   * Creates a table name not found error
   */
  static tableNameNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.TABLE_NAME_NOT_FOUND,
      'Table name not found',
      { tableName }
    );
  }

  /**
   * Creates an item count not found error
   */
  static itemCountNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.ITEM_COUNT_NOT_FOUND,
      'Item count not found',
      { tableName }
    );
  }

  /**
   * Creates a key schema not found error
   */
  static keySchemaNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.KEY_SCHEMA_NOT_FOUND,
      'Key schema not found',
      { tableName }
    );
  }

  /**
   * Creates an attribute definitions not found error
   */
  static attributeDefinitionsNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.ATTRIBUTE_DEFINITIONS_NOT_FOUND,
      'Attribute definitions not found',
      { tableName }
    );
  }

  /**
   * Creates a global secondary indexes not found error
   */
  static globalSecondaryIndexesNotFound(tableName?: string): MetadataError {
    return new MetadataError(
      ErrorCode.GLOBAL_SECONDARY_INDEXES_NOT_FOUND,
      'Global secondary indexes not found',
      { tableName }
    );
  }

  /**
   * Creates a generic DynamoDB operation error
   */
  static dynamoDbError(
    message: string,
    originalError?: string,
    tableName?: string
  ): OperationError {
    return new OperationError(ErrorCode.DYNAMODB_ERROR, message, {
      originalError,
      tableName,
    });
  }

  /**
   * Creates a transaction error
   */
  static transactionError(
    message: string,
    originalError?: string
  ): OperationError {
    return new OperationError(ErrorCode.TRANSACTION_ERROR, message, {
      originalError,
      operation: 'transaction',
    });
  }

  /**
   * Creates a network error
   */
  static networkError(message: string, originalError?: string): OperationError {
    return new OperationError(ErrorCode.NETWORK_ERROR, message, {
      originalError,
      operation: 'network',
    });
  }

  /**
   * Creates an authentication error
   */
  static authenticationError(
    message: string,
    originalError?: string
  ): OperationError {
    return new OperationError(ErrorCode.AUTHENTICATION_ERROR, message, {
      originalError,
      operation: 'authentication',
    });
  }

  /**
   * Creates a throttling error
   */
  static throttlingError(
    message: string,
    originalError?: string
  ): OperationError {
    return new OperationError(ErrorCode.THROTTLING_ERROR, message, {
      originalError,
      operation: 'throttling',
    });
  }

  /**
   * Creates a validation error
   */
  static validationError(
    message: string,
    originalError?: string
  ): OperationError {
    return new OperationError(ErrorCode.VALIDATION_ERROR, message, {
      originalError,
      operation: 'validation',
    });
  }
}

/**
 * Error type guards for runtime type checking
 */
export function isDynamoError(error: any): error is DynamoError {
  return error instanceof DynamoError;
}

export function isConfigurationError(error: any): error is ConfigurationError {
  return error instanceof ConfigurationError;
}

export function isValidationError(error: any): error is ValidationError {
  return error instanceof ValidationError;
}

export function isMetadataError(error: any): error is MetadataError {
  return error instanceof MetadataError;
}

export function isOperationError(error: any): error is OperationError {
  return error instanceof OperationError;
}

/**
 * Utility function to determine error type from AWS SDK error messages
 */
export function categorizeAwsError(error: Error): ErrorCode {
  const message = error.message.toLowerCase();

  if (message.includes('conditional') || message.includes('already exists')) {
    return ErrorCode.CONDITIONAL_CHECK_FAILED;
  }

  if (
    message.includes('transaction') &&
    message.includes('multiple operations')
  ) {
    return ErrorCode.DUPLICATE_ITEMS_IN_BATCH;
  }

  if (message.includes('throttl') || message.includes('limit exceeded')) {
    return ErrorCode.THROTTLING_ERROR;
  }

  if (message.includes('validation') || message.includes('invalid')) {
    return ErrorCode.VALIDATION_ERROR;
  }

  if (message.includes('access denied') || message.includes('unauthorized')) {
    return ErrorCode.AUTHENTICATION_ERROR;
  }

  if (message.includes('network') || message.includes('timeout')) {
    return ErrorCode.NETWORK_ERROR;
  }

  return ErrorCode.DYNAMODB_ERROR;
}
