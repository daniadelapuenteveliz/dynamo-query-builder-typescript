// dynamo-client.ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Config, KeyRec, DataRec } from '../types/types';
import { Table } from '../table';

export class DynamoClient {
  private readonly client: DynamoDBClient;

  constructor(config?: Config) {
    this.client = new DynamoDBClient(config ?? {});
  }

  /**
   * Creates a strongly typed Table instance.
   * If you do not have SK or Data, simply omit them.
   */
  table<
    PK extends KeyRec,
    SK extends KeyRec | never = never,
    DataDto extends DataRec | never = never,
  >(
    tableName: string,
    keySchema: ConstructorParameters<typeof Table<PK, SK, DataDto>>[2] // infers the exact KeySchema<PK, SK> type
  ): Table<PK, SK, DataDto> {
    return new Table<PK, SK, DataDto>(this.client, tableName, keySchema);
  }

  /**
   * Returns the native DynamoDB client instance.
   */
  getClient(): DynamoDBClient {
    return this.client;
  }
}
