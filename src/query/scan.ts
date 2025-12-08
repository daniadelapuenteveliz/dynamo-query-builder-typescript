import { KeyRec, DataRec } from '../types/types';
import { Chain } from './chain';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SchemaFormatter } from '../formatting/schemaFormatter';
import { CommandInput } from '../types/types';

export class Scan<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> extends Chain<PK, SK, DataDto> {
  constructor(
    params: CommandInput,
    schemaFormatter: SchemaFormatter<PK, SK, DataDto>,
    client: DynamoDBClient
  ) {
    super(params, 'scan', schemaFormatter, client);
  }
}
