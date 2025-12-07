import { KeyRec, DataRec } from '../types/types';
import { Chain } from './chain';
import { DynamoErrorFactory } from '../types/errors';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SchemaFormatter } from '../formatting/schemaFormatter';
import { CommandInput } from '../types/types';

export class Query<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> extends Chain<PK, SK, DataDto> {

  constructor(params: CommandInput, schemaFormatter: SchemaFormatter<PK, SK, DataDto>, client: DynamoDBClient) {
    super(params, 'query', schemaFormatter, client);
  }

  private compare(skparams: Partial<SK>, operator: string): Query<PK, SK, DataDto> {
    if (!this.schemaFormatter.keySchema.sk) {
      throw DynamoErrorFactory.SKNotDefinedInSchema();
    }
    if (!this.params.ExpressionAttributeNames) {
      this.params.ExpressionAttributeNames = {};
    }
    if (!this.params.ExpressionAttributeValues) {
      this.params.ExpressionAttributeValues = {};
    }
    const skName = this.schemaFormatter.getSK()?.name ?? '';
    this.params.ExpressionAttributeNames['#sk'] = skName;

    const skValue = this.schemaFormatter.formatPartialOrderedSK(skparams);
    if (typeof skparams.value === 'number') {
      this.params.ExpressionAttributeValues[':sk'] = {
        N: skValue.toString(),
      };
    } else {
      this.params.ExpressionAttributeValues[':sk'] = {
        S: skValue.toString(),
      };
    }

    this.params.KeyConditionExpression += ` AND #sk ${operator} :sk`;
    return new Query(this.params, this.schemaFormatter, this.getClient());
  }

  whereSKBeginsWith(sk: Partial<SK>): Query<PK, SK, DataDto> {
    if (!this.schemaFormatter.keySchema.sk) {
      throw DynamoErrorFactory.SKNotDefinedInSchema();
    }
    if (!this.params.ExpressionAttributeNames) {
      this.params.ExpressionAttributeNames = {};
    }
    if (!this.params.ExpressionAttributeValues) {
      this.params.ExpressionAttributeValues = {};
    }
    const skName = this.schemaFormatter.getSK()?.name ?? '';
    this.params.ExpressionAttributeNames['#sk'] = skName;

    const skValue = this.schemaFormatter.formatPartialOrderedSK(sk);
    this.params.ExpressionAttributeValues[':sk'] = {
      S: skValue.toString(),
    };
    this.params.KeyConditionExpression += ' AND begins_with(#sk, :sk)';
    return new Query(this.params, this.schemaFormatter, this.getClient());
  }

  whereSKBetween(sk1: Partial<SK>, sk2: Partial<SK>): Query<PK, SK, DataDto> {
    if (!this.schemaFormatter.keySchema.sk) {
      throw DynamoErrorFactory.SKNotDefinedInSchema();
    }
    if (!this.params.ExpressionAttributeNames) {
      this.params.ExpressionAttributeNames = {};
    }
    if (!this.params.ExpressionAttributeValues) {
      this.params.ExpressionAttributeValues = {};
    }
    const skName = this.schemaFormatter.getSK()?.name ?? '';
    this.params.ExpressionAttributeNames['#sk'] = skName;
    const sk1Value = this.schemaFormatter.formatPartialOrderedSK(sk1);
    const sk2Value = this.schemaFormatter.formatPartialOrderedSK(sk2);
    this.params.ExpressionAttributeValues[':low'] = {
      S: sk1Value.toString(),
    };
    this.params.ExpressionAttributeValues[':high'] = {
      S: sk2Value.toString(),
    };
    this.params.KeyConditionExpression += ' AND #sk between :low and :high';
    return new Query(this.params, this.schemaFormatter, this.getClient());
  }

  whereSKGreaterThan(sk: Partial<SK>): Query<PK, SK, DataDto> {
    return this.compare(sk, '>');
  }

  whereSKLowerThan(sk: Partial<SK>): Query<PK, SK, DataDto> {
    return this.compare(sk, '<');
  }

  whereSKGreaterThanOrEqual(sk: Partial<SK>): Query<PK, SK, DataDto> {
    return this.compare(sk, '>=');
  }

  whereSKLowerThanOrEqual(sk: Partial<SK>): Query<PK, SK, DataDto> {
    return this.compare(sk, '<=');
  }

  whereSKequal(sk: Partial<SK>): Query<PK, SK, DataDto> {
    return this.compare(sk, '=');
  }

  sortAscending(): Chain<PK, SK, DataDto> {
    this.params.ScanIndexForward = true;
    return this;
  }

  sortDescending(): Chain<PK, SK, DataDto> {
    this.params.ScanIndexForward = false;
    return this;
  }
}
