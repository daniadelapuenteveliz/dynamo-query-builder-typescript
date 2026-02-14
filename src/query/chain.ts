import {
  AttributeValue,
  DynamoDBClient,
  QueryCommandOutput,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import {
  QueryCommand,
  QueryCommandInput,
  ScanCommand,
  ScanCommandInput,
} from '@aws-sdk/client-dynamodb';
import { SchemaFormatter } from '../formatting/schemaFormatter';
import {
  KeyRec,
  DataRec,
  ProjectDto,
  ItemKeysOf,
  rawFilterParams,
  ItemOf,
  CommandInput,
  FilterObject,
  paginationResult,
} from '../types/types';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { DynamoErrorFactory } from '../types/errors';

export class Chain<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> {
  constructor(
    public params: CommandInput,
    public queryType: string,
    public schemaFormatter: SchemaFormatter<PK, SK, DataDto>,
    private client: DynamoDBClient
  ) { }

  getClient(): DynamoDBClient {
    return this.client;
  }

  private buildSharedCommandInput(): CommandInput {
    const params: CommandInput = {
      TableName: this.params.TableName,
      Limit: this.params.Limit,
    };
    if (this.params.KeyConditionExpression) {
      params.KeyConditionExpression = this.params.KeyConditionExpression;
    }
    if (this.params.ExpressionAttributeNames) {
      params.ExpressionAttributeNames = this.params.ExpressionAttributeNames;
    }
    if (this.params.ExpressionAttributeValues) {
      params.ExpressionAttributeValues = this.params.ExpressionAttributeValues;
    }
    if (this.params.FilterExpression) {
      params.FilterExpression = this.params.FilterExpression;
    }
    if (this.params.ProjectionExpression) {
      params.ProjectionExpression = this.params.ProjectionExpression;
    }
    if (this.params.ExclusiveStartKey) {
      params.ExclusiveStartKey = this.params.ExclusiveStartKey;
    }
    if (this.params.Limit) {
      params.Limit = this.params.Limit;
    }
    if (this.params.IndexName) {
      params.IndexName = this.params.IndexName;
    }
    return params;
  }

  private buildQueryCommandInput(): QueryCommandInput {
    const sharedParams = this.buildSharedCommandInput();
    sharedParams.ScanIndexForward = this.params.ScanIndexForward ? true : false;
    if (this.params.KeyConditionExpression) {
      sharedParams.KeyConditionExpression = this.params.KeyConditionExpression;
    }
    return sharedParams as QueryCommandInput;
  }

  private buildScanCommandInput(): ScanCommandInput {
    const sharedParams = this.buildSharedCommandInput();
    return sharedParams as ScanCommandInput;
  }

  async run(): Promise<paginationResult> {
    this.params.Limit += 1;
    let result: QueryCommandOutput | ScanCommandOutput;
    if (this.queryType === 'query') {
      const params: QueryCommandInput = this.buildQueryCommandInput();
      result = await this.getClient().send(new QueryCommand(params));
    } else if (this.queryType === 'scan') {
      const params: ScanCommandInput = this.buildScanCommandInput();
      result = await this.getClient().send(new ScanCommand(params));
    } else {
      throw DynamoErrorFactory.invalidQueryType(this.queryType);
    }
    const unmarshalledItems =
      result.Items?.map((item: any) => unmarshall(item)) || [];
    const items: ItemOf<PK, SK, DataDto>[] = unmarshalledItems.map(
      (item: Record<string, AttributeValue>) =>
        this.schemaFormatter.formatRecordAsItemDto(item)
    );

    let newLastEvaluatedKey: KeyRec | undefined = undefined;
    if (result.LastEvaluatedKey) {
      newLastEvaluatedKey = this.schemaFormatter.formatRecordAsItemDto(
        unmarshall(result.LastEvaluatedKey)
      );
    }
    let oldLastEvaluatedKey: KeyRec | undefined = undefined;
    if (this.params.ExclusiveStartKey) {
      oldLastEvaluatedKey = this.schemaFormatter.formatRecordAsItemDto(
        unmarshall(this.params.ExclusiveStartKey)
      );
    }

    return this.schemaFormatter.formatPaginationResult(
      items,
      this.params.Limit,
      this.params.ScanIndexForward ? 'forward' : 'backward',
      newLastEvaluatedKey,
      oldLastEvaluatedKey
    );
  }

  project(projectDto: ProjectDto<DataDto>): Chain<PK, SK, DataDto> {
    const attributes = [];
    attributes.push(this.schemaFormatter.getPK().name);
    const sk = this.schemaFormatter.getSK();
    if (sk) {
      attributes.push(sk.name);
    }
    if (projectDto && projectDto.length > 0) {
      attributes.push(...projectDto);
    }

    // Build ExpressionAttributeNames to handle reserved words
    const names: Record<string, string> = {};
    const projectionParts: string[] = [];
    let counter = 0;

    for (const attr of attributes) {
      const safeKey = (attr as string).replace(/\./g, '_').replace(/#/g, '0');
      const namePlaceholder = `#attr_${safeKey}_${counter}`;
      names[namePlaceholder] = attr as string;
      projectionParts.push(namePlaceholder);
      counter++;
    }

    this.params.ProjectionExpression = projectionParts.join(', ');

    // Merge with existing ExpressionAttributeNames if any
    if (!this.params.ExpressionAttributeNames) {
      this.params.ExpressionAttributeNames = {};
    }
    this.params.ExpressionAttributeNames = {
      ...this.params.ExpressionAttributeNames,
      ...names,
    };

    return this;
  }

  pivot(pivot: KeyRec): Chain<PK, SK, DataDto> {
    const startKey = this.params.IndexName
      ? this.schemaFormatter.formatItemKeysWithIndexDtoAsRecord(
          pivot as ItemKeysOf<PK, SK>,
          this.params.IndexName
        )
      : this.schemaFormatter.formatItemKeysDtoAsRecord(
          pivot as ItemKeysOf<PK, SK>
        );
    this.params.ExclusiveStartKey = startKey;
    return this;
  }

  filterRaw(filter: rawFilterParams): Chain<PK, SK, DataDto> {
    if (!this.params.FilterExpression) {
      this.params.FilterExpression = filter.FilterExpression;
    } else {
      this.params.FilterExpression += ` AND (${filter.FilterExpression})`;
    }

    if (!this.params.ExpressionAttributeNames) {
      this.params.ExpressionAttributeNames = {};
    } else {
      this.params.ExpressionAttributeNames = {
        ...this.params.ExpressionAttributeNames,
        ...filter.ExpressionAttributeNames,
      };
    }
    if (!this.params.ExpressionAttributeValues) {
      this.params.ExpressionAttributeValues = filter.ExpressionAttributeValues;
    } else {
      this.params.ExpressionAttributeValues = {
        ...this.params.ExpressionAttributeValues,
        ...filter.ExpressionAttributeValues,
      };
    }
    return this;
  }

  filter(filterObject: FilterObject<DataDto>): Chain<PK, SK, DataDto> {
    const expressionParts: string[] = [];
    const names: Record<string, string> = {};
    const values: Record<string, AttributeValue> = {};

    let counter = 0;

    for (const key in filterObject) {
      const condition = filterObject[key];
      if (condition === undefined) continue;

      const safeKey = (key as string).replace(/\./g, '_'); // Handle dot notation in keys if needed for placeholders
      const namePlaceholder = `#${safeKey}_${counter}`;

      names[namePlaceholder] = key as string;

      if (
        typeof condition === 'object' &&
        condition !== null &&
        !Array.isArray(condition)
      ) {
        // Handle operators
        // We need to cast condition to any or Record to iterate safely because TS doesn't like iterating over union types or mapped types easily in this context
        const condObj = condition as Record<string, any>;
        for (const op in condObj) {
          const operator = op;
          const value = condObj[operator];

          if (value !== undefined) {
            const valuePlaceholder = `:val_${safeKey}_${counter}_${operator.replace(/[^a-zA-Z0-9]/g, '')}`;
            expressionParts.push(
              `${namePlaceholder} ${operator} ${valuePlaceholder}`
            );

            if (typeof value === 'number') {
              values[valuePlaceholder] = { N: value.toString() };
            } else if (typeof value === 'boolean') {
              values[valuePlaceholder] = { BOOL: value };
            } else if (value === null) {
              values[valuePlaceholder] = { NULL: true };
            } else {
              values[valuePlaceholder] = { S: value.toString() };
            }
          }
        }
      } else {
        // Equality
        const valuePlaceholder = `:val_${safeKey}_${counter}`;
        expressionParts.push(`${namePlaceholder} = ${valuePlaceholder}`);

        if (typeof condition === 'number') {
          values[valuePlaceholder] = { N: condition.toString() };
        } else if (typeof condition === 'boolean') {
          values[valuePlaceholder] = { BOOL: condition };
        } else if (condition === null) {
          values[valuePlaceholder] = { NULL: true };
        } else {
          values[valuePlaceholder] = { S: condition.toString() };
        }
      }
      counter++;
    }

    if (expressionParts.length > 0) {
      const rawParams: rawFilterParams = {
        FilterExpression: expressionParts.join(' AND '),
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      };
      return this.filterRaw(rawParams);
    }

    return this;
  }
}
