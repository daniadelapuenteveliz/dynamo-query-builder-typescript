import { AttributeValue } from '@aws-sdk/client-dynamodb';

export interface Config {
  region?: string;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
  };
}

export interface Pagination {
  pivot?: KeyRec;
  direction?: 'forward' | 'backward';
}

export type ProjectDto<DataDto extends DataRec> = (keyof DataDto)[];

export interface Params<DataDto extends DataRec> {
  limit: number;
  IndexName?: string;
  pagination?: Pagination;
  project?: ProjectDto<DataDto>;
}

export interface ScanParams<DataDto extends DataRec> extends Params<DataDto> { }

export interface QueryParams<PK extends KeyRec, DataDto extends DataRec>
  extends Params<DataDto> {
  pk: PK;
}

export interface SearchParams<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> {
  pk: PK;
  skCondition?: SKCondition<SK>;
  filter?: FilterObject<DataDto>;
  IndexName?: string;
  pagination?: Pagination;
  project?: ProjectDto<DataDto>;
  limit?: number;
}
export interface rawFilterParams {
  FilterExpression: string;
  ExpressionAttributeNames: {
    [key: string]: string;
  };
  ExpressionAttributeValues: Record<string, AttributeValue>;
}

export type FilterOperator = '=' | '<>' | '<' | '<=' | '>' | '>=';
export type FilterValue = string | number | boolean | null;
export type FilterCondition =
  | FilterValue
  | { [key in FilterOperator]?: FilterValue };
export type FilterObject<DataDto> = {
  [key in keyof DataDto]?: FilterCondition;
};

export type SKCondition<SK extends KeyRec> =
  | SK // equal by default (backward compatible)
  | { equal: SK }
  | { greaterThan: SK }
  | { lowerThan: SK }
  | { greaterThanOrEqual: SK }
  | { lowerThanOrEqual: SK }
  | { beginsWith: Partial<SK> }
  | { between: { from: Partial<SK>; to: Partial<SK> } };

export interface KeySchemaItem {
  AttributeName: string;
  KeyType: string;
}

export interface AttributeDefinition {
  AttributeName: string;
  AttributeType: string;
}

export type DynamoScalar =
  | string
  | number
  | bigint
  | boolean
  | null
  | Buffer
  | Array<DynamoScalar>
  | Object;

interface DynamoList extends Array<DynamoValue> { }
type DynamoMap = { [key: string]: DynamoValue };
type DynamoValue = DynamoScalar | DynamoList | DynamoMap;
export interface KeySchema {
  pk: {
    name: string;
    keys: string[];
    separator?: string;
  };
  sk?: {
    name: string;
    keys: string[];
    separator?: string;
  };
  preserve?: string[];
}

// ----------------------------- aux types -----------------------------

export type KeyRec = Record<string, string>;
export type DataRec = Record<string, DynamoScalar>;

export type KeyDef<T extends KeyRec> = {
  name: string;
  keys: (keyof T)[];
  separator?: string;
};

export type KeysOf<
  PK extends KeyRec,
  SK extends KeyRec | never,
> = SK extends never ? PK : PK & SK;

export type ItemOf<
  PK extends KeyRec,
  SK extends KeyRec | never,
  Data extends DataRec | never,
> = KeysOf<PK, SK> & (Data extends never ? {} : Data);

export type ItemKeysOf<PK extends KeyRec, SK extends KeyRec | never> = KeysOf<
  PK,
  SK
>;

export type UpdateItemInput = {
  TableName: string;
  ReturnValues: 'ALL_NEW';
  Key: Record<string, AttributeValue>;
  ConditionExpression: string;
  UpdateExpression: string;
  ExpressionAttributeNames: Record<string, string>;
  ExpressionAttributeValues: Record<string, AttributeValue>;
};

export type DeleteItemInput = {
  TableName: string;
  Key: Record<string, AttributeValue>;
};

export type paginationResult = {
  items: ItemOf<KeyRec, KeyRec, DataRec>[];
  lastEvaluatedKey: KeyRec | undefined;
  firstEvaluatedKey: KeyRec | undefined;
  count: number;
  hasNext: boolean;
  hasPrevious: boolean;
  direction: 'forward' | 'backward';
};

export interface CommandInput {
  TableName: string;
  IndexName?: string;
  ScanIndexForward?: boolean;
  ProjectionExpression?: string;
  ExclusiveStartKey?: Record<string, AttributeValue>;
  KeyConditionExpression?: string;
  ExpressionAttributeNames?: {
    [key: string]: string;
  };
  Limit: number;
  ExpressionAttributeValues?: Record<string, AttributeValue>;
  FilterExpression?: string;
}
