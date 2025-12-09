// table.ts
import {
  DynamoDBClient,
  DescribeTableCommandInput,
  DescribeTableCommand,
  AttributeValue,
  PutItemCommand,
  PutItemCommandOutput,
  PutItemCommandInput,
  UpdateItemCommand,
  UpdateItemCommandOutput,
  TransactWriteItemsCommand,
  TransactWriteItemsCommandOutput,
  UpdateItemCommandInput,
  DeleteItemCommandOutput,
  DeleteItemCommand,
  DeleteItemCommandInput,
} from '@aws-sdk/client-dynamodb';
import {
  GlobalSecondaryIndexDescription,
  AttributeDefinition,
  KeySchemaElement,
  DescribeTableCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { DynamoErrorFactory } from './types/errors';
import { KeySchema, SearchParams } from './types/types';
import { Query } from './query/query';
import { Scan } from './query/scan';
import {
  ScanParams,
  QueryParams,
  KeyRec,
  DataRec,
  ItemOf,
  UpdateItemInput,
  DeleteItemInput,
  paginationResult,
  CommandInput,
  SKCondition,
} from './types/types';
import { SchemaFormatter } from './formatting/schemaFormatter';

// ----------------------------- Implementation -----------------------------

export class Table<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> {
  private schemaFormatter: SchemaFormatter<PK, SK, DataDto>;
  constructor(
    private client: DynamoDBClient,
    private tableName: string,
    private keySchema: KeySchema
  ) {
    this.schemaFormatter = new SchemaFormatter<PK, SK, DataDto>(this.keySchema);
  }

  // ------------------------- Dynamo commands (private) -------------------------

  private async putItemInDynamo(
    item: ItemOf<PK, SK, DataDto>,
    override: boolean
  ): Promise<PutItemCommandOutput> {
    try {
      const itemAsRecord = this.schemaFormatter.formatItemDtoAsRecord(item);
      const cmd = new PutItemCommand(
        this.createPutItemCommandInputFromRecord(itemAsRecord, override)
      );
      return await this.client.send(cmd);
    } catch (error: any) {
      throw DynamoErrorFactory.handlePutItemError(error);
    }
  }

  private async transactionalPutItemsInDynamo(
    items: ItemOf<PK, SK, DataDto>[],
    override: boolean
  ): Promise<TransactWriteItemsCommandOutput | null> {
    try {
      if (items.length === 0) return null;
      if (items.length > 25) {
        throw DynamoErrorFactory.batchLimitExceeded(items.length);
      }
      const params = {
        TransactItems: items.map(item => ({
          Put: this.createPutItemCommandInputFromRecord(
            this.schemaFormatter.formatItemDtoAsRecord(item),
            override
          ),
        })),
      };
      return await this.client.send(new TransactWriteItemsCommand(params));
    } catch (error: any) {
      throw DynamoErrorFactory.handlePutItemBatchError(error);
    }
  }

  private async transactionalDeleteItemsInDynamo(
    deletes: {
      pk: PK;
      sk: SK extends never ? undefined : SK;
    }[]
  ): Promise<TransactWriteItemsCommandOutput | null> {
    try {
      if (deletes.length === 0) return null;
      if (deletes.length > 25) {
        throw DynamoErrorFactory.batchLimitExceeded(deletes.length);
      }

      const commandInputs: DeleteItemCommandInput[] = [];
      for (const item of deletes) {
        const pk = item.pk as PK;
        const sk = item.sk as SK;
        const record = this.schemaFormatter.formatItemDtoAsRecord({
          ...pk,
          ...sk,
        } as ItemOf<PK, SK, DataDto>);
        commandInputs.push(this.createDeleteItemCommandInputFromRecord(record));
      }

      const params = {
        TransactItems: commandInputs.map(item => ({
          Delete: {
            TableName: item.TableName,
            Key: item.Key,
          },
        })),
      };
      return await this.client.send(new TransactWriteItemsCommand(params));
    } catch (error: any) {
      throw DynamoErrorFactory.handleDeleteItemBatchError(error);
    }
  }

  private async transactionalUpdateItemsInDynamo(
    updates: {
      pk: PK;
      sk: SK extends never ? undefined : SK;
      newData: Partial<DataDto>;
    }[]
  ): Promise<TransactWriteItemsCommandOutput | null> {
    try {
      if (updates.length === 0) return null;
      if (updates.length > 25) {
        throw DynamoErrorFactory.batchLimitExceeded(updates.length);
      }

      const commandInputs: UpdateItemCommandInput[] = [];
      for (const item of updates) {
        const pk = item.pk as PK;
        const sk = item.sk as SK;
        const newData = item.newData as Partial<DataDto>;
        const record = this.schemaFormatter.formatItemDtoAsRecord({
          ...pk,
          ...sk,
          ...newData,
        } as ItemOf<PK, SK, DataDto>);
        commandInputs.push(this.createUpdateItemInputFromRecord(record));
      }

      const params = {
        TransactItems: commandInputs.map(item => ({
          Update: {
            TableName: item.TableName,
            ReturnValues: item.ReturnValues,
            Key: item.Key,
            ConditionExpression: item.ConditionExpression,
            UpdateExpression: item.UpdateExpression,
            ExpressionAttributeNames: item.ExpressionAttributeNames,
            ExpressionAttributeValues: item.ExpressionAttributeValues,
          },
        })),
      };
      return await this.client.send(new TransactWriteItemsCommand(params));
    } catch (error: any) {
      throw DynamoErrorFactory.handleUpdateItemBatchError(error);
    }
  }

  private async updateItemInDynamo(
    pk: PK,
    sk: SK,
    newData: Partial<DataDto>
  ): Promise<UpdateItemCommandOutput> {
    try {
      const itemAsRecord = this.schemaFormatter.formatItemDtoAsRecord({
        ...pk,
        ...sk,
        ...newData,
      } as ItemOf<PK, SK, DataDto>);
      const cmd = new UpdateItemCommand(
        this.createUpdateItemCommandInputFromRecord(itemAsRecord)
      );
      return await this.client.send(cmd);
    } catch (error: any) {
      throw DynamoErrorFactory.handleUpdateItemError(error);
    }
  }

  private async deleteItemInDynamo(
    pk: PK,
    sk: SK
  ): Promise<DeleteItemCommandOutput> {
    try {
      const itemAsRecord = this.schemaFormatter.formatItemDtoAsRecord({
        ...pk,
        ...sk,
      } as ItemOf<PK, SK, DataDto>);
      const cmd = new DeleteItemCommand(
        this.createDeleteItemCommandInputFromRecord(itemAsRecord)
      );
      return await this.client.send(cmd);
    } catch (error: any) {
      throw DynamoErrorFactory.handleDeleteItemError(error);
    }
  }

  // ------------------------------ Formatting ------------------------------------

  /**
   * Creates a PutItemCommandInput with a ConditionExpression when `override=false`.
   */
  private createPutItemCommandInputFromRecord(
    itemAsRecord: Record<string, AttributeValue>,
    override: boolean
  ): PutItemCommandInput {
    const input: PutItemCommandInput = {
      TableName: this.tableName,
      Item: itemAsRecord,
    };
    if (override) return input;

    // Key non-existence condition (concurrency-friendly)
    let condition = 'attribute_not_exists(#pk)';
    const names: Record<string, string> = {
      '#pk': this.schemaFormatter.getPK().name,
    };

    const sk = this.schemaFormatter.getSK();
    if (sk) {
      condition += ' AND attribute_not_exists(#sk)';
      names['#sk'] = sk.name;
    }

    input.ConditionExpression = condition;
    input.ExpressionAttributeNames = names;
    return input;
  }

  /*
   * Creates a UpdateItemCommandInput from a record
   */
  private createUpdateItemInputFromRecord(
    itemAsRecord: Record<string, AttributeValue>
  ): UpdateItemCommandInput {
    const Key: Record<string, AttributeValue> = {};
    const pkName = this.schemaFormatter.getPK().name;
    Key[pkName] = itemAsRecord[pkName];
    let ExpressionAttributeNames: Record<string, string> = {};
    let ConditionExpression = `attribute_exists(#pk)`;
    ExpressionAttributeNames[`#pk`] = pkName;
    let skName = '';
    const sk = this.schemaFormatter.getSK();
    if (sk) {
      skName = sk.name;
      Key[skName] = itemAsRecord[skName];
      ConditionExpression += ` AND attribute_exists(#sk)`;
      ExpressionAttributeNames[`#sk`] = skName;
    }
    let UpdateExpression = 'SET ';
    let ExpressionAttributeValues: Record<string, AttributeValue> = {};
    for (const key in itemAsRecord) {
      if (key === pkName || key === skName) {
        continue;
      }
      UpdateExpression += `#${key} = :${key}, `;
      ExpressionAttributeNames[`#${key}`] = key;
      ExpressionAttributeValues[`:${key}`] = itemAsRecord[key];
    }
    UpdateExpression = UpdateExpression.slice(0, -2);

    const input: UpdateItemInput = {
      TableName: this.tableName,
      ReturnValues: 'ALL_NEW',
      Key: Key,
      ConditionExpression: ConditionExpression,
      UpdateExpression: UpdateExpression,
      ExpressionAttributeNames: ExpressionAttributeNames,
      ExpressionAttributeValues: ExpressionAttributeValues,
    };
    return input;
  }

  /*
   * Creates a DeleteItemCommandInput from a record
   */
  private createDeleteItemInputFromRecord(
    itemAsRecord: Record<string, AttributeValue>
  ): DeleteItemCommandInput {
    const Key: Record<string, AttributeValue> = {};
    const pkName = this.schemaFormatter.getPK().name;
    Key[pkName] = itemAsRecord[pkName];
    let skName = '';
    const sk = this.schemaFormatter.getSK();
    if (sk) {
      skName = sk.name;
      Key[skName] = itemAsRecord[skName];
    }
    const input: DeleteItemInput = {
      TableName: this.tableName,
      Key: Key,
    };
    return input;
  }

  /**
   * Creates a PutItemCommandInput with a ConditionExpression when `override=false`.
   */
  private createUpdateItemCommandInputFromRecord(
    itemAsRecord: Record<string, AttributeValue>
  ): UpdateItemCommandInput {
    const input = this.createUpdateItemInputFromRecord(itemAsRecord);
    const commandInput: UpdateItemCommandInput = {
      TableName: input.TableName,
      ReturnValues: input.ReturnValues,
      Key: input.Key,
      ConditionExpression: input.ConditionExpression,
      UpdateExpression: input.UpdateExpression,
      ExpressionAttributeNames: input.ExpressionAttributeNames,
      ExpressionAttributeValues: input.ExpressionAttributeValues,
    };
    return commandInput;
  }

  /*
   * Creates a DeleteItemCommandInput from a record
   */
  private createDeleteItemCommandInputFromRecord(
    itemAsRecord: Record<string, AttributeValue>
  ): DeleteItemCommandInput {
    const input = this.createDeleteItemInputFromRecord(itemAsRecord);
    const commandInput: DeleteItemCommandInput = {
      TableName: input.TableName,
      Key: input.Key,
    };
    return commandInput;
  }

  /**
   * Applies a SK condition to a query
   */
  private applySKCondition(
    query: Query<PK, SK, DataDto>,
    skCondition: SKCondition<SK>
  ): Query<PK, SK, DataDto> {
    // Type guard to verify if it is an object with specific properties
    const isConditionObject = (obj: any): obj is Record<string, any> => {
      return typeof obj === 'object' && obj !== null && !Array.isArray(obj);
    };

    if (!isConditionObject(skCondition)) {
      // If not an object, it is a direct SK (backward compatible)
      return query.whereSKequal(skCondition as SK);
    }

    // Check each condition in priority order
    if ('equal' in skCondition && skCondition.equal !== undefined) {
      return query.whereSKequal(skCondition.equal as SK);
    }
    if ('greaterThan' in skCondition && skCondition.greaterThan !== undefined) {
      return query.whereSKGreaterThan(skCondition.greaterThan as SK);
    }
    if ('lowerThan' in skCondition && skCondition.lowerThan !== undefined) {
      return query.whereSKLowerThan(skCondition.lowerThan as SK);
    }
    if (
      'greaterThanOrEqual' in skCondition &&
      skCondition.greaterThanOrEqual !== undefined
    ) {
      return query.whereSKGreaterThanOrEqual(
        skCondition.greaterThanOrEqual as SK
      );
    }
    if (
      'lowerThanOrEqual' in skCondition &&
      skCondition.lowerThanOrEqual !== undefined
    ) {
      return query.whereSKLowerThanOrEqual(skCondition.lowerThanOrEqual as SK);
    }
    if ('beginsWith' in skCondition && skCondition.beginsWith !== undefined) {
      return query.whereSKBeginsWith(skCondition.beginsWith as Partial<SK>);
    }
    if ('between' in skCondition && skCondition.between !== undefined) {
      const between = skCondition.between as {
        from: Partial<SK>;
        to: Partial<SK>;
      };
      return query.whereSKBetween(between.from, between.to);
    }

    // If it does not match any known condition, assume it is a direct SK
    return query.whereSKequal(skCondition as SK);
  }

  // ------------------------------ PUT methods ------------------------------------

  /**
   * Inserts an item. If `override=false` (default), it fails when the key already exists.
   */
  async put(
    item: ItemOf<PK, SK, DataDto>,
    override: boolean = false
  ): Promise<PutItemCommandOutput> {
    return await this.putItemInDynamo(item, override);
  }

  /**
   * Inserts using a raw PutItemCommandInput (for power users).
   */
  async putRaw(putItemCommandInput: PutItemCommandInput) {
    return await this.client.send(new PutItemCommand(putItemCommandInput));
  }

  /**
   * Performs an atomic batch insert (max 25 items) using TransactWriteItems.
   */
  async putBatch(
    items: ItemOf<PK, SK, DataDto>[],
    override: boolean = false
  ): Promise<TransactWriteItemsCommandOutput | null> {
    return await this.transactionalPutItemsInDynamo(items, override);
  }

  // ------------------------------ Update methods ------------------------------------

  /**
   * update specific columns of an item (using newData), however if override is true, it will overwrite the entire item
   */
  async update(
    pk: PK,
    sk: SK,
    newData: Partial<DataDto>
  ): Promise<UpdateItemCommandOutput> {
    if (!pk) throw DynamoErrorFactory.pkRequired();
    if (this.schemaFormatter.getSK() && !sk)
      throw DynamoErrorFactory.skRequired();
    if (newData && Object.keys(newData).length === 0)
      throw DynamoErrorFactory.newDataRequired();
    return await this.updateItemInDynamo(pk, sk, newData);
  }

  /**
   * update an item using a raw UpdateItemCommandInput (for power users).
   */
  async updateRaw(
    updateItemCommandInput: UpdateItemCommandInput
  ): Promise<UpdateItemCommandOutput> {
    return await this.client.send(
      new UpdateItemCommand(updateItemCommandInput)
    );
  }

  /**
   * update a batch of items using a raw UpdateItemCommandInput (for power users).
   */
  async updateBatch(
    updates: {
      pk: PK;
      sk: SK extends never ? undefined : SK;
      newData: Partial<DataDto>;
    }[]
  ): Promise<TransactWriteItemsCommandOutput | null> {
    for (let i = 0; i < updates.length; i++) {
      const item = updates[i];
      if (!item.pk) throw DynamoErrorFactory.pkRequired();
      if (this.schemaFormatter.getSK() && !item.sk)
        throw DynamoErrorFactory.skRequired();
      if (item.newData && Object.keys(item.newData).length === 0)
        throw DynamoErrorFactory.newDataRequired();
    }
    return await this.transactionalUpdateItemsInDynamo(updates);
  }

  // ------------------------------ delete methods ------------------------------------

  /**
   * delete an item by pk and sk
   */
  async delete(pk: PK, sk: SK): Promise<DeleteItemCommandOutput> {
    if (!pk) throw DynamoErrorFactory.pkRequired();
    if (this.schemaFormatter.getSK() && !sk)
      throw DynamoErrorFactory.skRequired();
    return await this.deleteItemInDynamo(pk, sk);
  }

  /**
   * delete an item using a raw DeleteItemCommandInput (for power users).
   */
  async deleteRaw(
    deleteItemCommandInput: DeleteItemCommandInput
  ): Promise<DeleteItemCommandOutput> {
    return await this.client.send(
      new DeleteItemCommand(deleteItemCommandInput)
    );
  }

  /**
   * given a PK delete all items with that PK
   * this method might take a while to complete if there are a lot of items to delete
   */
  async deletePartition(pk: PK): Promise<void> {
    if (!pk) throw DynamoErrorFactory.pkRequired();

    const pkKeys = this.schemaFormatter.getPK().keys;
    const skDef = this.schemaFormatter.getSK();
    const skKeys = skDef?.keys || [];

    let hasMore = true;
    let lastEvaluatedKey: any = undefined;

    while (hasMore) {
      const qparams: QueryParams<PK, DataDto> = {
        pk: pk,
        limit: 25,
      };

      if (lastEvaluatedKey) {
        qparams.pagination = {
          pivot: lastEvaluatedKey,
          direction: 'forward',
        };
      }

      const batch = await this.getPartitionBatch(qparams);
      const items = batch.items;

      if (items.length === 0) {
        hasMore = false;
        break;
      }

      // Extract PK and SK from each item
      const deletes: {
        pk: PK;
        sk: SK extends never ? undefined : SK;
      }[] = [];

      for (const item of items) {
        // Extract PK fields
        const pkObj: any = {};
        for (const key of pkKeys) {
          pkObj[key] = (item as any)[key];
        }

        // Extract SK fields if SK exists
        if (skDef) {
          const skObj: any = {};
          for (const key of skKeys) {
            skObj[key] = (item as any)[key];
          }
          deletes.push({
            pk: pkObj as PK,
            sk: skObj as SK extends never ? undefined : SK,
          });
        } else {
          deletes.push({
            pk: pkObj as PK,
            sk: undefined as SK extends never ? undefined : SK,
          });
        }
      }

      // Delete in batches
      if (deletes.length > 0) {
        await this.deleteBatch(deletes);
      }

      // Check if there are more items
      hasMore = batch.hasNext;
      lastEvaluatedKey = batch.lastEvaluatedKey;
    }
  }

  /**
   * delete a batch of items using a raw DeleteItemCommandInput (for power users).
   */
  async deleteBatch(
    deletes: {
      pk: PK;
      sk: SK extends never ? undefined : SK;
    }[]
  ): Promise<TransactWriteItemsCommandOutput | null> {
    for (let i = 0; i < deletes.length; i++) {
      const item = deletes[i];
      if (!item.pk) throw DynamoErrorFactory.pkRequired();
      if (this.schemaFormatter.getSK() && !item.sk)
        throw DynamoErrorFactory.skRequired();
    }
    return await this.transactionalDeleteItemsInDynamo(deletes);
  }

  /**
   * delete by pk and sk (the sk is optional only if it does not exist in the schema) and can filter dynamo attributes
   * (with a while loop till a limit given by the user is reached or all the pk and sk are read).
   * @returns The number of items deleted
   */
  async deleteWithCondition(
    deleteParams: SearchParams<PK, SK, DataDto>
  ): Promise<number> {
    if (!deleteParams.pk) throw DynamoErrorFactory.pkRequired();
    const searchLimit = 25;
    let lastEvaluatedKey: KeyRec | undefined = undefined;
    let totalDeleted = 0;
    while (!deleteParams.limit || totalDeleted < deleteParams.limit) {
      const qparams: QueryParams<PK, DataDto> = {
        pk: deleteParams.pk,
        limit: searchLimit,
      };
      let query: Query<PK, SK, DataDto> = this.query(qparams);

      if (this.schemaFormatter.getSK() && deleteParams.skCondition) {
        query = this.applySKCondition(query, deleteParams.skCondition);
      }

      if (lastEvaluatedKey) {
        query.pivot(lastEvaluatedKey);
      }
      if (deleteParams.filter) {
        query.filter(deleteParams.filter);
      }

      query.sortAscending();

      const result = await query.run();
      if (result.items.length === 0) {
        break;
      }

      // Process items in batches and delete immediately
      let itemsToProcess = result.items;
      if (
        deleteParams.limit &&
        totalDeleted + result.items.length > deleteParams.limit
      ) {
        itemsToProcess = result.items.slice(
          0,
          deleteParams.limit - totalDeleted
        );
      }

      // Delete in batches
      const batchOfKeys = itemsToProcess.map(item => {
        const aux: any = {
          pk: this.schemaFormatter.formatPKFromItem(
            item as ItemOf<PK, SK, DataDto>
          ),
        };
        if (this.schemaFormatter.getSK()) {
          aux.sk = this.schemaFormatter.formatSKFromItem(
            item as ItemOf<PK, SK, DataDto>
          ) as SK extends never ? undefined : SK;
        }
        return aux as { pk: PK; sk: SK extends never ? undefined : SK };
      });

      try {
        await this.deleteBatch(batchOfKeys);
        totalDeleted += itemsToProcess.length;
      } catch (error) {
        // If batch deletion fails, throw error with context
        throw new Error(
          `Failed to delete batch: ${error instanceof Error ? error.message : String(error)}`
        );
      }

      if (!result.hasNext) {
        break;
      }

      lastEvaluatedKey = result.lastEvaluatedKey;
    }

    return totalDeleted;
  }

  /**
   * Deletes all items from the table using scan for pagination.
   * This method scans the entire table and deletes all items in batches.
   * @returns The number of items deleted
   */
  async flush(): Promise<number> {
    const scanLimit = 25;
    let lastEvaluatedKey: KeyRec | undefined = undefined;
    let totalDeleted = 0;
    let hasMore = true;

    while (hasMore) {
      const sparams: ScanParams<DataDto> = { limit: scanLimit };
      let scanInstance: Scan<PK, SK, DataDto> = this.scan(sparams);

      if (lastEvaluatedKey) {
        scanInstance = scanInstance.pivot(lastEvaluatedKey);
      }

      const result = await scanInstance.run();
      if (result.items.length === 0) {
        hasMore = false;
        break;
      }

      // Delete in batches
      const batchOfKeys = result.items.map(item => {
        const aux: any = {
          pk: this.schemaFormatter.formatPKFromItem(
            item as ItemOf<PK, SK, DataDto>
          ),
        };
        if (this.schemaFormatter.getSK()) {
          aux.sk = this.schemaFormatter.formatSKFromItem(
            item as ItemOf<PK, SK, DataDto>
          ) as SK extends never ? undefined : SK;
        }
        return aux as { pk: PK; sk: SK extends never ? undefined : SK };
      });

      try {
        await this.deleteBatch(batchOfKeys);
        totalDeleted += result.items.length;
      } catch (error) {
        // If batch deletion fails, throw error with context
        throw new Error(
          `Failed to delete batch during flush: ${error instanceof Error ? error.message : String(error)}`
        );
      }

      hasMore = result.hasNext;
      if (hasMore) {
        lastEvaluatedKey = result.lastEvaluatedKey;
      }
    }

    return totalDeleted;
  }

  // ------------------------------ READ methods ---------------------------------------

  /**
   * read an item by pk and sk
   */
  async getOne(
    pk: PK,
    sk: SK,
    IndexName?: string
  ): Promise<ItemOf<PK, SK, DataDto>> {
    if (!pk) throw DynamoErrorFactory.pkRequired();
    if (this.schemaFormatter.getSK() && !sk)
      throw DynamoErrorFactory.skRequired();
    const queryParams: QueryParams<PK, DataDto> = {
      pk: pk,
      limit: 1,
    };
    if (IndexName) {
      queryParams.IndexName = IndexName;
    }
    let query: Query<PK, SK, DataDto> = this.query(queryParams);
    if (this.schemaFormatter.getSK()) {
      query = query.whereSKequal(sk);
    }
    const result = await query.run();
    if (result && result.items.length === 0) {
      throw DynamoErrorFactory.itemNotFound(this.tableName);
    }
    return result.items[0] as ItemOf<PK, SK, DataDto>;
  }

  /**
   * get a partition of items by pk, default limit is 50
   */
  async getPartitionBatch(
    qparams: QueryParams<PK, DataDto>
  ): Promise<paginationResult> {
    let query: Query<PK, SK, DataDto> = this.query(qparams);
    if (
      !qparams.pagination ||
      !qparams.pagination.direction ||
      qparams.pagination.direction === 'forward'
    ) {
      query.sortAscending();
    } else {
      query.sortDescending();
    }
    if (qparams.pagination && qparams.pagination.pivot) {
      query.pivot(qparams.pagination.pivot);
    }
    if (qparams.project) {
      query.project(qparams.project);
    }
    return await query.run();
  }

  /**
   * search by pk and sk (the sk is optional only if it does not exist in the schema) and can filter dynamo attributes
   * (with a while loop till a limit given by the user is reached or all the pk and sk are read).
   */
  async search(
    searchParams: SearchParams<PK, SK, DataDto>
  ): Promise<ItemOf<PK, SK, DataDto>[]> {
    if (!searchParams.pk) throw DynamoErrorFactory.pkRequired();
    const searchLimit = 25;
    const filteredItems: ItemOf<KeyRec, KeyRec, DataRec>[] = [];
    let lastEvaluatedKey: KeyRec | undefined = undefined;
    let itemsGot = 0;
    while (!searchParams.limit || itemsGot < searchParams.limit) {
      const qparams: QueryParams<PK, DataDto> = {
        pk: searchParams.pk,
        limit: searchLimit,
      };
      let query: Query<PK, SK, DataDto> = this.query(qparams);

      if (this.schemaFormatter.getSK() && searchParams.skCondition) {
        query = this.applySKCondition(query, searchParams.skCondition);
      }

      if (lastEvaluatedKey) {
        query.pivot(lastEvaluatedKey);
      }
      if (searchParams.filter) {
        query.filter(searchParams.filter);
      }
      if (searchParams.project) {
        query.project(searchParams.project);
      }

      query.sortAscending();

      const result = await query.run();
      if (
        searchParams.limit &&
        itemsGot + result.items.length > searchParams.limit
      ) {
        filteredItems.push(
          ...result.items.slice(0, searchParams.limit - itemsGot)
        );
        break;
      }

      filteredItems.push(...result.items);
      itemsGot += result.items.length;

      if (!result.hasNext) {
        break;
      }
      lastEvaluatedKey = result.lastEvaluatedKey;
    }

    return filteredItems as ItemOf<PK, SK, DataDto>[];
  }

  query(qparams: QueryParams<PK, DataDto>): Query<PK, SK, DataDto> {
    const params: CommandInput = {
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': this.schemaFormatter.getPK().name },
      TableName: this.tableName,
      Limit: qparams.limit,
    };
    if (qparams.IndexName) {
      params.IndexName = qparams.IndexName;
    }
    const pkValue = this.schemaFormatter.formatPK({ ...qparams.pk } as PK);
    params.ExpressionAttributeValues = { ':pk': { S: pkValue.toString() } };
    const query = new Query(params, this.schemaFormatter, this.client);
    if (qparams.project) {
      query.project(qparams.project);
    }
    return query;
  }

  scan(sparams: ScanParams<DataDto>): Scan<PK, SK, DataDto> {
    const params: CommandInput = {
      TableName: this.tableName,
      Limit: sparams.limit,
    };
    if (sparams.IndexName) {
      params.IndexName = sparams.IndexName;
    }
    const scan = new Scan(params, this.schemaFormatter, this.client);
    if (sparams.project) {
      scan.project(sparams.project);
    }
    return scan;
  }

  queryRaw(commandInput: CommandInput): Query<PK, SK, DataDto> {
    if (commandInput.TableName && commandInput.TableName !== this.tableName) {
      throw DynamoErrorFactory.tableNameMismatch(
        this.tableName,
        commandInput.TableName
      );
    }
    return new Query<PK, SK, DataDto>(
      commandInput,
      this.schemaFormatter,
      this.client
    );
  }

  scanRaw(commandInput: CommandInput): Scan<PK, SK, DataDto> {
    if (commandInput.TableName && commandInput.TableName !== this.tableName) {
      throw DynamoErrorFactory.tableNameMismatch(
        this.tableName,
        commandInput.TableName
      );
    }
    return new Scan<PK, SK, DataDto>(
      commandInput,
      this.schemaFormatter,
      this.client
    );
  }
  // ------------------------------ Metadata ---------------------------------------

  getTableName(): string {
    return this.tableName;
  }

  getClient(): DynamoDBClient {
    return this.client;
  }

  async getTableNameInDynamo(): Promise<string> {
    const info = await this.describe();
    if (!info.Table) throw DynamoErrorFactory.tableInfoNotFound(this.tableName);
    if (!info.Table.TableName)
      throw DynamoErrorFactory.tableNameNotFound(this.tableName);
    return info.Table.TableName;
  }

  async getItemCount(): Promise<number> {
    const info = await this.describe();
    if (!info.Table) throw DynamoErrorFactory.tableInfoNotFound(this.tableName);
    if (info.Table.ItemCount === undefined) {
      throw DynamoErrorFactory.itemCountNotFound(this.tableName);
    }
    return info.Table.ItemCount;
  }

  async getDynamoKeySchema(): Promise<KeySchemaElement[]> {
    const info = await this.describe();
    if (!info.Table) throw DynamoErrorFactory.tableInfoNotFound(this.tableName);
    if (!info.Table.KeySchema)
      throw DynamoErrorFactory.keySchemaNotFound(this.tableName);
    return info.Table.KeySchema;
  }

  async getAttributeDefinitions(): Promise<AttributeDefinition[]> {
    const info = await this.describe();
    if (!info.Table) throw DynamoErrorFactory.tableInfoNotFound(this.tableName);
    if (!info.Table.AttributeDefinitions) {
      throw DynamoErrorFactory.attributeDefinitionsNotFound(this.tableName);
    }
    return info.Table.AttributeDefinitions;
  }

  async getGlobalSecondaryIndexes(): Promise<
    GlobalSecondaryIndexDescription[]
  > {
    const info = await this.describe();
    if (!info.Table) throw DynamoErrorFactory.tableInfoNotFound(this.tableName);
    if (!info.Table.GlobalSecondaryIndexes) {
      throw DynamoErrorFactory.globalSecondaryIndexesNotFound(this.tableName);
    }
    return info.Table.GlobalSecondaryIndexes;
  }

  async describe(): Promise<DescribeTableCommandOutput> {
    const params: DescribeTableCommandInput = { TableName: this.tableName };
    return await this.client.send(new DescribeTableCommand(params));
  }

  getKeySchema(): KeySchema {
    return this.keySchema;
  }
}
