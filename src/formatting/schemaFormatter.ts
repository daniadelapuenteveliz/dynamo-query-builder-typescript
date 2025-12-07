// keySchemaHandler.ts
import { DynamoErrorFactory } from '../types/errors';
import {
  KeySchema,
  DynamoScalar,
  paginationResult,
  ItemKeysOf
} from '../types/types';
import { KeyRec, DataRec, KeyDef, ItemOf } from '../types/types';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

// ----------------------------- Implementation -----------------------------

export class SchemaFormatter<
  PK extends KeyRec,
  SK extends KeyRec,
  DataDto extends DataRec,
> {
  constructor(public keySchema: KeySchema) {
    if (!this.keySchema) throw DynamoErrorFactory.keySchemaRequired();
    if (!this.keySchema.pk) throw DynamoErrorFactory.pkRequired();

    if (this.keySchema.pk.keys.length > 1 && !this.keySchema.pk.separator) {
      throw DynamoErrorFactory.separatorRequired(
        'pk',
        this.keySchema.pk.keys.length
      );
    }
    if (this.keySchema.sk) {
      const sk = this.keySchema.sk as KeyDef<any>;
      if (sk.keys.length > 1 && !sk.separator) {
        throw DynamoErrorFactory.separatorRequired('sk', sk.keys.length);
      }
    }
  }

  // ------------------------------ Helpers of keys ------------------------------

  mustGet<T extends KeyRec>(
    obj: ItemOf<PK, SK, DataDto>,
    prop: keyof T
  ): DynamoScalar {
    const val = (obj as any)[prop];
    if (val === undefined || val === null) {
      throw DynamoErrorFactory.keyPartRequired(String(prop));
    }
    return val;
  }

  private formatKey<T extends KeyRec>(
    item: ItemOf<PK, SK, DataDto>,
    def: KeyDef<T>
  ): string {
    const sep = def.separator ?? '#';
    return def.keys
      .map(k => {
        const raw = this.mustGet<T>(item, k);
        const s = typeof raw === 'string' ? raw : String(raw);
        return s;
      })
      .join(sep);
  }

  formatPK(item: PK): string {
    return this.formatKey(
      { ...item } as ItemOf<PK, SK, DataDto>,
      this.keySchema.pk
    );
  }

  formatSK(item: SK): string {
    return this.formatKey(
      { ...item } as ItemOf<PK, SK, DataDto>,
      this.keySchema.sk as KeyDef<any>
    );
  }

  assertPartialOrderSKIsCorrect(sk: Partial<SK>): void {
    const skKeysInSchema = this.keySchema.sk?.keys ?? [];
    const keysInSK = Object.keys(sk);

    if (keysInSK.length === 0) {
      throw DynamoErrorFactory.SKEmpty();
    }

    const orderedKeysInSK = keysInSK.sort((a, b) => {
      return skKeysInSchema.indexOf(a) - skKeysInSchema.indexOf(b);
    });

    if (orderedKeysInSK[0] != skKeysInSchema[0]) {
      throw DynamoErrorFactory.firstKeyNotIncludedInSK();
    }

    for (let i = 1; i < skKeysInSchema.length; i++) {
      if (orderedKeysInSK.length == i) {
        break;
      }
      if (orderedKeysInSK[i] !== skKeysInSchema[i]) {
        throw DynamoErrorFactory.keyNotIncludedInSK(skKeysInSchema[i]);
      }
    }
  }

  formatPartialOrderedSK(sk: Partial<SK>): string {
    this.assertPartialOrderSKIsCorrect(sk);
    const skKeysInSchema = this.keySchema.sk;
    if (!skKeysInSchema) {
      throw DynamoErrorFactory.SKNotDefinedInSchema();
    }
    const partialOrderedSKKeyDef = {
      name: skKeysInSchema.name,
      keys: Object.keys(sk),
      separator: skKeysInSchema.separator,
    };
    return this.formatKey(
      { ...sk } as ItemOf<PK, SK, DataDto>,
      partialOrderedSKKeyDef as KeyDef<any>
    );
  }

  getPK(): KeyDef<PK> {
    return this.keySchema.pk;
  }

  getSK(): KeyDef<SK> | undefined {
    return this.keySchema.sk;
  }

  /**
   * Converts the plain item into Record<string, AttributeValue>:
   * - Generates pk and sk (if applicable) from the schema.
   * - Skips rewriting key fields unless they are listed in `preserve`.
   */
  formatItemKeysDtoAsRecord(
    item: ItemKeysOf<PK, SK>
  ): Record<string, AttributeValue> {
    const logical: Record<string, DynamoScalar> = {};

    // pk
    logical[this.keySchema.pk.name] = this.formatKey(
      { ...item } as ItemOf<PK, SK, DataDto>,
      this.keySchema.pk
    );

    // sk (optional)
    if (this.keySchema.sk) {
      logical[(this.keySchema.sk as KeyDef<any>).name] = this.formatKey(
        { ...item } as ItemOf<PK, SK, DataDto>,
        this.keySchema.sk as KeyDef<any>
      );
    }

    return marshall(logical, { removeUndefinedValues: true });
  }

  /**
   * Converts the plain item into Record<string, AttributeValue>:
   * - Generates pk and sk (if applicable) from the schema.
   * - Skips rewriting key fields unless they are listed in `preserve`.
   */
  formatItemDtoAsRecord(
    item: ItemOf<PK, SK, DataDto>
  ): Record<string, AttributeValue> {
    const logical: Record<string, DynamoScalar> = {};

    // pk
    logical[this.keySchema.pk.name] = this.formatKey(item, this.keySchema.pk);

    // sk (optional)
    if (this.keySchema.sk) {
      logical[(this.keySchema.sk as KeyDef<any>).name] = this.formatKey(
        item,
        this.keySchema.sk as KeyDef<any>
      );
    }

    const avoidKeys = new Set<string>([
      ...this.keySchema.pk.keys.map(String),
      ...((this.keySchema.sk?.keys ?? []) as (keyof any)[]).map(String),
    ]);

    const preserveKeys = new Set<string>(
      (this.keySchema.preserve ?? []).map(String)
    );

    for (const key in item) {
      if (avoidKeys.has(key) && !preserveKeys.has(key)) continue;
      logical[key] = item[key];
    }

    return marshall(logical, { removeUndefinedValues: true });
  }

  formatRecordAsItemDto(
    record: Record<string, AttributeValue>
  ): ItemOf<PK, SK, DataDto> {
    // build pk
    const pk: Record<string, string> = {};
    const pkKey = this.keySchema.pk.name;
    const pkInRecord = record[pkKey].toString();
    if (this.keySchema.pk.separator) {
      const pkKeyValueParts: string[] = pkInRecord.split(
        this.keySchema.pk.separator
      );
      const pkKeyParts = this.keySchema.pk.keys;
      for (let i = 0; i < pkKeyParts.length; i++) {
        pk[pkKeyParts[i]] = pkKeyValueParts[i];
      }
    } else {
      pk[pkKey] = pkInRecord;
    }

    // build sk
    const sk: Record<string, string> = {};
    if (this.keySchema.sk) {
      const skKey = this.keySchema.sk.name;
      const skInRecord = record[skKey].toString();
      if (this.keySchema.sk.separator) {
        const skKeyValueParts: string[] = skInRecord.split(
          this.keySchema.sk.separator
        );
        const skKeyParts = this.keySchema.sk.keys;
        for (let i = 0; i < skKeyParts.length; i++) {
          sk[skKeyParts[i]] = skKeyValueParts[i];
        }
      } else {
        sk[skKey] = skInRecord;
      }
    }

    // build data
    const data: Record<string, AttributeValue> = {};
    for (const key in record) {
      if (key === this.keySchema.pk.name || key === this.keySchema.sk?.name) {
        continue;
      }
      data[key] = record[key];
    }
    return { ...pk, ...sk, ...data } as ItemOf<PK, SK, DataDto>;
  }

  formatEmptyPaginationResult(
    direction: 'forward' | 'backward',
    lastEvaluatedKey: KeyRec | undefined,
  ): paginationResult {
    return {
      items: [],
      lastEvaluatedKey: lastEvaluatedKey,
      firstEvaluatedKey: lastEvaluatedKey,
      count: 0,
      hasNext: (lastEvaluatedKey)?true:false,
      hasPrevious: (lastEvaluatedKey)?true:false,
      direction: direction,
    };
  }

  formatPKFromItem(item: ItemOf<PK, SK, DataDto>): PK {
    const pk = this.getPK();
    const pkKeys = pk.keys;
    const pkFormatted: Partial<PK> = {};
    for (const key of pkKeys) {
      pkFormatted[key] = item[key];
    }
    return pkFormatted as PK;
  }

  formatSKFromItem(item: ItemOf<PK, SK, DataDto>): SK {
    const sk = this.getSK();
    if (!sk) {
      throw DynamoErrorFactory.SKNotDefinedInSchema();
    }
    const skKeys = sk.keys;
    const skFormatted: any = {};
    for (const key of skKeys) {
      skFormatted[key] = item[key];
    }
    return skFormatted as SK;
  }

  formatPaginationResult(
    items: ItemOf<PK, SK, DataDto>[],
    limit: number,
    direction: 'forward' | 'backward',
    lastEvaluatedKey: KeyRec | undefined,
    oldLastEvaluatedKey: KeyRec | undefined,
  ): paginationResult {
    if (items.length === 0) return this.formatEmptyPaginationResult(direction,lastEvaluatedKey);
    let hasNext = false;
    let hasPrevious = false;
    if (oldLastEvaluatedKey) {
      hasPrevious = true;
    }
    if (lastEvaluatedKey || (items.length === limit)) {
      hasNext = true;
    }
    if (items.length === limit) {
      items.pop();
    }

    return {
      items: items,
      lastEvaluatedKey: items[items.length - 1],
      firstEvaluatedKey: items[0],
      count: items.length,
      hasNext: hasNext,
      hasPrevious: hasPrevious,
      direction: direction,
    };
  }
}
