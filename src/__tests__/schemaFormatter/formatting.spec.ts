import { SchemaFormatter } from '../../formatting/schemaFormatter';
import { ItemKeysOf, ItemOf, KeySchema } from '../../types/types';
import { marshall } from '@aws-sdk/util-dynamodb';
import { AttributeValue } from '@aws-sdk/client-dynamodb';

const attr = (value: string): AttributeValue =>
  ({
    toString: () => value,
  }) as unknown as AttributeValue;

type PK = {
  tenantId: string;
  userId: string;
};

type SK = {
  sort: string;
  timestamp: string;
};

type Data = {
  message: string;
  unread: boolean;
  attachments: string[];
  details: { nested: string };
};

type OnlyPk = {
  id: string;
  value: string;
  flag: string;
};

type dummySk = {};
type dummyData = {};

describe('formatting', () => {
  const schema: KeySchema = {
    pk: {
      name: 'pk',
      keys: ['tenantId', 'userId'],
      separator: '#',
    },
    sk: {
      name: 'sk',
      keys: ['sort', 'timestamp'],
      separator: '|',
    },
    preserve: ['userId'],
  };

  describe('constructor validation', () => {
    it('throws when key schema is missing', () => {
      expect(
        () =>
          new SchemaFormatter<any, any, any>(undefined as unknown as KeySchema)
      ).toThrow('Key schema is required');
    });

    it('throws when primary key definition is missing', () => {
      const invalidSchema = {} as KeySchema;
      expect(() => new SchemaFormatter<any, any, any>(invalidSchema)).toThrow(
        'Primary key (pk) is required'
      );
    });

    it('throws when composite pk has no separator', () => {
      const invalidSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId', 'userId'],
        },
      };

      expect(() => new SchemaFormatter<any, any, any>(invalidSchema)).toThrow(
        'Separator is required when pk.keys.length is greater than 1 (current: 2)'
      );
    });

    it('throws when composite sk has no separator', () => {
      const invalidSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId'],
        },
        sk: {
          name: 'sk',
          keys: ['sort', 'timestamp'],
        },
      };

      expect(() => new SchemaFormatter<any, any, any>(invalidSchema)).toThrow(
        'Separator is required when sk.keys.length is greater than 1 (current: 2)'
      );
    });
  });

  describe('partials without sk in schema', () => {
    it('assertPartialOrderSKIsCorrect runs when schema has no sk and throws accordingly', () => {
      const simpleSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const formatter = new SchemaFormatter<
        { id: string },
        Record<string, string>,
        Data
      >(simpleSchema);
      // Non-empty partial to skip "SK is empty" and reach ordering checks that rely on sk?.keys ?? []
      expect(() =>
        formatter.assertPartialOrderSKIsCorrect({ any: 'value' } as any)
      ).toThrow('The first key is not included in the SK');
    });
  });

  describe('format key accessors', () => {
    it('returns formatted keys and underlying schema definitions', () => {
      const keyedSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId', 'userId'],
          separator: ':',
        },
        sk: {
          name: 'sk',
          keys: ['sort'],
        },
      };

      type SimpleSK = { sort: string };
      const schemaFormatter = new SchemaFormatter<PK, SimpleSK, Data>(
        keyedSchema
      );

      const pk = schemaFormatter.formatPK({
        tenantId: 'tenant',
        userId: 'user',
      });
      const sk = schemaFormatter.formatSK({ sort: 'MSG' });

      expect(pk).toBe('tenant:user');
      expect(sk).toBe('MSG');
      expect(schemaFormatter.getPK()).toBe(keyedSchema.pk);
      expect(schemaFormatter.getSK()).toBe(keyedSchema.sk);
    });
  });

  describe('mustGet', () => {
    it('mustGet throws when the property is missing', () => {
      const schemaFormatter = new SchemaFormatter<PK, SK, Data>(schema);
      const item = {
        tenantId: 'tenant',
      };

      expect(() =>
        schemaFormatter.mustGet(item as ItemOf<PK, SK, Data>, 'userId')
      ).toThrow('Key part "userId" is required');
    });
  });

  describe('formatItemDtoAsRecord', () => {
    it('removes key fields except those preserved and marshalls values', () => {
      const schemaFormatter = new SchemaFormatter<PK, SK, Data>(schema);
      const item = {
        tenantId: 'tenant',
        userId: 'user',
        sort: 'MSG',
        timestamp: '1700000000000',
        message: 'hello world',
        unread: false,
        attachments: ['a', 'b'],
        details: { nested: 'value' },
      } as PK & SK & Data & Record<string, unknown>;

      const record = schemaFormatter.formatItemDtoAsRecord(item as any);

      const expected = marshall(
        {
          pk: 'tenant#user',
          sk: 'MSG|1700000000000',
          message: 'hello world',
          unread: false,
          attachments: ['a', 'b'],
          details: { nested: 'value' },
          userId: 'user',
        },
        { removeUndefinedValues: true }
      );

      expect(record).toEqual(expected);
      expect(record).not.toHaveProperty('tenantId');
      expect(record).not.toHaveProperty('sort');
      expect(record).not.toHaveProperty('timestamp');
    });

    it('works without a sort key schema', () => {
      const simpleSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const schemaFormatter = new SchemaFormatter<OnlyPk, dummySk, dummyData>(
        simpleSchema
      );

      const item: OnlyPk = {
        id: 'abc',
        value: '42',
        flag: 'true',
      };

      const record = schemaFormatter.formatItemDtoAsRecord(item as any);

      expect(record).toEqual(
        marshall(
          { pk: 'abc', value: "42", flag: "true" },
          { removeUndefinedValues: true }
        )
      );
    });

    it('removes undefined values while marshalling', () => {
      const simpleSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const schemaFormatter = new SchemaFormatter<
        { id: string },
        dummySk,
        { label?: string }
      >(simpleSchema);

      const item = {
        id: '123',
        label: undefined,
        keep: 'value',
      } as Record<string, unknown>;

      const record = schemaFormatter.formatItemDtoAsRecord(item as any);

      expect(record).toEqual(
        marshall({ pk: '123', keep: 'value' }, { removeUndefinedValues: true })
      );
      expect(record).not.toHaveProperty('label');
    });
  });

  describe('formatItemKeysDtoAsRecord', () => {
    it('marshalls composite key values into pk and sk attributes', () => {
      const schemaFormatter = new SchemaFormatter<PK, SK, Data>(schema);

      const keys: ItemKeysOf<PK, SK> = {
        tenantId: 'tenant',
        userId: 'user',
        sort: 'MSG',
        timestamp: '1700000000000',
      };

      const record = schemaFormatter.formatItemKeysDtoAsRecord(keys);

      expect(record).toEqual(
        marshall(
          {
            pk: 'tenant#user',
            sk: 'MSG|1700000000000',
          },
          { removeUndefinedValues: true }
        )
      );
    });

    it('handles schemas without a sort key', () => {
      const simpleSchema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const schemaFormatter = new SchemaFormatter<
        { id: string },
        dummySk,
        dummyData
      >(simpleSchema);

      const record = schemaFormatter.formatItemKeysDtoAsRecord({ id: 'abc' });

      expect(record).toEqual(
        marshall(
          {
            pk: 'abc',
          },
          { removeUndefinedValues: true }
        )
      );
    });
  });

  describe('formatRecordAsItemDto', () => {
    afterEach(() => {
      jest.clearAllMocks();
    });

    it('splits composite pk and sk using separators and merges attributes', () => {
      const schema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId', 'userId'],
          separator: '#',
        },
        sk: {
          name: 'sk',
          keys: ['sort', 'timestamp'],
          separator: '|',
        },
        preserve: ['userId'],
      };
      const schemaFormatter = new SchemaFormatter<PK, SK, Data>(schema);

      const record: Record<string, AttributeValue> = {
        pk: attr('tenant#user'),
        sk: attr('purchase|1234'),
        status: attr('shipped'),
        attempts: attr('3'),
      };

      const item = schemaFormatter.formatRecordAsItemDto(record);

      expect(item.tenantId).toBe('tenant');
      expect(item.userId).toBe('user');
      expect(item.sort).toBe('purchase');
      expect(item.timestamp).toBe('1234');
      expect((item as any).status?.toString?.()).toBe('shipped');
      expect((item as any).attempts?.toString?.()).toBe('3');
    });

    it('handles single-key pk and sk without separators', () => {
      const schema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['tenantId'],
        },
        sk: {
          name: 'sk',
          keys: ['sortKey'],
        },
      };
      const schemaFormatter = new SchemaFormatter<
        { tenantId: string },
        { sortKey: string },
        Data
      >(schema);

      const record: Record<string, AttributeValue> = {
        pk: attr('tenant-123'),
        sk: attr('latest'),
        status: attr('archived'),
      };

      const item = schemaFormatter.formatRecordAsItemDto(record);

      expect((item as any).pk).toBe('tenant-123');
      expect((item as any).sk).toBe('latest');
      expect((item as any).status?.toString?.()).toBe('archived');
    });

    it('handles schemas without sort keys', () => {
      const schema: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const schemaFormatter = new SchemaFormatter<
        { id: string },
        dummySk,
        dummyData
      >(schema);

      const record: Record<string, AttributeValue> = {
        pk: attr('abc'),
        status: attr('active'),
      };

      const item = schemaFormatter.formatRecordAsItemDto(record);

      expect((item as any).pk).toBe('abc');
      expect((item as any).status?.toString?.()).toBe('active');
    });
  });

  describe('pagination helpers', () => {
    type SimplePK = { id: string };
    type SimpleData = { value?: string };
    const simpleSchema: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['id'],
      },
    };

    let formatter: SchemaFormatter<SimplePK, dummySk, SimpleData>;

    beforeEach(() => {
      formatter = new SchemaFormatter<SimplePK, dummySk, SimpleData>(
        simpleSchema
      );
    });

    it('formatEmptyPaginationResult returns consistent empty pagination structure', () => {
      const empty = formatter.formatEmptyPaginationResult('forward', undefined);

      expect(empty).toEqual({
        items: [],
        lastEvaluatedKey: undefined,
        firstEvaluatedKey: undefined,
        count: 0,
        hasNext: false,
        hasPrevious: false,
        direction: 'forward',
      });
    });

    it('formatPaginationResult delegates to empty formatter when no items', () => {
      const spy = jest.spyOn(formatter as any, 'formatEmptyPaginationResult');
      const result = formatter.formatPaginationResult(
        [], 
        5, 
        'forward',
        undefined,
        undefined,
      );

      expect(spy).toHaveBeenCalledWith('forward', undefined);
      expect(result).toEqual({
        items: [],
        lastEvaluatedKey: undefined,
        firstEvaluatedKey: undefined,
        count: 0,
        hasNext: false,
        hasPrevious: false,
        direction: 'forward',
      });
    });

    it('formatPaginationResult tracks navigation flags and trims extra item for next page', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const result = formatter.formatPaginationResult(
        items.slice(), 
        3,
        'backward',
        { id: '3' },
        { id: '1' },
      );

      expect(result.direction).toBe('backward');
      expect(result.hasPrevious).toBe(true);
      expect(result.hasNext).toBe(true);
      expect(result.count).toBe(2);
      expect(result.items).toEqual([{ id: '1' }, { id: '2' }]);
      expect(result.firstEvaluatedKey).toEqual({ id: '1' });
      expect(result.lastEvaluatedKey).toEqual({ id: '2' });
    });

    it('formatPaginationResult preserves items and flags when under limit without pivot', () => {
      const items = [{ id: '100' }, { id: '101' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        3,
        'forward',
        { id: '101' },
        undefined,
      );

      expect(result.direction).toBe('forward');
      expect(result.hasPrevious).toBe(false);
      expect(result.hasNext).toBe(true);
      expect(result.count).toBe(2);
      expect(result.items).toEqual(items);
      expect(result.firstEvaluatedKey).toEqual({ id: '100' });
      expect(result.lastEvaluatedKey).toEqual({ id: '101' });
    });

    it('formatPaginationResult sets hasNext and pops item when items.length exactly equals limit', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        3,
        'forward',
        { id: '3' },
        undefined,
      );

      expect(result.hasNext).toBe(true);
      expect(result.hasPrevious).toBe(false);
      expect(result.count).toBe(2);
      expect(result.items).toEqual([{ id: '1' }, { id: '2' }]);
    });

    it('formatPaginationResult sets hasNext and pops item when items.length exceeds limit', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }];
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        4,
        'forward',
        { id: '4' },
        undefined,
      );

      expect(result.hasNext).toBe(true);
      expect(result.hasPrevious).toBe(false);
      expect(result.count).toBe(3);
      expect(result.items).toEqual([{ id: '1' }, { id: '2' }, { id: '3' }]);
      expect(result.items.length).toBe(3);
    });

    it('formatPaginationResult does not set hasNext when items.length is less than limit', () => {
      const items = [{ id: '1' }, { id: '2' }];
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        5,
        'forward',
        { id: '2' },
        undefined,
      );

      expect(result.hasNext).toBe(true);
      expect(result.hasPrevious).toBe(false);
      expect(result.count).toBe(2);
      expect(result.items).toEqual(items);
      expect(result.items.length).toBe(2);
      expect(itemsCopy.length).toBe(2); // Array was not mutated
    });

    it('formatPaginationResult handles single item with limit 1', () => {
      const items = [{ id: '1' }];
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        1,
        'forward',
        { id: '1' },
        undefined,
      );

      expect(result.hasNext).toBe(true);
      expect(result.hasPrevious).toBe(false);
      expect(result.count).toBe(0);
      expect(result.items).toEqual([]);
    });

    it('formatPaginationResult sets hasPrevious when firstEvaluatedKey is provided', () => {
      const items = [{ id: '1' }, { id: '2' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        5,
        'forward',
        { id: '2' },
        { id: '0' },
      );

      expect(result.hasPrevious).toBe(true);
      expect(result.hasNext).toBe(true);
      expect(result.firstEvaluatedKey).toEqual({ id: '1' });
    });

    it('formatPaginationResult does not set hasPrevious when firstEvaluatedKey is undefined', () => {
      const items = [{ id: '1' }, { id: '2' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        5,
        'forward',
        { id: '2' },
        undefined,
      );

      expect(result.hasPrevious).toBe(false);
      expect(result.firstEvaluatedKey).toEqual({ id: '1' });
    });

    it('formatPaginationResult handles newlastEvaluatedKey as undefined', () => {
      const items = [{ id: '1' }, { id: '2' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        5,
        'forward',
        undefined,
        undefined,
      );

      expect(result.lastEvaluatedKey).toEqual({ id: '2' });
      expect(result.count).toBe(2);
    });

    it('formatPaginationResult handles newlastEvaluatedKey as defined', () => {
      const items = [{ id: '1' }, { id: '2' }];
      const lastKey = { id: '2' };
      const result = formatter.formatPaginationResult(
        items.slice(),
        5,
        'forward',
        lastKey,
        undefined,
      );

      expect(result.lastEvaluatedKey).toEqual(lastKey);
      expect(result.count).toBe(2);
    });

    it('formatPaginationResult works with backward direction', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        3,
        'backward',
        { id: '0' },
        { id: '2' },
      );

      expect(result.direction).toBe('backward');
      expect(result.hasPrevious).toBe(true);
      expect(result.hasNext).toBe(true);
      expect(result.count).toBe(2);
      expect(result.items).toEqual([{ id: '1' }, { id: '2' }]);
    });

    it('formatPaginationResult works with forward direction', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        3,
        'forward',
        { id: '0' },
        { id: '2' },
      );

      expect(result.direction).toBe('forward');
      expect(result.hasPrevious).toBe(true);
      expect(result.hasNext).toBe(true);
      expect(result.count).toBe(2);
      expect(result.items).toEqual([{ id: '1' }, { id: '2' }]);
    });

    it('formatPaginationResult handles large number of items exceeding limit', () => {
      const items = Array.from({ length: 10 }, (_, i) => ({ id: String(i) }));
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        10,
        'forward',
        undefined,
        { id: '9' },
      );

      expect(result.hasNext).toBe(true);
      expect(result.count).toBe(9);
      expect(result.items.length).toBe(9);
      expect(result.items).toEqual([
        { id: '0' },
        { id: '1' },
        { id: '2' },
        { id: '3' },
        { id: '4' },
        { id: '5' },
        { id: '6' },
        { id: '7' },
        { id: '8' },
      ]);
      expect(itemsCopy.length).toBe(9); // Array was mutated, one item popped
    });

    it('formatPaginationResult handles all flags combinations with firstEvaluatedKey and limit', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const result = formatter.formatPaginationResult(
        items.slice(),
        3,
        'forward',
        { id: '3' },
        { id: '0' },
      );

      expect(result.hasPrevious).toBe(true);
      expect(result.hasNext).toBe(true);
      expect(result.firstEvaluatedKey).toEqual({ id: '1' });
      expect(result.lastEvaluatedKey).toEqual({ id: '2' });
      expect(result.count).toBe(2);
    });

    it('formatPaginationResult handles empty items array with different directions', () => {
      const forwardResult = formatter.formatPaginationResult(
        [],
        5,
        'forward',
        undefined,
        undefined,
      );
      const backwardResult = formatter.formatPaginationResult(
        [],
        5,
        'backward',
        { id: '0' },
        undefined,
      );

      expect(forwardResult.direction).toBe('forward');
      expect(backwardResult.direction).toBe('backward');
      expect(forwardResult.items).toEqual([]);
      expect(backwardResult.items).toEqual([]);
      expect(forwardResult.count).toBe(0);
      expect(backwardResult.count).toBe(0);
    });

    it('formatPaginationResult correctly counts items after pop operation', () => {
      const items = [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }];
      const itemsCopy = items.slice();
      const result = formatter.formatPaginationResult(
        itemsCopy,
        4,
        'forward',
        undefined,
        { id: '4' },
      );

      // Count should reflect the items after pop
      expect(result.count).toBe(3);
      expect(result.items.length).toBe(3);
      expect(result.count).toBe(result.items.length);
    });
  });

  describe('partials', () => {
    const schema2: KeySchema = {
      pk: {
        name: 'pk',
        keys: ['tenantId', 'userId'],
        separator: '#',
      },
      sk: {
        name: 'sk',
        keys: ['A', 'B', 'C', 'D', 'E'],
        separator: '#',
      },
      preserve: ['userId'],
    };

    type SK2 = {
      A: string;
      B: string;
      C: string;
      D: string;
      E: string;
    };

    const schemaFormatter = new SchemaFormatter<PK, SK2, Data>(schema2);
    describe('assertPartialOrderSKIsCorrect', () => {
      it('throws when the first key is not included', () => {
        expect(() => schemaFormatter.assertPartialOrderSKIsCorrect({})).toThrow(
          'SK is empty'
        );
      });

      it('throws when the key 2 is included but 1 is not', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({ B: 'b' })
        ).toThrow('The first key is not included in the SK');
      });

      it('throws when all keys are included but not the first one', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({
            B: 'b',
            C: 'c',
            D: 'd',
          })
        ).toThrow('The first key is not included in the SK');
      });

      it('if only key 1 is included it should return', () => {
        schemaFormatter.assertPartialOrderSKIsCorrect({ A: 'a' });
      });

      it('if only key 1 and 2 are included it should return', () => {
        schemaFormatter.assertPartialOrderSKIsCorrect({ A: 'a', B: 'b' });
      });

      it('if only key 1, 2, 3 are included it should return', () => {
        schemaFormatter.assertPartialOrderSKIsCorrect({
          A: 'a',
          B: 'b',
          C: 'c',
        });
      });

      it('if only key 1, 2, 3, 4 are included it should return', () => {
        schemaFormatter.assertPartialOrderSKIsCorrect({
          A: 'a',
          B: 'b',
          C: 'c',
          D: 'd',
        });
      });

      it('if only key 1, 2, 3, 4, 5 are included it should return', () => {
        schemaFormatter.assertPartialOrderSKIsCorrect({
          A: 'a',
          B: 'b',
          C: 'c',
          D: 'd',
          E: 'e',
        });
      });

      it('should throw error', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({ A: 'a', C: 'c' })
        ).toThrow('Key "B" is not included in the SK');
      });

      it('should throw error', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({ A: 'a', D: 'd' })
        ).toThrow('Key "B" is not included in the SK');
      });

      it('should throw error', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({
            A: 'a',
            B: 'b',
            D: 'd',
          })
        ).toThrow('Key "C" is not included in the SK');
      });

      it('should throw error', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({
            A: 'a',
            B: 'b',
            E: 'e',
          })
        ).toThrow('Key "C" is not included in the SK');
      });

      it('should throw error', () => {
        expect(() =>
          schemaFormatter.assertPartialOrderSKIsCorrect({
            A: 'a',
            B: 'b',
            C: 'c',
            E: 'e',
          })
        ).toThrow('Key "D" is not included in the SK');
      });
    });

    describe('formatPartialOrderedSK', () => {
      it('throws when the first key is not included', () => {
        expect(() => schemaFormatter.formatPartialOrderedSK({})).toThrow(
          'SK is empty'
        );
      });
      it('should return the formatted SK', () => {
        expect(schemaFormatter.formatPartialOrderedSK({ A: 'a' })).toBe('a');
      });
      it('should return the formatted SK', () => {
        expect(schemaFormatter.formatPartialOrderedSK({ A: 'a', B: 'b' })).toBe(
          'a#b'
        );
      });
      it('should return the formatted SK', () => {
        expect(
          schemaFormatter.formatPartialOrderedSK({ A: 'a', B: 'b', C: 'c' })
        ).toBe('a#b#c');
      });
      it('should return the formatted SK', () => {
        expect(
          schemaFormatter.formatPartialOrderedSK({
            A: 'a',
            B: 'b',
            C: 'c',
            D: 'd',
          })
        ).toBe('a#b#c#d');
      });
      it('should return the formatted SK', () => {
        expect(
          schemaFormatter.formatPartialOrderedSK({
            A: 'a',
            B: 'b',
            C: 'c',
            D: 'd',
            E: 'e',
          })
        ).toBe('a#b#c#d#e');
      });
      it('should throw error', () => {
        expect(() =>
          schemaFormatter.formatPartialOrderedSK({ A: 'a', B: 'b', D: 'd' })
        ).toThrow('Key "C" is not included in the SK');
      });

      it('throws "SK is not defined" when schema has no sk', () => {
        const simpleSchema: KeySchema = {
          pk: {
            name: 'pk',
            keys: ['id'],
          },
        };
        const formatter = new SchemaFormatter<
          { id: string },
          Record<string, string>,
          Data
        >(simpleSchema);
        jest
          .spyOn(formatter as any, 'assertPartialOrderSKIsCorrect')
          .mockImplementation(() => {});
        expect(() =>
          formatter.formatPartialOrderedSK({ any: 'value' } as any)
        ).toThrow('SK is not defined');
      });
    });
  });

  describe('formatPKFromItem', () => {
    it('extracts PK from item', () => {
      const formatter = new SchemaFormatter<PK, SK, Data>(schema);
      const item: ItemOf<PK, SK, Data> = {
        tenantId: 'tenant1',
        userId: 'user1',
        sort: 'sort1',
        timestamp: '123',
        message: 'test',
        unread: true,
        attachments: [],
        details: { nested: 'value' },
      };

      const pk = formatter.formatPKFromItem(item);

      expect(pk).toEqual({
        tenantId: 'tenant1',
        userId: 'user1',
      });
    });
  });

  describe('formatSKFromItem', () => {
    it('extracts SK from item', () => {
      const formatter = new SchemaFormatter<PK, SK, Data>(schema);
      const item: ItemOf<PK, SK, Data> = {
        tenantId: 'tenant1',
        userId: 'user1',
        sort: 'sort1',
        timestamp: '123',
        message: 'test',
        unread: true,
        attachments: [],
        details: { nested: 'value' },
      };

      const sk = formatter.formatSKFromItem(item);

      expect(sk).toEqual({
        sort: 'sort1',
        timestamp: '123',
      });
    });

    it('throws error when SK is not defined in schema', () => {
      const schemaWithoutSk: KeySchema = {
        pk: {
          name: 'pk',
          keys: ['id'],
        },
      };
      const formatter = new SchemaFormatter<OnlyPk, dummySk, dummyData>(schemaWithoutSk);
      const item: ItemOf<OnlyPk, dummySk, dummyData> = {
        id: 'id1',
        value: 'value1',
        flag: 'flag1',
      };

      expect(() => {
        formatter.formatSKFromItem(item);
      }).toThrow();
    });
  });
});
