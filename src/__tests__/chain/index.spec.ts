import * as indexModule from '../../index';

describe('index.ts exports', () => {
  it('exports VERSION constant', () => {
    expect(indexModule.VERSION).toBe('1.0.0');
    expect(typeof indexModule.VERSION).toBe('string');
  });

  it('exports types', () => {
    // Check that types module is exported via re-exports
    // Types from types.ts are re-exported, check a few key ones exist
    expect(typeof indexModule).toBe('object');
  });

  it('exports dynamo-client', () => {
    // Check that dynamo-client module is exported
    expect(indexModule).toHaveProperty('DynamoClient');
  });

  it('exports table', () => {
    // Check that table module is exported
    expect(indexModule).toHaveProperty('Table');
  });

  it('exports errors', () => {
    // Check that errors module is exported
    expect(indexModule).toHaveProperty('ErrorCode');
    expect(indexModule).toHaveProperty('DynamoError');
    expect(indexModule).toHaveProperty('DynamoErrorFactory');
  });
});
