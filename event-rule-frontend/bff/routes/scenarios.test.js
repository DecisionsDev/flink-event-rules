import { describe, test, expect, vi } from 'vitest';
import { createScenariosRouter } from './scenarios.js';

describe('Scenarios Router', () => {
  const isValidAgentId = (id) => typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);

  test('createScenariosRouter returns an Express router instance', () => {
    const router = createScenariosRouter({
      samplesDir: '/tmp/samples',
      isValidAgentId,
    });
    expect(router).toBeDefined();
    expect(typeof router).toBe('function'); // Express routers are callable middleware functions
  });

  test('validates useCase parameter correctly', () => {
    expect(isValidAgentId('banking-fraud-agent')).toBe(true);
    expect(isValidAgentId('airtraffic-agent')).toBe(true);
    expect(isValidAgentId('../etc/passwd')).toBe(false);
    expect(isValidAgentId('')).toBe(false);
    expect(isValidAgentId(null)).toBe(false);
  });
});
