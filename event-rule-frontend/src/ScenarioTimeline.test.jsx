import { describe, test, expect } from 'vitest';
import { formatDuration, extractTimestampMs } from './ScenarioTimeline';

describe('formatDuration', () => {
  test('returns "0s" for 0ms', () => {
    expect(formatDuration(0)).toBe('0s');
  });

  test('formats milliseconds when under 1 second', () => {
    expect(formatDuration(500)).toBe('500ms');
    expect(formatDuration(50)).toBe('50ms');
  });

  test('formats seconds', () => {
    expect(formatDuration(10000)).toBe('10s');
    expect(formatDuration(45000)).toBe('45s');
  });

  test('formats minutes and seconds', () => {
    expect(formatDuration(60000)).toBe('1min');
    expect(formatDuration(75000)).toBe('1min 15s');
    expect(formatDuration(190000)).toBe('3min 10s');
  });

  test('formats hours, minutes, and seconds', () => {
    expect(formatDuration(3600000)).toBe('1h');
    expect(formatDuration(3660000)).toBe('1h 1min');
    expect(formatDuration(3665000)).toBe('1h 1min 5s');
    expect(formatDuration(18190000)).toBe('5h 3min 10s');
  });

  test('formats days, hours, minutes, and seconds', () => {
    expect(formatDuration(86400000)).toBe('1d');
    expect(formatDuration(104590000)).toBe('1d 5h 3min 10s');
    expect(formatDuration(2 * 86400000 + 45 * 1000)).toBe('2d 45s');
  });

  test('returns null for invalid inputs', () => {
    expect(formatDuration(null)).toBeNull();
    expect(formatDuration(undefined)).toBeNull();
    expect(formatDuration(-100)).toBeNull();
    expect(formatDuration(NaN)).toBeNull();
    expect(formatDuration('100')).toBeNull();
  });
});

describe('extractTimestampMs', () => {
  test('extracts ISO 8601 string timestamp from payload', () => {
    const event = {
      payload: {
        timestamp: '2026-09-09T14:00:00Z',
      },
    };
    expect(extractTimestampMs(event)).toBe(Date.parse('2026-09-09T14:00:00Z'));
  });

  test('extracts numeric timestamp in ms', () => {
    const ts = 1750000000000;
    const event = { payload: { timestamp: ts } };
    expect(extractTimestampMs(event)).toBe(ts);
  });

  test('extracts numeric timestamp in seconds and converts to ms', () => {
    const tsSec = 1750000000;
    const event = { payload: { timestamp: tsSec } };
    expect(extractTimestampMs(event)).toBe(tsSec * 1000);
  });

  test('extracts timestamp directly from root event if not in payload', () => {
    const event = { timestamp: '2026-09-09T15:00:00Z' };
    expect(extractTimestampMs(event)).toBe(Date.parse('2026-09-09T15:00:00Z'));
  });

  test('extracts time / dateTime field fallbacks', () => {
    expect(extractTimestampMs({ payload: { time: '2026-09-09T15:00:00Z' } })).toBe(Date.parse('2026-09-09T15:00:00Z'));
    expect(extractTimestampMs({ payload: { dateTime: '2026-09-09T15:00:00Z' } })).toBe(Date.parse('2026-09-09T15:00:00Z'));
  });

  test('returns null when timestamp is missing or invalid', () => {
    expect(extractTimestampMs(null)).toBeNull();
    expect(extractTimestampMs({})).toBeNull();
    expect(extractTimestampMs({ payload: {} })).toBeNull();
    expect(extractTimestampMs({ payload: { timestamp: 'invalid-date' } })).toBeNull();
  });
});
