import React from 'react';
import { describe, test, expect, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import NotificationToast from './NotificationToast';
import NotificationContainer from './NotificationContainer';
import JsonCardRenderer from './JsonCardRenderer';
import ProcessingErrorCard from './ProcessingErrorCard';
import DecisionResultCard from './DecisionResultCard';

describe('Frontend Component Regression Suite', () => {
  describe('NotificationToast & NotificationContainer', () => {
    test('renders notification content correctly', () => {
      const notif = {
        id: 'n-1',
        label: 'Fraud Alert Triggered',
        timestamp: Date.now(),
        message: 'Account flagged for high risk activity',
      };
      const onDismiss = vi.fn();

      render(<NotificationToast notification={notif} onDismiss={onDismiss} autoDismissMs={0} />);

      expect(screen.getByText('Fraud Alert Triggered')).toBeInTheDocument();
      expect(screen.getByText('Account flagged for high risk activity')).toBeInTheDocument();
    });

    test('dismiss button invokes onDismiss callback', () => {
      vi.useFakeTimers();
      const notif = { id: 'n-2', label: 'Warning', message: 'Test message' };
      const onDismiss = vi.fn();

      render(<NotificationToast notification={notif} onDismiss={onDismiss} autoDismissMs={0} />);
      const closeBtn = screen.getByRole('button', { name: /dismiss notification/i });
      fireEvent.click(closeBtn);

      act(() => {
        vi.advanceTimersByTime(300);
      });

      expect(onDismiss).toHaveBeenCalledWith('n-2');
      vi.useRealTimers();
    });

    test('NotificationContainer renders nothing for empty list', () => {
      const { container } = render(<NotificationContainer notifications={[]} onDismiss={() => {}} />);
      expect(container.firstChild).toBeNull();
    });

    test('NotificationContainer renders multiple toasts', () => {
      const notifications = [
        { id: '1', label: 'One', message: 'Msg 1' },
        { id: '2', label: 'Two', message: 'Msg 2' },
      ];
      render(<NotificationContainer notifications={notifications} onDismiss={() => {}} autoDismissMs={0} />);

      expect(screen.getByText('One')).toBeInTheDocument();
      expect(screen.getByText('Two')).toBeInTheDocument();
    });
  });

  describe('JsonCardRenderer', () => {
    test('renders primitive key-value pairs', () => {
      const data = {
        accountId: 'acc-999',
        amount: 2500,
        isSuspicious: true,
      };

      render(<JsonCardRenderer data={data} />);

      expect(screen.getByText('Account Id')).toBeInTheDocument();
      expect(screen.getByText('acc-999')).toBeInTheDocument();
      expect(screen.getByText('Amount')).toBeInTheDocument();
      expect(screen.getByText('2500')).toBeInTheDocument();
      expect(screen.getByText('Is Suspicious')).toBeInTheDocument();
      expect(screen.getByText('Yes')).toBeInTheDocument();
    });

    test('renders nested objects and arrays', () => {
      const data = {
        user: { name: 'Alice', city: 'Paris' },
        tags: ['vip', 'verified'],
      };

      render(<JsonCardRenderer data={data} />);

      expect(screen.getByText('User')).toBeInTheDocument();
      expect(screen.getByText('Alice')).toBeInTheDocument();
      expect(screen.getByText('Paris')).toBeInTheDocument();
      expect(screen.getByText('Tags')).toBeInTheDocument();
      expect(screen.getByText('vip')).toBeInTheDocument();
      expect(screen.getByText('verified')).toBeInTheDocument();
    });
  });

  describe('ProcessingErrorCard', () => {
    test('renders error event correctly', () => {
      const errorObj = {
        source: 'banking-fraud-agent',
        entityId: 'acc-1234',
        origin: 'agent-transactions',
        reason: 'Connection to ruleset failed',
        timestamp: Date.now(),
      };

      render(<ProcessingErrorCard error={errorObj} />);

      expect(screen.getByText(/banking-fraud-agent/i)).toBeInTheDocument();
      expect(screen.getByText(/acc-1234/i)).toBeInTheDocument();
      expect(screen.getByText(/Connection to ruleset failed/i)).toBeInTheDocument();
    });
  });

  describe('DecisionResultCard', () => {
    test('renders decision result card', () => {
      const resultObj = {
        id: 'DEC-001',
        decision: 'SUSPICIOUS_TRANSACTION',
        rules: [
          { id: 'RULE-001', name: 'Velocity exceeded', severity: 'HIGH', message: 'Too many txns' },
        ],
      };

      render(<DecisionResultCard result={resultObj} />);

      expect(screen.getByText(/DEC-001/i)).toBeInTheDocument();
      expect(screen.getByText(/SUSPICIOUS_TRANSACTION/i)).toBeInTheDocument();
      expect(screen.getByText(/Velocity exceeded/i)).toBeInTheDocument();
    });
  });
});
