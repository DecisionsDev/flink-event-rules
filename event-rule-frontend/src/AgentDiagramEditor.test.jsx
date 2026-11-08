// @vitest-environment jsdom
import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import AgentDiagramEditor, { buildDiagramDescriptor } from './AgentDiagramEditor';

describe('AgentDiagramEditor', () => {
  describe('buildDiagramDescriptor', () => {
    test('builds empty descriptor when no inputs are provided', () => {
      const result = buildDiagramDescriptor({}, [], []);
      expect(result).toEqual({ agents: [] });
    });

    test('builds top-level metadata', () => {
      const meta = {
        name: 'Fraud Detection',
        flinkJobName: 'fraud-detection-job',
        description: 'Detects fraudulent transactions',
        consumerGroupId: 'fraud-group',
        folderName: 'banking-fraud',
      };
      const result = buildDiagramDescriptor(meta, [], []);
      expect(result).toEqual({
        name: 'Fraud Detection',
        flinkJobName: 'fraud-detection-job',
        description: 'Detects fraudulent transactions',
        consumerGroupId: 'fraud-group',
        agents: [],
      });
    });

    test('builds topics from topic nodes', () => {
      const nodes = [
        {
          id: 't-1',
          type: 'topic',
          data: {
            name: 'agent-transactions',
            type: 'Transaction',
            label: 'Transactions',
            schema: 'samples/schemas/tx.json',
            idleness: '30s',
          },
        },
      ];
      const result = buildDiagramDescriptor({}, nodes, []);
      expect(result.topics).toEqual([
        {
          name: 'agent-transactions',
          type: 'Transaction',
          label: 'Transactions',
          schema: 'samples/schemas/tx.json',
          idleness: '30s',
        },
      ]);
    });

    test('builds agents with entity relation, subscriptions, ruleset operations and outputs', () => {
      const nodes = [
        {
          id: 'topic-1',
          type: 'topic',
          data: { name: 'agent-tx', type: 'Transaction' },
        },
        {
          id: 'entity-1',
          type: 'entity',
          data: { entityType: 'Account', topic: 'account-changelog' },
        },
        {
          id: 'outbound-1',
          type: 'outbound',
          data: { topic: 'alerts-topic', eventType: 'Alert', label: 'Alerts', description: 'Fraud alerts' },
        },
        {
          id: 'agent-1',
          type: 'agent',
          data: {
            agentId: 'fraud-agent',
            label: 'Fraud Detection Agent',
            description: 'Agent for detecting fraud',
            ruleJar: 'samples/fraud/rules.jar',
            ruleOperations: ['EvaluateTransaction'],
            operationConfigs: {
              EvaluateTransaction: {
                entityField: 'theAccount',
                currentEventField: 'theTransaction',
                scheduleRequestsField: 'theScheduleRequests',
                outboundEntityField: 'theUpdatedAccount',
                history: [{ eventType: 'Transaction', field: 'thePastTransactions' }],
              },
            },
          },
        },
      ];

      const edges = [
        // Topic -> Agent subscription
        {
          id: 'edge-sub',
          source: 'topic-1',
          target: 'agent-1',
          targetHandle: 'op-in:EvaluateTransaction',
          data: {
            eventType: 'Transaction',
            operation: 'EvaluateTransaction',
            horizon: { type: 'hybrid', countLimit: 100, timeLimit: '30d', description: 'Keep last 100 or 30d' },
          },
        },
        // Entity -> Agent link
        {
          id: 'edge-ent',
          source: 'entity-1',
          target: 'agent-1',
          targetHandle: 'agent-entity-in',
        },
        // Agent -> Outbound operation output
        {
          id: 'edge-out',
          source: 'agent-1',
          sourceHandle: 'op-out:EvaluateTransaction',
          target: 'outbound-1',
          data: {
            field: 'theFraudAlerts',
            eventType: 'Alert',
          },
        },
      ];

      const result = buildDiagramDescriptor({ name: 'Fraud App' }, nodes, edges);
      expect(result.name).toBe('Fraud App');
      expect(result.topics).toEqual([{ name: 'agent-tx', type: 'Transaction' }]);
      expect(result.agents).toHaveLength(1);

      const agent = result.agents[0];
      expect(agent.agentId).toBe('fraud-agent');
      expect(agent.name).toBe('Fraud Detection Agent');
      expect(agent.description).toBe('Agent for detecting fraud');
      expect(agent.entityType).toBe('Account');
      expect(agent.stateConfig).toEqual({ changeLogTopic: 'account-changelog' });

      expect(agent.subscriptions).toEqual([
        {
          eventType: 'Transaction',
          topic: 'agent-tx',
          operation: 'EvaluateTransaction',
          horizon: {
            type: 'hybrid',
            countLimit: 100,
            timeLimit: '30d',
            description: 'Keep last 100 or 30d',
          },
        },
      ]);

      expect(agent.ruleset).toEqual({
        path: 'samples/fraud/rules.jar',
        operations: [
          {
            operation: 'EvaluateTransaction',
            entityField: 'theAccount',
            currentEventField: 'theTransaction',
            scheduleRequestsField: 'theScheduleRequests',
            outboundEntityField: 'theUpdatedAccount',
            history: [{ eventType: 'Transaction', field: 'thePastTransactions' }],
            outbounds: [{ field: 'theFraudAlerts', eventType: 'Alert' }],
          },
        ],
      });

      expect(agent.outputs).toEqual([
        {
          eventType: 'Alert',
          topic: 'alerts-topic',
          label: 'Alerts',
          description: 'Fraud alerts',
        },
      ]);
    });
  });

  describe('Save button UI in AgentDiagramEditor', () => {
    let originalFetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
      vi.restoreAllMocks();
    });

    describe('empty canvas', () => {
      test('suggests dropping an Agent and shows the Decision Service project drop zone', () => {
        render(<AgentDiagramEditor onClose={vi.fn()} />);
        expect(screen.getByText('This diagram is empty')).toBeInTheDocument();
        expect(screen.getByText(/from the palette onto the canvas/i)).toBeInTheDocument();
        expect(screen.getByText(/Drop a Decision Service project \(\.zip\) here/i)).toBeInTheDocument();
      });

      test('is not shown once the diagram has nodes', () => {
        render(
          <AgentDiagramEditor
            onClose={vi.fn()}
            initialFolderName="orders"
            initialDescriptor={{ agents: [{ id: 'a', name: 'A', subscriptions: [], outputs: [] }] }}
          />,
        );
        expect(screen.queryByText('This diagram is empty')).not.toBeInTheDocument();
      });

      test('dropping a project zip creates an Agent linked to it', async () => {
        const JSZip = (await import('jszip')).default;
        const zip = new JSZip();
        zip.file('readme.txt', 'no operations in here');
        const blob = await zip.generateAsync({ type: 'uint8array' });
        const file = new File([blob], 'FraudService.zip', { type: 'application/zip' });

        render(<AgentDiagramEditor onClose={vi.fn()} />);
        fireEvent.drop(screen.getByRole('button', { name: /Drop a Decision Service project/i }), {
          dataTransfer: { files: [file], items: [{ kind: 'file' }], getData: () => '' },
        });

        await waitFor(() => expect(screen.queryByText('This diagram is empty')).not.toBeInTheDocument());
        expect(screen.getByDisplayValue('di-project/FraudService.zip')).toBeInTheDocument();
        expect(screen.getAllByText('FraudService', { selector: '.ade-node-name' }).length).toBeGreaterThan(0);
      });

      test('a dropped file that is not a zip is ignored', () => {
        render(<AgentDiagramEditor onClose={vi.fn()} />);
        const file = new File(['x'], 'notes.txt', { type: 'text/plain' });
        fireEvent.drop(screen.getByRole('button', { name: /Drop a Decision Service project/i }), {
          dataTransfer: { files: [file], items: [{ kind: 'file' }], getData: () => '' },
        });
        expect(screen.getByText('This diagram is empty')).toBeInTheDocument();
      });
    });

    test('a new application defaults to the name "My Application" and folder "my-application"', () => {
      render(<AgentDiagramEditor onClose={vi.fn()} />);
      expect(screen.getByDisplayValue('My Application')).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/e\.g\. banking-fraud-agent/i)).toHaveValue('my-application');
      expect(screen.getByRole('button', { name: /save/i })).toBeInTheDocument();
    });

    test('Save button is hidden when the folder name is cleared', () => {
      render(<AgentDiagramEditor onClose={vi.fn()} />);
      fireEvent.change(screen.getByPlaceholderText(/e\.g\. banking-fraud-agent/i), { target: { value: '' } });
      expect(screen.queryByRole('button', { name: /save/i })).not.toBeInTheDocument();
    });

    test('Save button is visible when initialFolderName is provided', () => {
      render(<AgentDiagramEditor onClose={vi.fn()} initialFolderName="banking-fraud-agent" />);
      expect(screen.getByRole('button', { name: /save/i })).toBeInTheDocument();
    });

    test('Save button appears when user enters folder name in properties panel', () => {
      render(<AgentDiagramEditor onClose={vi.fn()} />);
      const folderInput = screen.getByPlaceholderText(/e\.g\. banking-fraud-agent/i);
      fireEvent.change(folderInput, { target: { value: '' } });
      expect(screen.queryByRole('button', { name: /save/i })).not.toBeInTheDocument();

      fireEvent.change(folderInput, { target: { value: 'my-custom-agent' } });

      expect(screen.getByRole('button', { name: /save/i })).toBeInTheDocument();
    });

    test('Clicking Save button triggers POST /api/agents with descriptor', async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ saved: true, useCaseId: 'my-custom-agent', path: 'samples/my-custom-agent/agents.json' }),
      });
      global.fetch = fetchMock;

      render(<AgentDiagramEditor onClose={vi.fn()} initialFolderName="my-custom-agent" />);

      const saveBtn = screen.getByRole('button', { name: /save/i });
      fireEvent.click(saveBtn);

      await waitFor(() => {
        expect(fetchMock).toHaveBeenCalledTimes(1);
      });

      const [url, options] = fetchMock.mock.calls[0];
      expect(url).toContain('/api/agents');
      expect(options.method).toBe('POST');
      expect(JSON.parse(options.body)).toEqual({
        useCaseId: 'my-custom-agent',
        descriptor: { agents: [] },
        mode: 'replace',
      });

      expect(await screen.findByText(/✓ Saved/i)).toBeInTheDocument();
    });

    test('Displays error message when backend save fails', async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: false,
        status: 409,
        json: async () => ({ error: 'Agent is currently running. Stop the Flink job first.' }),
      });
      global.fetch = fetchMock;

      render(<AgentDiagramEditor onClose={vi.fn()} initialFolderName="running-agent" />);

      const saveBtn = screen.getByRole('button', { name: /save/i });
      fireEvent.click(saveBtn);

      expect(await screen.findByText(/Agent is currently running/i)).toBeInTheDocument();
    });
  });

  describe('Topic editor in detail panel', () => {
    test('renders topic fields in the correct order with descriptions', () => {
      render(<AgentDiagramEditor onClose={vi.fn()} />);

      // Drag and drop a topic node onto the canvas area
      const paletteTopic = screen.getByText(/^Topic$/i, { selector: '.ade-palette-item-label' }).closest('.ade-palette-item');
      const canvas = document.querySelector('.ade-canvas-area');

      const dataTransfer = {
        data: {},
        setData(key, val) { this.data[key] = val; },
        getData(key) { return this.data[key] || ''; },
      };

      fireEvent.dragStart(paletteTopic, { dataTransfer });
      fireEvent.drop(canvas, { clientX: 100, clientY: 100, dataTransfer });

      // Verify the topic editor fields are rendered
      expect(screen.getByText('Name')).toBeInTheDocument();
      expect(screen.getByText('Human-readable display name for this topic.')).toBeInTheDocument();

      expect(screen.getByText('Kafka Topic Name')).toBeInTheDocument();
      expect(screen.getByText('Kafka topic name where events are published and consumed.')).toBeInTheDocument();

      expect(screen.getByText('Event Type')).toBeInTheDocument();
      expect(screen.getByText('Java or business class name of events carried on this topic.')).toBeInTheDocument();

      expect(screen.getByText('Event Schema (optional)')).toBeInTheDocument();
      expect(screen.getByText('JSON Schema file defining the structure and validation rules for event payloads.')).toBeInTheDocument();

      expect(screen.getByText('Topic Idleness (optional)')).toBeInTheDocument();
      expect(screen.getByText('Flink watermark idleness timeout for advancing event time when a partition is idle.')).toBeInTheDocument();
    });
  });

  describe('Link editors event type read-only behavior', () => {
    test('buildDiagramDescriptor derives event type from Topic for subscription and from Outbound for outbound', () => {
      const nodes = [
        { id: 'top-1', type: 'topic', data: { name: 'my-topic', type: 'OrderEvent' } },
        { id: 'out-1', type: 'outbound', data: { topic: 'my-out-topic', eventType: 'OrderNotification' } },
        {
          id: 'agent-1',
          type: 'agent',
          data: {
            agentId: 'my-agent',
            ruleOperations: ['ProcessOrder'],
            operationConfigs: { ProcessOrder: {} },
          },
        },
      ];
      const edges = [
        {
          id: 'e-sub',
          source: 'top-1',
          target: 'agent-1',
          targetHandle: 'op-in:ProcessOrder',
          data: { operation: 'ProcessOrder' },
        },
        {
          id: 'e-out',
          source: 'agent-1',
          sourceHandle: 'op-out:ProcessOrder',
          target: 'out-1',
          data: { field: 'theNotifications' },
        },
      ];

      const descriptor = buildDiagramDescriptor({}, nodes, edges);
      expect(descriptor.agents[0].subscriptions[0].eventType).toBe('OrderEvent');
      expect(descriptor.agents[0].ruleset.operations[0].outbounds[0].eventType).toBe('OrderNotification');
      expect(descriptor.agents[0].outputs[0].eventType).toBe('OrderNotification');
    });
  });

  describe('editing an existing descriptor', () => {
    const descriptor = {
      name: 'Orders',
      flinkJobName: 'orders-job',
      consumerGroupId: 'orders-group',
      parallelism: 2,
      icon: 'cart',
      topics: [{ name: 'orders-in', type: 'Order', label: 'Orders' }],
      agents: [{
        agentId: 'order-agent',
        name: 'Order Agent',
        entityType: 'Customer',
        entityKeyExtractor: '$.customerId',
        chainingEnabled: false,
        references: [{ name: 'customers', type: 'csv', file: 'samples/orders/customers.csv' }],
        subscriptions: [
          {
            eventType: 'Order', topic: 'orders-in', operation: 'ProcessOrder',
            horizon: { type: 'count', value: 5 },
            enrichment: [{ reference: 'customers', key: 'id', on: '$.customerId', into: '$.customer', fields: ['name'] }],
          },
          { eventType: 'Schedule', operation: 'ProcessSchedule' },
        ],
        ruleset: {
          path: 'samples/orders/rules.jar',
          operations: [
            { operation: 'ProcessOrder', entityField: 'theCustomer', currentEventField: 'theOrder',
              outbounds: [{ field: 'theAlerts', eventType: 'Alert' }] },
            { operation: 'ProcessSchedule', entityField: 'theCustomer', currentEventField: 'theSchedule' },
          ],
        },
        outputs: [
          { eventType: 'Alert', topic: 'alerts', label: 'Alerts' },
          { eventType: 'agent-error', topic: 'errors' },
        ],
        stateConfig: { ttl: '7d', backend: 'rocksdb', changeLogTopic: 'customer-changelog' },
      }],
    };

    let originalFetch;
    beforeEach(() => { originalFetch = global.fetch; });
    afterEach(() => { global.fetch = originalFetch; vi.restoreAllMocks(); });

    const openEditor = () => render(
      <AgentDiagramEditor onClose={vi.fn()} initialFolderName="orders" initialDescriptor={descriptor} />,
    );

    const selectNode = (text) => {
      const node = screen.getByText(text, { selector: '.ade-node-name' }).closest('.ade-node');
      fireEvent.click(node);
    };

    test('shows every node of the descriptor and the application properties', () => {
      openEditor();
      expect(screen.getByDisplayValue('Orders')).toBeInTheDocument();
      expect(screen.getByDisplayValue('orders-job')).toBeInTheDocument();
      expect(screen.getByLabelText('Job parallelism')).toHaveValue(2);
      for (const name of ['Order Agent', 'Orders', 'Customer', 'Alerts']) {
        expect(screen.getAllByText(name, { selector: '.ade-node-name' }).length).toBeGreaterThan(0);
      }
    });

    test('saving without any change posts the same descriptor', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
        useCaseId: 'orders', descriptor, mode: 'replace',
      });
    });

    test('an edited agent field is saved', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');
      fireEvent.change(screen.getByPlaceholderText('e.g. $.accountId'), { target: { value: '$.id' } });
      fireEvent.change(screen.getByLabelText('Agent parallelism'), { target: { value: '3' } });
      fireEvent.change(screen.getByPlaceholderText('e.g. 30d'), { target: { value: '14d' } });
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      expect(agent.entityKeyExtractor).toBe('$.id');
      expect(agent.parallelism).toBe(3);
      expect(agent.chainingEnabled).toBe(false);
      expect(agent.stateConfig).toEqual({ ttl: '14d', backend: 'rocksdb', changeLogTopic: 'customer-changelog' });
    });

    test('the Decision Service project path is shown and can be edited', async () => {
      const fetchMock = vi.fn(async (url) => (String(url).includes('/api/project-file')
        ? { ok: false, status: 404 }
        : { ok: true, json: async () => ({}) }));
      global.fetch = fetchMock;
      render(
        <AgentDiagramEditor
          onClose={vi.fn()}
          initialFolderName="orders"
          initialDescriptor={{
            ...descriptor,
            agents: [{ ...descriptor.agents[0], ruleset: { ...descriptor.agents[0].ruleset, project: 'samples/orders/di-project/Orders.zip' } }],
          }}
        />,
      );
      selectNode('Order Agent');
      const input = screen.getByDisplayValue('samples/orders/di-project/Orders.zip');
      fireEvent.change(input, { target: { value: 'samples/orders/di-project/Other.zip' } });
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
      const agent = JSON.parse(fetchMock.mock.calls[1][1].body).descriptor.agents[0];
      expect(agent.ruleset.project).toBe('samples/orders/di-project/Other.zip');
    });

    test('a registered Decision Service project is loaded when the editor opens', async () => {
      const zipPath = path.resolve(__dirname, '../../samples/banking-fraud-agent/di-project/FraudDetectionService.zip');
      const buf = fs.readFileSync(zipPath);
      const arrayBuffer = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
      const fetchMock = vi.fn(async () => ({ ok: true, arrayBuffer: async () => arrayBuffer }));
      global.fetch = fetchMock;
      render(
        <AgentDiagramEditor
          onClose={vi.fn()}
          initialFolderName="orders"
          initialDescriptor={{
            ...descriptor,
            agents: [{ ...descriptor.agents[0], ruleset: { ...descriptor.agents[0].ruleset, project: 'samples/orders/di-project/FraudDetectionService.zip' } }],
          }}
        />,
      );
      expect(fetchMock.mock.calls[0][0]).toMatch(/\/api\/project-file\?path=samples%2Forders%2Fdi-project%2FFraudDetectionService\.zip$/);
      // selecting the linked topic shows its Event Type selector, fed by the project's classes
      fireEvent.click(screen.getAllByText('Orders', { selector: '.ade-node-name' })[0].closest('.ade-node'));
      await waitFor(() => expect(document.querySelector('option[value="AccountEvent"]')).not.toBeNull());
    });

    test('a selected Decision Service project zip is uploaded to di-project on save', async () => {
      const fetchMock = vi.fn(async (url) => ({
        ok: true,
        json: async () => (String(url).endsWith('/project')
          ? { saved: true, path: 'samples/orders/di-project/Orders Service.zip' }
          : {}),
      }));
      global.fetch = fetchMock;
      const { container } = openEditor();
      selectNode('Order Agent');
      const zipInput = container.querySelectorAll('input[type="file"]')[1];
      const file = new File(['not a real zip'], 'Orders Service.zip', { type: 'application/zip' });
      fireEvent.change(zipInput, { target: { files: [file] } });
      expect(await screen.findByDisplayValue('di-project/Orders Service.zip')).toBeInTheDocument();

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
      const [uploadUrl, uploadOptions] = fetchMock.mock.calls[0];
      expect(uploadUrl).toMatch(/\/api\/agents\/orders\/project$/);
      expect(uploadOptions.body.get('project').name).toBe('Orders Service.zip');
      const agent = JSON.parse(fetchMock.mock.calls[1][1].body).descriptor.agents[0];
      expect(agent.ruleset.project).toBe('samples/orders/di-project/Orders Service.zip');
    });

    test('references can be added, edited and removed', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');
      fireEvent.click(screen.getByRole('button', { name: '+ Add reference' }));
      fireEvent.change(screen.getByLabelText('Reference 2 name'), { target: { value: 'products' } });
      fireEvent.change(screen.getByLabelText('Reference 2 file'), { target: { value: 'p.csv' } });
      fireEvent.click(screen.getByLabelText('Remove reference 1'));
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      expect(agent.references).toEqual([{ name: 'products', type: 'csv', file: 'p.csv' }]);
    });

    test('a CSV picked from disk for a reference is uploaded to enrichment on save', async () => {
      const fetchMock = vi.fn(async (url) => ({
        ok: true,
        json: async () => (String(url).endsWith('/reference')
          ? { saved: true, path: 'samples/orders/enrichment/products.csv' }
          : {}),
      }));
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');
      fireEvent.click(screen.getByRole('button', { name: '+ Add reference' }));
      const file = new File(['id,name\n1,a'], 'products.csv', { type: 'text/csv' });
      fireEvent.change(screen.getByLabelText('Reference 2 file picker'), { target: { files: [file] } });
      // the path and the (empty) name are filled in from the picked file
      expect(screen.getByLabelText('Reference 2 file')).toHaveValue('enrichment/products.csv');
      expect(screen.getByLabelText('Reference 2 name')).toHaveValue('products');

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
      const [uploadUrl, uploadOptions] = fetchMock.mock.calls[0];
      expect(uploadUrl).toMatch(/\/api\/agents\/orders\/reference$/);
      expect(uploadOptions.body.get('reference').name).toBe('products.csv');
      const agent = JSON.parse(fetchMock.mock.calls[1][1].body).descriptor.agents[0];
      expect(agent.references).toEqual([
        { name: 'customers', type: 'csv', file: 'samples/orders/customers.csv' },
        { name: 'products', type: 'csv', file: 'samples/orders/enrichment/products.csv' },
      ]);
    });

    test('operations can be added and show up in the descriptor', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');
      const input = screen.getByLabelText('New operation name');
      fireEvent.change(input, { target: { value: 'ProcessRefund' } });
      fireEvent.keyDown(input, { key: 'Enter' });
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const ops = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0].ruleset.operations;
      expect(ops.map(o => o.operation)).toEqual(['ProcessOrder', 'ProcessSchedule', 'ProcessRefund']);
    });

    test('the enrichment reference is picked from the references registered in the agent', () => {
      openEditor();
      // register a second reference on the agent
      selectNode('Order Agent');
      fireEvent.click(screen.getByRole('button', { name: '+ Add reference' }));
      fireEvent.change(screen.getByLabelText('Reference 2 name'), { target: { value: 'products' } });
      // an outbound's enrichment offers both registered references
      selectNode('Alerts');
      fireEvent.click(screen.getByRole('button', { name: '+ Add enrichment' }));
      const select = screen.getByLabelText('Enrichment 1 reference');
      expect(select.tagName).toBe('SELECT');
      expect([...select.options].map(o => o.value)).toEqual(['', 'customers', 'products']);
      expect(select).toHaveValue('');
      fireEvent.change(select, { target: { value: 'products' } });
      expect(select).toHaveValue('products');
    });

    test('renaming an operation keeps its links', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      const sub = document.querySelector('[data-op="ProcessOrder"]');
      fireEvent.click(sub);
      const input = screen.getByLabelText('Operation name');
      fireEvent.change(input, { target: { value: 'HandleOrder' } });
      fireEvent.keyDown(input, { key: 'Enter' });
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      expect(agent.subscriptions[0].operation).toBe('HandleOrder');
      const op = agent.ruleset.operations.find(o => o.operation === 'HandleOrder');
      expect(op.outbounds).toEqual([{ field: 'theAlerts', eventType: 'Alert' }]);
      expect(op.entityField).toBe('theCustomer');
    });

    test('an operation can be deleted along with its links', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      fireEvent.click(document.querySelector('[data-op="ProcessOrder"]'));
      fireEvent.click(screen.getByRole('button', { name: 'Delete operation' }));
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      expect(agent.ruleset.operations.map(o => o.operation)).toEqual(['ProcessSchedule']);
      expect(agent.subscriptions.map(s => s.eventType)).toEqual(['Schedule']);
    });

    test('an operation can be removed from the agent detail editor', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');
      fireEvent.click(screen.getByLabelText('Remove operation ProcessOrder'));
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      expect(agent.ruleset.operations.map(o => o.operation)).toEqual(['ProcessSchedule']);
      expect(agent.subscriptions.map(s => s.eventType)).toEqual(['Schedule']);
    });

    test('the topic-less Schedule subscription is editable on the agent', () => {
      openEditor();
      selectNode('Order Agent');
      expect(screen.getByText('Schedules')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Schedule')).toBeInTheDocument();
    });

    test('the agent node has no "other outputs" section', () => {
      openEditor();
      expect(screen.queryByText('other outputs')).not.toBeInTheDocument();
    });

    test('the agent detail editor has an Errors section with fixed event type agent-error and editable fields', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
      global.fetch = fetchMock;
      openEditor();
      selectNode('Order Agent');

      expect(screen.getByText('Errors')).toBeInTheDocument();
      expect(screen.getByDisplayValue('agent-error')).toHaveAttribute('readonly');
      expect(screen.getByDisplayValue('errors')).toBeInTheDocument();

      fireEvent.change(screen.getByPlaceholderText('e.g. agent-banking-errors'), { target: { value: 'custom-errors' } });
      fireEvent.change(screen.getByPlaceholderText('e.g. Processing errors for investigation'), { target: { value: 'Custom Error Label' } });
      fireEvent.change(screen.getByPlaceholderText('e.g. Details of errors encountered during execution'), { target: { value: 'Custom Error Desc' } });

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const agent = JSON.parse(fetchMock.mock.calls[0][1].body).descriptor.agents[0];
      const errorOutput = agent.outputs.find(o => o.eventType === 'agent-error');
      expect(errorOutput).toEqual({
        eventType: 'agent-error',
        topic: 'custom-errors',
        label: 'Custom Error Label',
        description: 'Custom Error Desc',
      });
    });

    test('the Outbound panel has no dashboard filter section', () => {
      openEditor();
      selectNode('Alerts');
      expect(screen.queryByText(/dashboard filter/i)).not.toBeInTheDocument();
      expect(screen.queryByPlaceholderText('e.g. severity')).not.toBeInTheDocument();
    });

    test('lists the problems the backend would reject', () => {
      openEditor();
      selectNode('Order Agent');
      fireEvent.change(screen.getByPlaceholderText('e.g. order-agent'), { target: { value: '' } });
      fireEvent.click(screen.getByRole('button', { name: /problem/i }));
      expect(screen.getByLabelText('Descriptor problems')).toHaveTextContent(/Agent ID is required/);
    });

    test('Export JSON downloads the descriptor', () => {
      const createObjectURL = vi.fn(() => 'blob:x');
      global.URL.createObjectURL = createObjectURL;
      global.URL.revokeObjectURL = vi.fn();
      vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
      openEditor();
      fireEvent.click(screen.getByRole('button', { name: /export json/i }));
      expect(createObjectURL).toHaveBeenCalledTimes(1);
    });
  });

  describe('Undo / Redo mechanism in AgentDiagramEditor', () => {
    const testDescriptor = {
      name: 'Orders',
      flinkJobName: 'orders-job',
      consumerGroupId: 'orders-group',
      parallelism: 2,
      topics: [{ name: 'orders-in', type: 'Order', label: 'Orders' }],
      agents: [{
        agentId: 'order-agent',
        name: 'Order Agent',
        description: 'Processes customer orders',
        entityType: 'Customer',
        subscriptions: [
          { eventType: 'Order', topic: 'orders-in', operation: 'ProcessOrder' },
        ],
        ruleset: {
          path: 'samples/orders/rules.jar',
          operations: [
            { operation: 'ProcessOrder', entityField: 'theCustomer', currentEventField: 'theOrder' },
            { operation: 'ProcessSchedule', entityField: 'theCustomer', currentEventField: 'theSchedule' },
          ],
        },
        outputs: [{ eventType: 'Alert', topic: 'alerts', label: 'Alerts' }],
      }],
    };

    let originalFetch;
    beforeEach(() => { originalFetch = global.fetch; });
    afterEach(() => { global.fetch = originalFetch; vi.restoreAllMocks(); });

    const openEditor = (initialDesc = null) => render(
      <AgentDiagramEditor
        onClose={vi.fn()}
        initialFolderName="orders"
        initialDescriptor={initialDesc}
      />,
    );

    test('Undo and Redo buttons are disabled initially when opening an empty editor', () => {
      openEditor();
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });
      expect(undoBtn).toBeDisabled();
      expect(redoBtn).toBeDisabled();
    });

    test('Undo and Redo buttons are disabled initially when opening an existing diagram', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });
      expect(undoBtn).toBeDisabled();
      expect(redoBtn).toBeDisabled();
    });

    test('Editing an Application property enables Undo, and Undo/Redo toggles the value', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });

      const nameInput = screen.getByDisplayValue('Orders');
      fireEvent.change(nameInput, { target: { value: 'Modified Orders App' } });

      expect(undoBtn).not.toBeDisabled();
      expect(redoBtn).toBeDisabled();
      expect(screen.getByDisplayValue('Modified Orders App')).toBeInTheDocument();

      // Undo
      fireEvent.click(undoBtn);
      expect(screen.getByDisplayValue('Orders')).toBeInTheDocument();
      expect(undoBtn).toBeDisabled();
      expect(redoBtn).not.toBeDisabled();

      // Redo
      fireEvent.click(redoBtn);
      expect(screen.getByDisplayValue('Modified Orders App')).toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();
      expect(redoBtn).toBeDisabled();
    });

    test('Editing a node property and undoing reverts the node property', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });

      // Select 'Order Agent'
      const node = screen.getByText('Order Agent', { selector: '.ade-node-name' }).closest('.ade-node');
      fireEvent.click(node);

      const agentDescInput = screen.getByDisplayValue('Processes customer orders');
      fireEvent.change(agentDescInput, { target: { value: 'New description for orders' } });

      expect(undoBtn).not.toBeDisabled();

      fireEvent.click(undoBtn);
      expect(screen.getByDisplayValue('Processes customer orders')).toBeInTheDocument();

      fireEvent.click(redoBtn);
      expect(screen.getByDisplayValue('New description for orders')).toBeInTheDocument();
    });

    test('Clear diagram opens Carbon confirmation modal and can be confirmed, undone, and redone', () => {
      openEditor(testDescriptor);

      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });

      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      // Click toolbar Clear button
      fireEvent.click(screen.getByRole('button', { name: /^clear$/i }));

      // Carbon confirmation modal appears
      expect(screen.getByRole('dialog')).toBeInTheDocument();
      expect(screen.getByRole('heading', { name: 'Clear diagram' })).toBeInTheDocument();
      expect(screen.getByText(/Are you sure you want to clear all nodes and edges from the diagram\?/i)).toBeInTheDocument();

      // Confirm Clear in modal
      fireEvent.click(screen.getByRole('button', { name: /^clear diagram$/i }));

      // Diagram is cleared, empty hint is displayed, modal is dismissed
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(screen.getByText('This diagram is empty')).toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();

      // Undo Clear
      fireEvent.click(undoBtn);
      expect(screen.queryByText('This diagram is empty')).not.toBeInTheDocument();
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      // Redo Clear
      fireEvent.click(redoBtn);
      expect(screen.getByText('This diagram is empty')).toBeInTheDocument();
    });

    test('Clear diagram confirmation modal can be cancelled or dismissed with Escape', () => {
      openEditor(testDescriptor);

      // 1. Cancel via Cancel button
      fireEvent.click(screen.getByRole('button', { name: /^clear$/i }));
      expect(screen.getByRole('dialog')).toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: /cancel/i }));
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      // 2. Dismiss via Close button
      fireEvent.click(screen.getByRole('button', { name: /^clear$/i }));
      const dialog = screen.getByRole('dialog');
      expect(dialog).toBeInTheDocument();
      fireEvent.click(within(dialog).getByRole('button', { name: /^close$/i }));
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      // 3. Dismiss via Escape key
      fireEvent.click(screen.getByRole('button', { name: /^clear$/i }));
      expect(screen.getByRole('dialog')).toBeInTheDocument();
      fireEvent.keyDown(window, { key: 'Escape' });
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();
    });

    test('Keyboard shortcuts Ctrl+Z and Ctrl+Shift+Z / Ctrl+Y trigger Undo and Redo', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });

      const nameInput = screen.getByDisplayValue('Orders');
      fireEvent.change(nameInput, { target: { value: 'Keyboard Test App' } });
      expect(undoBtn).not.toBeDisabled();

      // Blur input so focus is on document
      fireEvent.blur(nameInput);

      // Trigger Ctrl+Z (Undo) on window
      fireEvent.keyDown(window, { key: 'z', ctrlKey: true });
      expect(screen.getByDisplayValue('Orders')).toBeInTheDocument();

      // Trigger Ctrl+Shift+Z (Redo)
      fireEvent.keyDown(window, { key: 'z', ctrlKey: true, shiftKey: true });
      expect(screen.getByDisplayValue('Keyboard Test App')).toBeInTheDocument();

      // Trigger Cmd+Z (Undo) on Mac
      fireEvent.keyDown(window, { key: 'z', metaKey: true });
      expect(screen.getByDisplayValue('Orders')).toBeInTheDocument();

      // Trigger Ctrl+Y (Redo)
      fireEvent.keyDown(window, { key: 'y', ctrlKey: true });
      expect(screen.getByDisplayValue('Keyboard Test App')).toBeInTheDocument();
    });

    test('Keyboard shortcuts do not intercept Ctrl+Z when typing inside input elements', () => {
      openEditor(testDescriptor);
      const nameInput = screen.getByDisplayValue('Orders');
      fireEvent.change(nameInput, { target: { value: 'In Input Test' } });

      // Press Ctrl+Z with input as target
      fireEvent.keyDown(nameInput, { key: 'z', ctrlKey: true });

      // Diagram undo should not be triggered, value remains
      expect(screen.getByDisplayValue('In Input Test')).toBeInTheDocument();
    });

    test('Removing an operation and undoing restores the operation', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });

      // Select 'Order Agent'
      const node = screen.getByText('Order Agent', { selector: '.ade-node-name' }).closest('.ade-node');
      fireEvent.click(node);

      expect(screen.getByRole('button', { name: 'Remove operation ProcessSchedule' })).toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: 'Remove operation ProcessSchedule' }));

      expect(screen.queryByRole('button', { name: 'Remove operation ProcessSchedule' })).not.toBeInTheDocument();

      // Undo removal
      fireEvent.click(undoBtn);
      expect(screen.getByRole('button', { name: 'Remove operation ProcessSchedule' })).toBeInTheDocument();
    });

    test('deleting a node with Backspace removes incoming and outgoing links in exactly 1 undo step', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });

      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      const agentNode = screen.getByText('Order Agent', { selector: '.ade-node-name' }).closest('.ade-node');
      fireEvent.click(agentNode);

      // Trigger Backspace deletion
      fireEvent.keyDown(window, { key: 'Backspace' });

      // Node is deleted
      expect(screen.queryByText('Order Agent', { selector: '.ade-node-name' })).not.toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();

      // Undo deletion in ONE single click
      fireEvent.click(undoBtn);

      // Node is restored
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();
      // Exactly one undo step was created (undo is disabled after 1 click)
      expect(undoBtn).toBeDisabled();
      expect(redoBtn).not.toBeDisabled();

      // Redo deletion in ONE single click
      fireEvent.click(redoBtn);
      expect(screen.queryByText('Order Agent', { selector: '.ade-node-name' })).not.toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();
      expect(redoBtn).toBeDisabled();
    });

    test('deleting a node using the delete button in the detail editor generates exactly 1 undo step', () => {
      openEditor(testDescriptor);
      const undoBtn = screen.getByRole('button', { name: /undo/i });
      const redoBtn = screen.getByRole('button', { name: /redo/i });

      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();

      const agentNode = screen.getByText('Order Agent', { selector: '.ade-node-name' }).closest('.ade-node');
      fireEvent.click(agentNode);

      // Click detail panel delete button
      const deleteBtn = screen.getByRole('button', { name: 'Delete agent node' });
      fireEvent.click(deleteBtn);

      // Node is deleted
      expect(screen.queryByText('Order Agent', { selector: '.ade-node-name' })).not.toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();

      // Undo deletion in ONE single click
      fireEvent.click(undoBtn);

      // Node is restored
      expect(screen.getByText('Order Agent', { selector: '.ade-node-name' })).toBeInTheDocument();
      // Exactly one undo step was created
      expect(undoBtn).toBeDisabled();
      expect(redoBtn).not.toBeDisabled();

      // Redo deletion in ONE single click
      fireEvent.click(redoBtn);
      expect(screen.queryByText('Order Agent', { selector: '.ade-node-name' })).not.toBeInTheDocument();
      expect(undoBtn).not.toBeDisabled();
      expect(redoBtn).toBeDisabled();
    });
  });
});
