/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
/**
 * Scenario CRUD routes.
 *
 * Manages demo-scenarios.json files stored alongside agent descriptors:
 *
 *   GET    /api/use-cases/:useCase/scenarios
 *   PUT    /api/use-cases/:useCase/scenarios/:scenarioId
 *   POST   /api/use-cases/:useCase/scenarios
 *   DELETE /api/use-cases/:useCase/scenarios/:scenarioId
 *
 * Factory export: createScenariosRouter({ samplesDir, isValidAgentId })
 */

'use strict';

const express = require('express');
const fs      = require('fs').promises;
const path    = require('path');

/**
 * @param {object} opts
 * @param {string}   opts.samplesDir     - absolute path to the samples directory
 * @param {Function} opts.isValidAgentId - id validation predicate
 */
function createScenariosRouter({ samplesDir, isValidAgentId }) {
  const router = express.Router({ mergeParams: true });

  /** GET /api/use-cases/:useCase/scenarios — list all scenarios */
  router.get('/', async (req, res) => {
    const { useCase } = req.params;
    if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
    try {
      const scenariosPath = path.join(samplesDir, useCase, 'demo-scenarios.json');
      const content = await fs.readFile(scenariosPath, 'utf8');
      res.json(JSON.parse(content));
    } catch (error) {
      if (error.code === 'ENOENT') return res.json([]);
      console.error('Error reading scenarios:', error);
      res.status(500).json({ error: 'Failed to read scenarios' });
    }
  });

  /**
   * PUT /api/use-cases/:useCase/scenarios/:scenarioId
   * Saves (updates) an existing scenario.
   * Body: the full scenario object (must include an id matching :scenarioId).
   */
  router.put('/:scenarioId', async (req, res) => {
    const { useCase, scenarioId } = req.params;
    if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
    try {
      const scenariosPath = path.join(samplesDir, useCase, 'demo-scenarios.json');
      let scenarios = [];
      try {
        const content = await fs.readFile(scenariosPath, 'utf8');
        scenarios = JSON.parse(content);
      } catch (err) {
        if (err.code !== 'ENOENT') throw err;
      }
      const idx = scenarios.findIndex(s => s.id === scenarioId);
      if (idx === -1) return res.status(404).json({ error: 'Scenario not found' });
      scenarios[idx] = { ...scenarios[idx], ...req.body, id: scenarioId };
      await fs.writeFile(scenariosPath, JSON.stringify(scenarios, null, 2), 'utf8');
      res.json(scenarios[idx]);
    } catch (error) {
      console.error('Error saving scenario:', error);
      res.status(500).json({ error: 'Failed to save scenario' });
    }
  });

  /**
   * POST /api/use-cases/:useCase/scenarios
   * Creates a new scenario and appends it to demo-scenarios.json.
   * Body: { id, label, description, ... }
   * Responds 409 if a scenario with the same id already exists.
   */
  router.post('/', async (req, res) => {
    const { useCase } = req.params;
    if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
    try {
      const scenariosPath = path.join(samplesDir, useCase, 'demo-scenarios.json');
      let scenarios = [];
      try {
        const content = await fs.readFile(scenariosPath, 'utf8');
        scenarios = JSON.parse(content);
      } catch (err) {
        if (err.code !== 'ENOENT') throw err;
        // File doesn't exist yet — ensure the directory does
        await fs.mkdir(path.join(samplesDir, useCase), { recursive: true });
      }
      const newScenario = req.body;
      if (!newScenario.id) return res.status(400).json({ error: 'Scenario id is required' });
      if (scenarios.some(s => s.id === newScenario.id)) {
        return res.status(409).json({ error: `Scenario id "${newScenario.id}" already exists` });
      }
      scenarios.push(newScenario);
      await fs.writeFile(scenariosPath, JSON.stringify(scenarios, null, 2), 'utf8');
      res.status(201).json(newScenario);
    } catch (error) {
      console.error('Error creating scenario:', error);
      res.status(500).json({ error: 'Failed to create scenario' });
    }
  });

  /**
   * DELETE /api/use-cases/:useCase/scenarios/:scenarioId
   * Removes a scenario from demo-scenarios.json.
   * Returns 404 if the scenario does not exist.
   */
  router.delete('/:scenarioId', async (req, res) => {
    const { useCase } = req.params;
    if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
    try {
      const scenariosPath = path.join(samplesDir, useCase, 'demo-scenarios.json');
      let scenarios = [];
      try {
        const content = await fs.readFile(scenariosPath, 'utf8');
        scenarios = JSON.parse(content);
      } catch (err) {
        if (err.code === 'ENOENT') return res.status(404).json({ error: 'No scenarios file found' });
        throw err;
      }
      const idx = scenarios.findIndex(s => s.id === req.params.scenarioId);
      if (idx === -1) return res.status(404).json({ error: 'Scenario not found' });
      const [removed] = scenarios.splice(idx, 1);
      await fs.writeFile(scenariosPath, JSON.stringify(scenarios, null, 2), 'utf8');
      res.json({ deleted: removed.id });
    } catch (error) {
      console.error('Error deleting scenario:', error);
      res.status(500).json({ error: 'Failed to delete scenario' });
    }
  });

  return router;
}

module.exports = { createScenariosRouter };
