// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { exploreDecisionServiceProject, parseVoc, parseVocAttributes, parseDop, parseDmoVariables, parseDmoOutputType, parseVarType, toVariableName, buildVocabOptions } from './decisionServiceProject';

describe('decisionServiceProject', () => {
  it('parseVoc extracts class name (before #, after last dot) and label', () => {
    const txt = 'a.b.c.AccountEvent#concept.label = Account Event\nfoo#bar = x\n';
    expect(parseVoc(txt)).toEqual([{ className: 'AccountEvent', label: 'Account Event' }]);
  });

  it('parseVocAttributes extracts attributeName/attributeLabel keyed by class', () => {
    const txt = [
      'a.b.AccountEvent.accountType#phrase.navigation = {account type} of {this}',
      'a.b.AccountEvent.accountType#phrase.action = set the account type of {this} to {account type}',
      'a.b.AccountEvent.AccountEvent(java.lang.String)#phrase.navigation = a new {Account Event} where {0, x}',
      'a.b.Other.ownerCountry#phrase.navigation = {owner country} of {this}',
    ].join('\n');
    expect(parseVocAttributes(txt)).toEqual({
      AccountEvent: [{ attributeName: 'accountType', attributeLabel: 'account type' }],
      Other: [{ attributeName: 'ownerCountry', attributeLabel: 'owner country' }],
    });
  });

  it('parseDop extracts the operationName tag value, without quotes', () => {
    const xml = '<a:Operation xmlns:a="urn:x"><name>Other</name><tags key="operationName"><value><![CDATA["Op1"]]></value></tags></a:Operation>';
    expect(parseDop(xml)).toBe('Op1');
  });

  it('toVariableName camel-cases a verbalization', () => {
    expect(toVariableName('the past transactions')).toBe('thePastTransactions');
    expect(toVariableName('now')).toBe('now');
  });

  it('parseDmoVariables reads <initialization>/<output name> of input data only', () => {
    const xml = `<m xmlns="urn:dmn"><inputData name="A"><extensionElements><initialization xmlns="urn:x"><output name="the past transactions"/></initialization></extensionElements></inputData>
      <decision><extensionElements><ruleset xmlns="urn:x"><output name="the fraud assessment"/></ruleset></extensionElements></decision></m>`;
    expect(parseDmoVariables(xml)).toEqual(['thePastTransactions']);
  });

  it('parseDmoOutputType returns the ruleset output type of the top-level decision', () => {
    const xml = `<defs xmlns="urn:dmn">
      <decision id="top"><extensionElements><ruleset xmlns="urn:x"><input bomType="p.In"/><output bomType="p.q.Top"/></ruleset></extensionElements>
        <informationRequirement><requiredDecision href="#sub"/></informationRequirement></decision>
      <decision id="sub"><extensionElements><ruleset xmlns="urn:x"><output bomType="p.q.Sub"/></ruleset></extensionElements></decision></defs>`;
    expect(parseDmoOutputType(xml)).toBe('Top');
  });

  it('parseVarType returns the class name of a variable type', () => {
    const xml = '<v:VariableSet xmlns:v="urn:v"><variables name="result" type="a.b.Res"/><variables name="l" type="java.util.List&lt;a.b.X>"/></v:VariableSet>';
    expect(parseVarType(xml, 'result')).toBe('Res');
    expect(parseVarType(xml, 'nope')).toBeNull();
  });

  it('explores the sample banking project zip', async () => {
    const zipPath = path.resolve(
      __dirname,
      '../../samples/banking-fraud-agent/di-project/Fraud Detection Service.zip',
    );
    const buf = fs.readFileSync(zipPath);
    const { operations, classes, attributes, operationVariables, operationOutputTypes } = await exploreDecisionServiceProject(buf);
    expect(attributes.AccountEvent).toContainEqual({ attributeName: 'accountType', attributeLabel: 'account type' });
    expect(operations.sort()).toEqual(['EvaluateTransaction', 'HandleAccountEvent']);
    expect(operations.length).toBe(2);
    expect(classes.find(c => c.className === 'AccountEvent')?.label).toBe('Account Event');
    // decision model operation: input data of the .dmo, camel-cased
    expect(operationVariables.EvaluateTransaction).toEqual(
      ['theTransaction', 'theAccount', 'thePastTransactions', 'now', 'thePastAccountEvents'],
    );
    // output type: decision model top-level decision / task model OUT variable
    expect(operationOutputTypes).toEqual({
      EvaluateTransaction: 'FraudAssessment',
      HandleAccountEvent: 'AccountEventProcessingResult',
    });
    // task model operation: variables referenced by the .dop
    expect(operationVariables.HandleAccountEvent).toEqual(
      ['accountEvent', 'pastAccountEvents', 'now', 'account', 'result'],
    );
  });
});

describe('buildVocabOptions', () => {
  it('merges classes and operation variables of the given agents', () => {
    const { typeOptions, variablesByOperation, outputFieldsByOperation } = buildVocabOptions([
      { ruleClasses: [{ className: 'A', label: 'Class A' }], ruleOperationVariables: { Op: ['x', 'y'] }, ruleOperationOutputTypes: { Op: 'R' }, ruleAttributes: { R: [{ attributeName: 'f', attributeLabel: 'the f' }] } },
      { ruleClasses: [{ className: 'A', label: 'Class A' }, { className: 'B', label: 'B' }], ruleOperationVariables: { Op: ['y', 'z'] } },
      { label: 'no project' },
    ]);
    expect(typeOptions).toEqual([{ value: 'A', label: 'Class A' }, { value: 'B', label: 'B' }]);
    expect(variablesByOperation).toEqual({ Op: ['x', 'y', 'z'] });
    expect(outputFieldsByOperation).toEqual({ Op: [{ value: 'f', label: 'the f' }] });
  });
});
