/*
 * Decision Service Project (.zip) explorer.
 *
 * Scans a zipped ODM/DI decision service project and extracts:
 *   - operations: the value of <tags key="operationName"> of every *.dop file
 *   - classes:    every "<qualified.ClassName>#concept.label = <label>" line of
 *                 every *.voc file, as { className, label }
 *   - attributes: every "<qualified.ClassName>.<attr>#phrase.navigation = {<label>} of {this}"
 *                 line of every *.voc file, as { [className]: [{ attributeName, attributeLabel }] }
 *   - operationVariables: { [operationName]: [variableName] }, the variables exposed by each operation
 *                 - task model (.dop has <ruleflow>): the <referencedVariables variableName> of the .dop
 *                 - decision model (.dop has <decisionModel href>): the .dmo's
 *                   <inputData>/<extensionElements>/<initialization>/<output name="the past transactions">,
 *                   converted to a variable name ("thePastTransactions")
 *   - operationOutputTypes: { [operationName]: className }, the type of the value returned by the operation
 *                 - task model: type (in the .var variable set) of the <referencedVariables direction="OUT"> variable
 *                 - decision model: bomType of <decision>/<extensionElements>/<ruleset>/<output> of the
 *                   top-level decision (the one no other decision requires)
 * *.bom files are visited as part of the exploration but carry no extra data.
 */
import JSZip from 'jszip';

const CONCEPT_LABEL_RE = /^\s*([^#=\s][^#]*?)#concept\.label\s*=\s*(.*?)\s*$/;
// key must not contain whitespace or "(" so constructor/method phrases are excluded
const ATTRIBUTE_NAVIGATION_RE = /^\s*([^#=\s()]+)#phrase\.navigation\s*=\s*(.*?)\s*$/;

/** "the past transactions" -> "thePastTransactions" */
export function toVariableName(verbalization) {
  return String(verbalization || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((w, i) => w.charAt(0)[i === 0 ? 'toLowerCase' : 'toUpperCase']() + w.slice(1))
    .join('');
}

/** "a.b.generateddatamodel.FraudAssessment" -> "FraudAssessment" */
export function simpleClassName(qualifiedType) {
  return String(qualifiedType || '').trim().split('.').pop().replace(/[<>[\]\s].*$/, '');
}

/**
 * Returns { name, isTaskModel, variables, outputVariable, decisionModelHref } or null when the XML is invalid.
 * variables = <referencedVariables> names (task models only).
 * outputVariable = { name, variableSetHref } of the direction="OUT" variable (task models only).
 */
export function parseDopDetails(xml) {
  const doc = new DOMParser().parseFromString(xml, 'application/xml');
  if (doc.getElementsByTagName('parsererror').length) return null;
  const children = [...doc.documentElement.children];
  const tag = children.find(
    c => c.localName === 'tags' && c.getAttribute('key') === 'operationName',
  );
  const valueEl = tag && [...tag.children].find(c => c.localName === 'value');
  // The value is a CDATA string that includes surrounding quotes: "EvaluateTransaction"
  const name = valueEl?.textContent?.trim().replace(/^"(.*)"$/, '$1').trim() || null;
  const isTaskModel = children.some(c => c.localName === 'ruleflow');
  const variables = isTaskModel
    ? children
        .filter(c => c.localName === 'referencedVariables' && c.getAttribute('variableName'))
        .map(c => c.getAttribute('variableName'))
    : [];
  const out = isTaskModel
    ? children.find(c => c.localName === 'referencedVariables' && c.getAttribute('direction') === 'OUT')
    : null;
  const outputVariable = out
    ? {
        name: out.getAttribute('variableName'),
        variableSetHref: [...out.children].find(c => c.localName === 'variableSet')?.getAttribute('href') || null,
      }
    : null;
  const decisionModelHref = children.find(c => c.localName === 'decisionModel')?.getAttribute('href') || null;
  return { name, isTaskModel, variables, outputVariable, decisionModelHref };
}

/** Class name of the type of variable `variableName` in a .var variable set, or null. */
export function parseVarType(xml, variableName) {
  const doc = new DOMParser().parseFromString(xml, 'application/xml');
  if (doc.getElementsByTagName('parsererror').length) return null;
  const v = [...doc.getElementsByTagNameNS('*', 'variables')].find(e => e.getAttribute('name') === variableName)
    || [...doc.getElementsByTagName('variables')].find(e => e.getAttribute('name') === variableName);
  return v ? simpleClassName(v.getAttribute('type')) || null : null;
}

/**
 * Class name of the output of a decision model (.dmo): the <ruleset>/<output> bomType of the
 * top-level decision, i.e. the decision that is not required by any other decision.
 */
export function parseDmoOutputType(xml) {
  const doc = new DOMParser().parseFromString(xml, 'application/xml');
  if (doc.getElementsByTagName('parsererror').length) return null;
  const required = new Set(
    [...doc.getElementsByTagNameNS('*', 'requiredDecision')].map(e => (e.getAttribute('href') || '').replace(/^#/, '')),
  );
  for (const decision of doc.getElementsByTagNameNS('*', 'decision')) {
    if (required.has(decision.getAttribute('id'))) continue;
    for (const ruleset of decision.getElementsByTagNameNS('*', 'ruleset')) {
      const output = [...ruleset.children].find(c => c.localName === 'output');
      const type = output && simpleClassName(output.getAttribute('bomType'));
      if (type) return type;
    }
  }
  return null;
}

export function parseDop(xml) {
  return parseDopDetails(xml)?.name ?? null;
}

/** Variable names of a decision model (.dmo): the <output name> of each <inputData> initialization. */
export function parseDmoVariables(xml) {
  const doc = new DOMParser().parseFromString(xml, 'application/xml');
  if (doc.getElementsByTagName('parsererror').length) return [];
  const names = [];
  for (const input of doc.getElementsByTagNameNS('*', 'inputData')) {
    for (const init of input.getElementsByTagNameNS('*', 'initialization')) {
      for (const out of init.getElementsByTagNameNS('*', 'output')) {
        const v = toVariableName(out.getAttribute('name'));
        if (v && !names.includes(v)) names.push(v);
      }
    }
  }
  return names;
}

/** Resolves a relative (possibly URL-encoded, "#fragment") href against the path of the referencing file. */
function resolveHref(fromPath, href) {
  const parts = fromPath.split('/').slice(0, -1);
  for (const seg of decodeURIComponent(href.split('#')[0]).split('/')) {
    if (seg === '..') parts.pop();
    else if (seg && seg !== '.') parts.push(seg);
  }
  return parts.join('/');
}

export function parseVoc(text) {
  const classes = [];
  for (const line of text.split(/\r?\n/)) {
    const m = CONCEPT_LABEL_RE.exec(line);
    if (!m) continue;
    // Class name = text between the last "." and the "#"
    const className = m[1].split('.').pop();
    if (className) classes.push({ className, label: m[2] });
  }
  return classes;
}

/*
 * Extracts class attributes from "<pkg>.<Class>.<attr>#phrase.navigation = {<label>} of {this}".
 * Returns { [className]: [{ attributeName, attributeLabel }] }.
 */
export function parseVocAttributes(text) {
  const attributes = {};
  for (const line of text.split(/\r?\n/)) {
    const m = ATTRIBUTE_NAVIGATION_RE.exec(line);
    if (!m) continue;
    const parts = m[1].split('.');
    if (parts.length < 2) continue;
    const attributeName = parts.pop();
    const className = parts.pop();
    // Label = first {placeholder} that is not {this}
    const attributeLabel = [...m[2].matchAll(/\{([^}]*)\}/g)].map(x => x[1]).find(l => l !== 'this');
    if (!className || !attributeName || !attributeLabel) continue;
    const list = (attributes[className] ||= []);
    if (!list.some(a => a.attributeName === attributeName)) list.push({ attributeName, attributeLabel });
  }
  return attributes;
}

/*
 * Builds the options of the "Type" (class) and "Field" combo boxes from the data of the
 * given agent nodes (callers pass only the agents in scope).
 * Returns {
 *   typeOptions: [{ value, label }],                       classes
 *   variablesByOperation: { [operation]: [variableName] }, Field of an operation
 *   outputFieldsByOperation: { [operation]: [{ value, label }] }, Field of an outbound link:
 *                           attributes of the type of the operation's output
 * }
 */
export function buildVocabOptions(agentDatas) {
  const typeOptions = [];
  const seenTypes = new Set();
  const variablesByOperation = {};
  const outputFieldsByOperation = {};
  for (const d of agentDatas || []) {
    for (const c of d?.ruleClasses || []) {
      if (seenTypes.has(c.className)) continue;
      seenTypes.add(c.className);
      typeOptions.push({ value: c.className, label: c.label });
    }
    for (const [op, vars] of Object.entries(d?.ruleOperationVariables || {})) {
      const list = (variablesByOperation[op] ||= []);
      for (const v of vars) if (!list.includes(v)) list.push(v);
    }
    for (const [op, className] of Object.entries(d?.ruleOperationOutputTypes || {})) {
      const list = (outputFieldsByOperation[op] ||= []);
      for (const a of d?.ruleAttributes?.[className] || []) {
        if (!list.some(o => o.value === a.attributeName)) {
          list.push({ value: a.attributeName, label: a.attributeLabel });
        }
      }
    }
  }
  return { typeOptions, variablesByOperation, outputFieldsByOperation };
}

export async function exploreDecisionServiceProject(file) {
  const zip = await JSZip.loadAsync(file);
  const operations = [];
  const classes = [];
  const attributes = {};
  const operationVariables = {};
  const operationOutputTypes = {};
  const seenClasses = new Set();

  const entries = Object.values(zip.files).filter(
    e => !e.dir && !e.name.startsWith('__MACOSX/') && !e.name.split('/').pop().startsWith('._'),
  );

  for (const entry of entries) {
    const lower = entry.name.toLowerCase();
    if (lower.endsWith('.dop')) {
      const dop = parseDopDetails(await entry.async('string'));
      const name = dop?.name;
      if (name && !operations.includes(name)) operations.push(name);
      if (name) {
        let variables = dop.variables;
        let outputType = null;
        if (dop.isTaskModel && dop.outputVariable?.variableSetHref) {
          const varFile = zip.file(resolveHref(entry.name, dop.outputVariable.variableSetHref));
          if (varFile) outputType = parseVarType(await varFile.async('string'), dop.outputVariable.name);
        } else if (dop.decisionModelHref) {
          const dmo = zip.file(resolveHref(entry.name, dop.decisionModelHref));
          if (dmo) {
            const dmoXml = await dmo.async('string');
            variables = parseDmoVariables(dmoXml);
            outputType = parseDmoOutputType(dmoXml);
          }
        }
        if (outputType) operationOutputTypes[name] = outputType;
        const list = (operationVariables[name] ||= []);
        for (const v of variables) if (!list.includes(v)) list.push(v);
      }
    } else if (lower.endsWith('.voc')) {
      const text = await entry.async('string');
      for (const c of parseVoc(text)) {
        if (seenClasses.has(c.className)) continue;
        seenClasses.add(c.className);
        classes.push(c);
      }
      for (const [className, attrs] of Object.entries(parseVocAttributes(text))) {
        const list = (attributes[className] ||= []);
        for (const a of attrs) {
          if (!list.some(x => x.attributeName === a.attributeName)) list.push(a);
        }
      }
    }
    // .bom files: explored but nothing to extract
  }

  return { operations, classes, attributes, operationVariables, operationOutputTypes };
}
