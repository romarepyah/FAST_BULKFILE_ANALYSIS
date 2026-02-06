let faFile = null;
let analysisData = null;   // raw analysis JSON (reused for suggestions)
let allSugs = [];          // suggestion objects with .checked
let activeSugCat = null;   // active category filter

// ══════════════════════════════════════════════════════════════
// RULES CONFIGURATION SYSTEM
// ══════════════════════════════════════════════════════════════

// Available metrics for conditions
const RULE_METRICS = {
  acos: { label: 'ACOS (%)', type: 'percent' },
  cvr: { label: 'CVR (%)', type: 'percent' },
  ctr: { label: 'CTR (%)', type: 'percent' },
  spend: { label: 'Spend ($)', type: 'currency' },
  sales: { label: 'Sales ($)', type: 'currency' },
  clicks: { label: 'Clicks', type: 'number' },
  orders: { label: 'Orders', type: 'number' },
  impressions: { label: 'Impressions', type: 'number' },
  roas: { label: 'ROAS', type: 'number' },
  cpc: { label: 'CPC ($)', type: 'currency' },
  bid: { label: 'Bid ($)', type: 'currency' },
  percentage: { label: 'Placement %', type: 'percent' },
};

const RULE_OPERATORS = [
  { value: '>', label: '>' },
  { value: '>=', label: '>=' },
  { value: '<', label: '<' },
  { value: '<=', label: '<=' },
  { value: '==', label: '=' },
  { value: '!=', label: '≠' },
];

// Default rules for each category
const DEFAULT_RULES = {
  placement: [
    {
      id: 'pl_1',
      name: 'High ACOS placement → reduce to 0%',
      enabled: true,
      conditions: [
        { metric: 'acos', operator: '>', value: 35 },
        { metric: 'spend', operator: '>', value: 5 },
        { metric: 'percentage', operator: '>', value: 0 },  // Skip if already 0%
      ],
      action: { type: 'set_percentage', value: 0 },
    },
  ],
  exact: [
    {
      id: 'ex_1',
      name: 'High converting search term → create exact',
      enabled: true,
      conditions: [
        { metric: 'orders', operator: '>=', value: 2 },
        { metric: 'cvr', operator: '>=', value: 20 },
      ],
      action: { type: 'create_campaign', bid_multiplier: 1.1 },
    },
  ],
  bids: [
    {
      id: 'bd_1',
      name: 'Strong performer → increase bid',
      enabled: true,
      conditions: [
        { metric: 'cvr', operator: '>=', value: 30 },
        { metric: 'acos', operator: '<=', value: 20 },
        { metric: 'orders', operator: '>=', value: 3 },
      ],
      action: { type: 'increase_bid', step: 15 },
    },
  ],
  negatives: [
    {
      id: 'ng_1',
      name: 'Wasting spend with no orders → negative',
      enabled: true,
      conditions: [
        { metric: 'clicks', operator: '>=', value: 10 },
        { metric: 'orders', operator: '==', value: 0 },
      ],
      action: { type: 'add_negative', match_type: 'Negative Exact' },
    },
  ],
  pause: [
    {
      id: 'ps_1',
      name: 'No orders with high spend → pause',
      enabled: true,
      conditions: [
        { metric: 'spend', operator: '>=', value: 15 },
        { metric: 'orders', operator: '==', value: 0 },
      ],
      action: { type: 'pause' },
    },
  ],
};

// Current rules state
let customRules = JSON.parse(JSON.stringify(DEFAULT_RULES));

// Load rules from localStorage
function loadRules() {
  try {
    const saved = localStorage.getItem('fa_custom_rules');
    if (saved) {
      customRules = JSON.parse(saved);
    }
  } catch (e) {
    console.warn('Failed to load rules:', e);
  }
  renderAllRules();
}

// Save rules to localStorage
function saveRules() {
  try {
    localStorage.setItem('fa_custom_rules', JSON.stringify(customRules));
  } catch (e) {
    console.warn('Failed to save rules:', e);
  }
}

// Generate unique rule ID
function generateRuleId(category) {
  const prefix = category.substring(0, 2);
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
}

// Add a new rule
function addRule(category) {
  const newRule = {
    id: generateRuleId(category),
    name: 'New Rule',
    enabled: true,
    conditions: [{ metric: 'spend', operator: '>', value: 0 }],
    action: getDefaultAction(category),
  };
  customRules[category].push(newRule);
  saveRules();
  renderRules(category);
}

function getDefaultAction(category) {
  switch (category) {
    case 'placement': return { type: 'set_percentage', value: 0 };
    case 'exact': return { type: 'create_campaign', bid_multiplier: 1.1 };
    case 'bids': return { type: 'increase_bid', step: 15 };
    case 'negatives': return { type: 'add_negative', match_type: 'Negative Exact' };
    case 'pause': return { type: 'pause' };
    default: return {};
  }
}

// Delete a rule
function deleteRule(category, ruleId) {
  customRules[category] = customRules[category].filter(r => r.id !== ruleId);
  saveRules();
  renderRules(category);
}

// Toggle rule enabled/disabled
function toggleRule(category, ruleId) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule) {
    rule.enabled = !rule.enabled;
    saveRules();
    renderRules(category);
  }
}

// Update rule name
function updateRuleName(category, ruleId, name) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule) {
    rule.name = name;
    saveRules();
  }
}

// Add condition to rule
function addCondition(category, ruleId) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule) {
    rule.conditions.push({ metric: 'spend', operator: '>', value: 0 });
    saveRules();
    renderRules(category);
  }
}

// Remove condition from rule
function removeCondition(category, ruleId, condIndex) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule && rule.conditions.length > 1) {
    rule.conditions.splice(condIndex, 1);
    saveRules();
    renderRules(category);
  }
}

// Update condition
function updateCondition(category, ruleId, condIndex, field, value) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule && rule.conditions[condIndex]) {
    rule.conditions[condIndex][field] = field === 'value' ? parseFloat(value) || 0 : value;
    saveRules();
  }
}

// Update action
function updateAction(category, ruleId, field, value) {
  const rule = customRules[category].find(r => r.id === ruleId);
  if (rule) {
    if (field === 'value' || field === 'step' || field === 'bid_multiplier') {
      rule.action[field] = parseFloat(value) || 0;
    } else {
      rule.action[field] = value;
    }
    saveRules();
  }
}

// Move rule up/down
function moveRule(category, ruleId, direction) {
  const rules = customRules[category];
  const idx = rules.findIndex(r => r.id === ruleId);
  const newIdx = idx + direction;
  if (newIdx >= 0 && newIdx < rules.length) {
    [rules[idx], rules[newIdx]] = [rules[newIdx], rules[idx]];
    saveRules();
    renderRules(category);
  }
}

// Render all rules for all categories
function renderAllRules() {
  ['placement', 'exact', 'bids', 'negatives', 'pause'].forEach(renderRules);
}

// Render rules for a specific category
function renderRules(category) {
  const listEl = document.getElementById(`${category}-rules-list`);
  if (!listEl) return;

  const rules = customRules[category] || [];
  listEl.innerHTML = rules.map((rule, idx) => `
    <div class="rule-card ${rule.enabled ? '' : 'disabled'}" data-rule-id="${rule.id}">
      <div class="rule-card-header">
        <input type="text" value="${esc(rule.name)}"
               onchange="updateRuleName('${category}', '${rule.id}', this.value)">
        <div class="rule-card-actions">
          <button onclick="moveRule('${category}', '${rule.id}', -1)" ${idx === 0 ? 'disabled' : ''}>↑</button>
          <button onclick="moveRule('${category}', '${rule.id}', 1)" ${idx === rules.length - 1 ? 'disabled' : ''}>↓</button>
          <button onclick="toggleRule('${category}', '${rule.id}')">${rule.enabled ? '✓ On' : '○ Off'}</button>
          <button class="btn-danger" onclick="deleteRule('${category}', '${rule.id}')">×</button>
        </div>
      </div>
      <div class="rule-conditions">
        <span style="font-size:.8rem;color:#666;margin-right:.3rem">IF</span>
        ${rule.conditions.map((cond, ci) => `
          <div class="rule-condition">
            ${ci > 0 ? '<span style="color:#666">AND</span>' : ''}
            <select onchange="updateCondition('${category}', '${rule.id}', ${ci}, 'metric', this.value)">
              ${Object.entries(RULE_METRICS).map(([k, v]) =>
                `<option value="${k}" ${cond.metric === k ? 'selected' : ''}>${v.label}</option>`
              ).join('')}
            </select>
            <select onchange="updateCondition('${category}', '${rule.id}', ${ci}, 'operator', this.value)">
              ${RULE_OPERATORS.map(op =>
                `<option value="${op.value}" ${cond.operator === op.value ? 'selected' : ''}>${op.label}</option>`
              ).join('')}
            </select>
            <input type="number" value="${cond.value}" step="any"
                   onchange="updateCondition('${category}', '${rule.id}', ${ci}, 'value', this.value)">
            <span class="remove-cond" onclick="removeCondition('${category}', '${rule.id}', ${ci})">×</span>
          </div>
        `).join('')}
        <button class="add-condition-btn" onclick="addCondition('${category}', '${rule.id}')">+ AND</button>
      </div>
      <div class="rule-action">
        <label>THEN:</label>
        ${renderActionFields(category, rule)}
      </div>
    </div>
  `).join('') || '<p class="hint">No rules defined. Click "Add Rule" to create one.</p>';
}

// Render action fields based on category
function renderActionFields(category, rule) {
  const action = rule.action;
  switch (category) {
    case 'placement':
      return `
        <select onchange="updateAction('${category}', '${rule.id}', 'type', this.value)">
          <option value="set_percentage" ${action.type === 'set_percentage' ? 'selected' : ''}>Set placement %</option>
          <option value="reduce_bid" ${action.type === 'reduce_bid' ? 'selected' : ''}>Reduce bid by %</option>
        </select>
        <input type="number" value="${action.value || 0}" step="1" min="0" max="900"
               onchange="updateAction('${category}', '${rule.id}', 'value', this.value)">
        ${action.type === 'set_percentage' ? '%' : '% reduction'}
      `;
    case 'exact':
      return `
        <span>Create exact campaign with bid multiplier:</span>
        <input type="number" value="${action.bid_multiplier || 1.1}" step="0.1" min="0.5" max="3"
               onchange="updateAction('${category}', '${rule.id}', 'bid_multiplier', this.value)">
        <span>× CPC</span>
      `;
    case 'bids':
      return `
        <span>Increase bid by</span>
        <input type="number" value="${action.step || 15}" step="1" min="1" max="100"
               onchange="updateAction('${category}', '${rule.id}', 'step', this.value)">
        <span>%</span>
      `;
    case 'negatives':
      return `
        <span>Add as</span>
        <select onchange="updateAction('${category}', '${rule.id}', 'match_type', this.value)">
          <option value="Negative Exact" ${action.match_type === 'Negative Exact' ? 'selected' : ''}>Negative Exact</option>
          <option value="Negative Phrase" ${action.match_type === 'Negative Phrase' ? 'selected' : ''}>Negative Phrase</option>
        </select>
      `;
    case 'pause':
      return `<span>Pause the target/campaign</span>`;
    default:
      return '';
  }
}

// Reset rules to defaults
function resetRulesToDefaults() {
  if (confirm('Reset all rules to defaults? Your custom rules will be lost.')) {
    customRules = JSON.parse(JSON.stringify(DEFAULT_RULES));
    saveRules();
    renderAllRules();
    showAlert('Rules reset to defaults', 'success');
  }
}

// Export rules to JSON file
function exportRules() {
  const data = JSON.stringify(customRules, null, 2);
  const blob = new Blob([data], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'optimization_rules.json';
  a.click();
  URL.revokeObjectURL(url);
  showAlert('Rules exported', 'success');
}

// Import rules from JSON file
function importRules(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    try {
      const imported = JSON.parse(e.target.result);
      // Validate structure
      const validKeys = ['placement', 'exact', 'bids', 'negatives', 'pause'];
      if (!validKeys.every(k => Array.isArray(imported[k]))) {
        throw new Error('Invalid rules format');
      }
      customRules = imported;
      saveRules();
      renderAllRules();
      showAlert('Rules imported successfully', 'success');
    } catch (err) {
      showAlert('Failed to import rules: ' + err.message, 'error');
    }
  };
  reader.readAsText(file);
  event.target.value = '';
}

// ── Table column definitions ─────────────────────────────────

const TABLE_DEFS = {
  campaigns: {
    id: 'fa-campaigns-tbl',
    storageKey: 'fa_cols_campaigns',
    columns: [
      { key: 'name', label: 'Campaign', render: r => esc(r.name), left: true },
      { key: 'state', label: 'State', render: r => r.state },
      { key: 'targeting_type', label: 'Type', render: r => r.targeting_type },
      { key: 'daily_budget', label: 'Budget', render: r => fmt.usd(r.daily_budget) },
      { key: 'impressions', label: 'Impr.', render: r => fmt.num(r.impressions) },
      { key: 'clicks', label: 'Clicks', render: r => fmt.num(r.clicks) },
      { key: 'ctr', label: 'CTR', render: r => fmt.pct(r.ctr) },
      { key: 'spend', label: 'Spend', render: r => fmt.usd(r.spend) },
      { key: 'sales', label: 'Sales', render: r => fmt.usd(r.sales) },
      { key: 'orders', label: 'Orders', render: r => fmt.num(r.orders) },
      { key: 'acos', label: 'ACOS', render: r => fmt.pct(r.acos) },
      { key: 'roas', label: 'ROAS', render: r => fmt.num(r.roas, 2) },
    ],
  },
  searchTerms: {
    id: 'fa-st-tbl',
    storageKey: 'fa_cols_st',
    columns: [
      { key: 'search_term', label: 'Search Term', render: r => esc(r.search_term), left: true },
      { key: 'impressions', label: 'Impr.', render: r => fmt.num(r.impressions) },
      { key: 'clicks', label: 'Clicks', render: r => fmt.num(r.clicks) },
      { key: 'ctr', label: 'CTR', render: r => fmt.pct(r.ctr) },
      { key: 'spend', label: 'Spend', render: r => fmt.usd(r.spend) },
      { key: 'sales', label: 'Sales', render: r => fmt.usd(r.sales) },
      { key: 'orders', label: 'Orders', render: r => fmt.num(r.orders) },
      { key: 'acos', label: 'ACOS', render: r => fmt.pct(r.acos) },
      { key: 'roas', label: 'ROAS', render: r => fmt.num(r.roas, 2) },
    ],
  },
  wasted: {
    id: 'fa-wasted-tbl',
    storageKey: 'fa_cols_wasted',
    columns: [
      { key: 'search_term', label: 'Search Term', render: r => esc(r.search_term), left: true },
      { key: 'impressions', label: 'Impr.', render: r => fmt.num(r.impressions) },
      { key: 'clicks', label: 'Clicks', render: r => fmt.num(r.clicks) },
      { key: 'ctr', label: 'CTR', render: r => fmt.pct(r.ctr) },
      { key: 'spend', label: 'Spend', render: r => fmt.usd(r.spend) },
      { key: 'sales', label: 'Sales', render: r => fmt.usd(r.sales) },
      { key: 'acos', label: 'ACOS', render: r => fmt.pct(r.acos) },
    ],
  },
  entities: {
    id: 'fa-entity-tbl',
    storageKey: 'fa_cols_entities',
    columns: [
      { key: 'entity', label: 'Entity Type', render: r => esc(r.entity), left: true },
      { key: 'count', label: 'Count', render: r => fmt.num(r.count) },
      { key: 'spend', label: 'Total Spend', render: r => fmt.usd(r.spend) },
    ],
  },
  preview: {
    id: 'fa-preview-tbl',
    storageKey: 'fa_cols_preview',
    columns: [
      { key: 'Entity', label: 'Entity', render: r => esc(r.Entity || ''), left: true },
      { key: 'Operation', label: 'Operation', render: r => esc(r.Operation || '') },
      { key: 'CampaignName', label: 'Campaign', render: r => esc(r['Campaign Name'] || r['Campaign ID'] || '') },
      { key: 'AdGroupName', label: 'Ad Group', render: r => esc(r['Ad Group Name'] || r['Ad Group ID'] || '') },
      { key: 'KeywordText', label: 'Keyword / Target', render: r => esc(r['Keyword Text'] || r['Product Targeting Expression'] || '') },
      { key: 'MatchType', label: 'Match Type', render: r => esc(r['Match Type'] || '') },
      { key: 'State', label: 'State', render: r => esc(r.State || '') },
      { key: 'Bid', label: 'Bid', render: r => r.Bid ? fmt.usd(r.Bid) : '' },
      { key: 'Placement', label: 'Placement', render: r => esc(r.Placement || '') },
      { key: 'Percentage', label: 'Percentage', render: r => r.Percentage !== undefined && r.Percentage !== '' ? r.Percentage + '%' : '' },
    ],
  },
};

// ── Table Manager class ──────────────────────────────────────

class TableManager {
  constructor(name) {
    const def = TABLE_DEFS[name];
    this.name = name;
    this.tableId = def.id;
    this.storageKey = def.storageKey;
    this.colDefs = def.columns;
    this.state = this._loadState();
    this.data = [];
  }

  _defaultState() {
    return this.colDefs.map(c => ({ key: c.key, visible: true }));
  }

  _loadState() {
    try {
      const raw = sessionStorage.getItem(this.storageKey);
      if (raw) {
        const saved = JSON.parse(raw);
        const known = new Set(this.colDefs.map(c => c.key));
        const inSaved = new Set(saved.map(s => s.key));
        const result = saved.filter(s => known.has(s.key));
        this.colDefs.forEach(c => {
          if (!inSaved.has(c.key)) result.push({ key: c.key, visible: true });
        });
        return result;
      }
    } catch (e) {}
    return this._defaultState();
  }

  _saveState() {
    try { sessionStorage.setItem(this.storageKey, JSON.stringify(this.state)); } catch (e) {}
  }

  _def(key) { return this.colDefs.find(c => c.key === key); }

  visibleCols() {
    return this.state.filter(s => s.visible).map(s => this._def(s.key)).filter(Boolean);
  }

  setData(data) {
    this.data = data;
    this.render();
  }

  render() {
    const tbl = document.getElementById(this.tableId);
    if (!tbl) return;
    const cols = this.visibleCols();

    tbl.querySelector('thead').innerHTML = '<tr>' +
      cols.map(c =>
        '<th style="text-align:' + (c.left ? 'left' : 'right') + '">' + c.label + '</th>'
      ).join('') + '</tr>';

    tbl.querySelector('tbody').innerHTML = this.data.map(row =>
      '<tr>' + cols.map(c =>
        '<td style="text-align:' + (c.left ? 'left' : 'right') + '">' + c.render(row) + '</td>'
      ).join('') + '</tr>'
    ).join('');
  }

  toggleCol(key) {
    const s = this.state.find(s => s.key === key);
    if (s) { s.visible = !s.visible; this._saveState(); this.render(); }
  }

  moveCol(key, dir) {
    const i = this.state.findIndex(s => s.key === key);
    const j = i + dir;
    if (i < 0 || j < 0 || j >= this.state.length) return;
    [this.state[i], this.state[j]] = [this.state[j], this.state[i]];
    this._saveState();
    this.render();
  }

  reset() {
    this.state = this._defaultState();
    this._saveState();
    this.render();
  }
}

// Initialise table managers
const tables = {};
Object.keys(TABLE_DEFS).forEach(name => { tables[name] = new TableManager(name); });

// ── Column settings dropdown ─────────────────────────────────

function openColSettings(manager, btnEl) {
  // Close any already-open panel
  document.querySelectorAll('.col-settings-panel').forEach(el => el.remove());

  const panel = document.createElement('div');
  panel.className = 'col-settings-panel';

  function build() {
    panel.innerHTML =
      '<div class="col-settings-head"><strong>Columns</strong>' +
      '<button class="btn btn-sm" data-act="reset">Reset</button></div>' +
      '<div class="col-settings-body">' +
      manager.state.map((s, i) => {
        const d = manager._def(s.key);
        if (!d) return '';
        return '<div class="col-settings-row" data-key="' + s.key + '">' +
          '<label><input type="checkbox" ' + (s.visible ? 'checked' : '') + '> ' + d.label + '</label>' +
          '<span class="col-arrows">' +
          '<button data-dir="-1"' + (i === 0 ? ' disabled' : '') + '>&#9664;</button>' +
          '<button data-dir="1"' + (i === manager.state.length - 1 ? ' disabled' : '') + '>&#9654;</button>' +
          '</span></div>';
      }).join('') +
      '</div>';

    panel.querySelector('[data-act="reset"]').onclick = () => { manager.reset(); build(); };
    panel.querySelectorAll('.col-settings-row').forEach(row => {
      const key = row.dataset.key;
      row.querySelector('input').onchange = () => { manager.toggleCol(key); build(); };
      row.querySelectorAll('[data-dir]').forEach(b => {
        b.onclick = () => { manager.moveCol(key, +b.dataset.dir); build(); };
      });
    });
  }

  build();
  btnEl.parentElement.style.position = 'relative';
  btnEl.parentElement.appendChild(panel);

  // Close on outside click
  const closer = (e) => {
    if (!panel.contains(e.target) && e.target !== btnEl) {
      panel.remove();
      document.removeEventListener('mousedown', closer);
    }
  };
  setTimeout(() => document.addEventListener('mousedown', closer), 0);
}

// ── Initialisation ───────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  const zone = document.getElementById('fa-drop-zone');
  const input = document.getElementById('fa-file-input');
  const btn = document.getElementById('fa-analyze-btn');

  zone.addEventListener('click', () => input.click());
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('over'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('over');
    pickFile(e.dataTransfer.files[0]);
  });
  input.addEventListener('change', () => { if (input.files[0]) pickFile(input.files[0]); });
  btn.addEventListener('click', analyze);

  // Tab switching
  document.querySelectorAll('.tab-btn').forEach(b => {
    b.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(x => x.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      document.getElementById(b.dataset.tab).classList.add('active');
    });
  });

  // Column settings buttons
  document.querySelectorAll('.col-settings-btn').forEach(b => {
    b.addEventListener('click', () => {
      const name = b.dataset.table;
      if (tables[name]) openColSettings(tables[name], b);
    });
  });

  // Suggestion controls
  document.getElementById('fa-select-all').addEventListener('click', sugSelectAllVisible);
  document.getElementById('fa-deselect-all').addEventListener('click', sugDeselectAll);
  document.getElementById('fa-preview-btn').addEventListener('click', showPreview);
  document.getElementById('fa-generate-csv').addEventListener('click', generateCSV);
  document.getElementById('fa-rerun-btn').addEventListener('click', () => runSuggestions());

  // Rules tabs switching
  document.querySelectorAll('.rules-tab').forEach(b => {
    b.addEventListener('click', () => {
      document.querySelectorAll('.rules-tab').forEach(x => x.classList.remove('active'));
      document.querySelectorAll('.rules-content').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      document.getElementById(b.dataset.rulesTab).classList.add('active');
    });
  });

  // Load custom rules from localStorage
  loadRules();

  // Restore previous session state (survives tab navigation)
  restoreState();
});

// ── File handling ────────────────────────────────────────────

function pickFile(f) {
  if (!f) return;
  const lname = f.name.toLowerCase();
  if (!lname.endsWith('.xlsx') && !lname.endsWith('.xls')) {
    showAlert('Please select an .xlsx or .xls file', 'error');
    return;
  }
  faFile = f;
  document.getElementById('fa-file-info').innerHTML =
    `<div class="file-item"><span>${f.name} (${(f.size/1024).toFixed(0)} KB)</span>
     <button class="btn btn-sm btn-danger" onclick="clearFile()">Remove</button></div>`;
  document.getElementById('fa-analyze-btn').disabled = false;
}

function clearFile() {
  faFile = null;
  analysisData = null;
  allSugs = [];
  activeSugCat = null;
  document.getElementById('fa-file-info').innerHTML = '';
  document.getElementById('fa-analyze-btn').disabled = true;
  document.getElementById('fa-file-input').value = '';
  document.getElementById('fa-results').style.display = 'none';
  document.getElementById('fa-suggestions-section').style.display = 'none';
  clearSessionState();
}

async function analyze() {
  if (!faFile) return;
  const btn = document.getElementById('fa-analyze-btn');
  btn.disabled = true;
  document.getElementById('fa-loading').style.display = '';
  document.getElementById('fa-results').style.display = 'none';
  document.getElementById('fa-suggestions-section').style.display = 'none';
  document.getElementById('fa-progress').style.width = '40%';

  const fd = new FormData();
  fd.append('file', faFile);

  try {
    document.getElementById('fa-progress').style.width = '70%';
    const r = await fetch('/api/fast-analysis', { method: 'POST', body: fd });
    const d = await r.json();
    document.getElementById('fa-progress').style.width = '100%';

    if (!d.success) throw new Error(d.error || 'Analysis failed');

    analysisData = d.analysis;
    renderResults(analysisData);
    document.getElementById('fa-results').style.display = '';

    // Auto-run suggestions
    await runSuggestions();

    showAlert('Analysis complete', 'success');
    saveState();
  } catch(e) {
    showAlert('Analysis failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    setTimeout(() => { document.getElementById('fa-loading').style.display = 'none'; }, 800);
  }
}

// ── Render analysis tables ──────────────────────────────────

function renderResults(a) {
  const ov = a.overview;
  const ps = a.performance_summary;

  const cards = [
    { label: 'Campaigns',   val: fmt.num(ov.campaigns) },
    { label: 'Ad Groups',   val: fmt.num(ov.ad_groups) },
    { label: 'Keywords',    val: fmt.num(ov.keywords) },
    { label: 'Total Spend', val: fmt.usd(ps.spend) },
    { label: 'Total Sales', val: fmt.usd(ps.sales) },
    { label: 'Orders',      val: fmt.num(ps.orders) },
    { label: 'ACOS',        val: fmt.pct(ps.acos) },
    { label: 'ROAS',        val: fmt.num(ps.roas, 2) },
  ];
  document.getElementById('fa-cards').innerHTML = cards.map(c =>
    `<div class="card"><div class="card-label">${c.label}</div><div class="card-val">${c.val}</div></div>`
  ).join('');

  document.getElementById('fa-sheets').innerHTML = a.sheets_found.map(s =>
    `<span class="chip">${s.name} <strong>${s.rows}</strong></span>`
  ).join('');

  // Render data tables via TableManager
  tables.campaigns.setData(a.campaigns_table);
  tables.searchTerms.setData(a.search_terms_top);
  tables.wasted.setData(a.search_terms_wasted);
  tables.entities.setData(a.entity_breakdown);
}

// ── Suggestions ─────────────────────────────────────────────

function getThresholds() {
  const getVal = (id, def) => {
    const el = document.getElementById(id);
    return el ? parseFloat(el.value) || def : def;
  };
  const getInt = (id, def) => {
    const el = document.getElementById(id);
    return el ? parseInt(el.value) || def : def;
  };

  return {
    // Placement optimization
    acos_ineffective:     getVal('th-acos-ineffective', 35) / 100,
    max_placement_pct:    getInt('th-max-placement', 900),
    bid_reduction_ratio:  getVal('th-bid-reduction', 0.5),

    // Create exact campaign
    acos_target:          getVal('th-acos-target', 30) / 100,
    orders_create_exact:  getInt('th-orders-exact', 2),
    cvr_create_exact:     getVal('th-cvr-exact', 20) / 100,
    bid_multiplier:       getVal('th-bid-multiplier', 1.1),

    // Increase bids
    cvr_bid_increase:     getVal('th-cvr-bid', 30) / 100,
    acos_bid_increase:    getVal('th-acos-bid', 20) / 100,
    acos_target_increase: getVal('th-acos-ceiling', 25) / 100,
    bid_increase_step:    getVal('th-bid-step', 15) / 100,
    orders_bid_increase:  getInt('th-orders-bid', 3),
    clicks_bid_increase:  getInt('th-clicks-bid', 10),

    // Search term negatives
    clicks_negative:      getInt('th-clicks-neg', 10),
    spend_negative:       getVal('th-spend-neg', 5),
    negative_match_type:  document.getElementById('th-neg-match-type')?.value || 'Negative Exact',

    // Pause targets/campaigns
    spend_campaign_pause: getVal('th-spend-camp', 15),
    spend_target_pause:   getVal('th-spend-target', 10),
    acos_pause:           getVal('th-acos-pause', 100) / 100,

    // Custom rules (sent to backend for rule-based processing)
    custom_rules:         customRules,
  };
}

async function runSuggestions() {
  if (!analysisData) return;
  const section = document.getElementById('fa-suggestions-section');
  section.style.display = '';

  const listEl = document.getElementById('fa-sug-list');
  listEl.innerHTML = '<div class="panel" style="text-align:center;color:#888">Computing suggestions...</div>';

  // Clear preview panel when rerunning
  document.getElementById('fa-preview-panel').style.display = 'none';

  try {
    const r = await fetch('/api/fast-analysis/suggestions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ analysis: analysisData, thresholds: getThresholds() }),
    });
    const d = await r.json();
    if (!d.success) throw new Error(d.error || 'Failed');

    // Reset all suggestions with new data
    allSugs = (d.suggestions || []).map(s => ({...s, checked: false}));
    activeSugCat = null;

    // Update UI
    renderSugChips();
    renderSugs();
    updateSugCount();
    document.getElementById('fa-sug-action-bar').style.display = allSugs.length ? '' : 'none';
    saveState();
  } catch(e) {
    listEl.innerHTML = `<div class="panel"><div class="alert alert-error">Error: ${esc(e.message)}</div></div>`;
  }
}

function renderSugChips() {
  const cats = {};
  allSugs.forEach(s => { cats[s.category] = (cats[s.category]||0)+1; });
  const el = document.getElementById('fa-sug-chips');
  el.innerHTML = '';

  const all = document.createElement('span');
  all.className = 'chip' + (activeSugCat===null?' active':'');
  all.textContent = `All (${allSugs.length})`;
  all.onclick = () => { activeSugCat=null; renderSugChips(); renderSugs(); };
  el.appendChild(all);

  Object.entries(cats).sort((a,b)=>b[1]-a[1]).forEach(([cat, n]) => {
    const c = document.createElement('span');
    c.className = 'chip' + (activeSugCat===cat?' active':'');
    c.textContent = `${cat} (${n})`;
    c.onclick = () => { activeSugCat=cat; renderSugChips(); renderSugs(); };
    el.appendChild(c);
  });
}

function getVisibleSugs() {
  return allSugs.filter(s => !activeSugCat || s.category === activeSugCat);
}

function renderSugs() {
  const list = document.getElementById('fa-sug-list');
  list.innerHTML = '';
  document.getElementById('fa-preview-panel').style.display = 'none';

  const visible = getVisibleSugs();
  if (!visible.length) {
    list.innerHTML = '<div class="panel" style="text-align:center;color:#888">No suggestions for the current thresholds.</div>';
    return;
  }

  const groups = {};
  visible.forEach(s => { (groups[s.category] = groups[s.category]||[]).push(s); });

  for (const [cat, items] of Object.entries(groups)) {
    // Each category group is a collapsible <details>
    const group = document.createElement('details');
    group.className = 'sug-group';
    group.open = true;

    const header = document.createElement('summary');
    header.className = 'sug-group-header';
    const sev = items[0].severity;
    header.innerHTML = `<h2>${cat}</h2><span class="badge badge-${sev}">${items.length}</span>`;
    group.appendChild(header);

    items.forEach(s => {
      const card = document.createElement('div');
      card.className = 'sug-card';
      const hasActions = s.actions && s.actions.length > 0;
      card.innerHTML = `
        ${hasActions ? `<input type="checkbox" ${s.checked?'checked':''} data-id="${s.id}">` : '<div style="width:18px"></div>'}
        <div class="sug-body">
          <div class="sug-title">${esc(s.title)}</div>
          <div class="sug-detail">${esc(s.detail)}</div>
          <div class="sug-metrics">${renderMetrics(s.metrics)}</div>
          ${renderSugExtras(s)}
        </div>
      `;
      if (hasActions) {
        card.querySelector('input').addEventListener('change', e => {
          s.checked = e.target.checked;
          updateSugCount();
        });
      }
      // Wire up SKU dropdown
      const skuSel = card.querySelector('.sug-sku-select');
      if (skuSel) {
        skuSel.addEventListener('change', e => {
          const newSku = e.target.value;
          s.metrics.sku = newSku;
          // Update Product Ad action's SKU
          (s.actions || []).forEach(a => {
            if (a.Entity === 'Product Ad') a.SKU = newSku;
          });
        });
      }
      // Wire up Portfolio dropdown
      const portSel = card.querySelector('.sug-portfolio-select');
      if (portSel) {
        portSel.addEventListener('change', e => {
          const newPid = e.target.value;
          s.metrics.source_portfolio_id = newPid;
          // Update Campaign action's Portfolio ID
          (s.actions || []).forEach(a => {
            if (a.Entity === 'Campaign' && a.Operation === 'Create') {
              a['Portfolio ID'] = newPid;
            }
          });
        });
      }
      group.appendChild(card);
    });
    list.appendChild(group);
  }
  updateSugCount();
}

// Keys to skip in the generic metrics display (shown via dedicated UI instead)
const _SKIP_METRIC_KEYS = new Set([
  'amazon_url', 'available_skus', 'source_portfolio_id', 'source_portfolio_name',
]);

function renderMetrics(m) {
  if (!m) return '';
  return Object.entries(m).filter(([k]) => !_SKIP_METRIC_KEYS.has(k)).map(([k,v]) => {
    if (typeof v === 'number') {
      if (k.includes('acos')||k.includes('ctr')||k.includes('cvr')) v = fmt.pct(v,1);
      else if (k.includes('spend')||k.includes('sales')||k.includes('budget')||k.includes('cpc')||k.includes('bid')) v = fmt.usd(v);
      else v = fmt.num(v);
    }
    if (Array.isArray(v)) return '';
    return `<span><strong>${k.replace(/_/g,' ')}:</strong> ${v}</span>`;
  }).join('');
}

function renderSugExtras(s) {
  const m = s.metrics || {};
  const parts = [];

  // Amazon search link (for search-term-based suggestions)
  if (m.amazon_url) {
    parts.push(
      `<a href="${esc(m.amazon_url)}" target="_blank" rel="noopener" class="sug-amazon-link">` +
      `&#128269; View on Amazon</a>`
    );
  }

  // SKU chooser (Create Exact Campaign only)
  if (m.available_skus && m.available_skus.length > 0) {
    const currentSku = m.sku || '';
    let opts = m.available_skus.map(sk =>
      `<option value="${esc(sk)}"${sk === currentSku ? ' selected' : ''}>${esc(sk)}</option>`
    ).join('');
    if (!currentSku || !m.available_skus.includes(currentSku)) {
      opts = `<option value="">— select SKU —</option>` + opts;
    }
    parts.push(
      `<span class="sug-chooser"><strong>SKU:</strong> ` +
      `<select class="sug-sku-select">${opts}</select></span>`
    );
  } else if (m.sku !== undefined) {
    parts.push(`<span class="sug-chooser"><strong>SKU:</strong> ${esc(m.sku) || '—'}</span>`);
  }

  // Portfolio chooser (Create Exact Campaign only)
  if (s.category === 'Create Exact Campaign' && analysisData && analysisData.portfolios) {
    const portfolios = analysisData.portfolios;
    const currentPid = m.source_portfolio_id || '';
    const currentName = m.source_portfolio_name || '';
    if (portfolios.length > 0) {
      let opts = `<option value="">— no portfolio —</option>`;
      opts += portfolios.map(p => {
        const label = p.name ? `${p.name} (${p.portfolio_id})` : p.portfolio_id;
        return `<option value="${esc(p.portfolio_id)}"${p.portfolio_id === currentPid ? ' selected' : ''}>${esc(label)}</option>`;
      }).join('');
      parts.push(
        `<span class="sug-chooser"><strong>Portfolio:</strong> ` +
        `<select class="sug-portfolio-select">${opts}</select></span>`
      );
    } else if (currentPid) {
      const label = currentName ? `${currentName} (${currentPid})` : currentPid;
      parts.push(`<span class="sug-chooser"><strong>Portfolio:</strong> ${esc(label)}</span>`);
    }
  }

  if (!parts.length) return '';
  return `<div class="sug-extras">${parts.join('')}</div>`;
}

function updateSugCount() {
  const n = allSugs.filter(s => s.checked).length;
  document.getElementById('fa-sug-count').textContent = n + ' selected';
  document.getElementById('fa-generate-csv').disabled = n === 0;
  document.getElementById('fa-preview-btn').disabled = n === 0;
}

function sugSelectAllVisible() {
  getVisibleSugs().forEach(s => { if (s.actions && s.actions.length) s.checked = true; });
  renderSugs();
}

function sugDeselectAll() {
  allSugs.forEach(s => s.checked = false);
  renderSugs();
}

// ── Preview ─────────────────────────────────────────────────

function showPreview() {
  const selected = allSugs.filter(s => s.checked && s.actions && s.actions.length);
  if (!selected.length) return;

  const actions = selected.flatMap(s => s.actions);
  tables.preview.setData(actions);

  const panel = document.getElementById('fa-preview-panel');
  panel.style.display = '';
  panel.open = true;
  panel.scrollIntoView({behavior:'smooth'});
}

// ── Generate CSV ────────────────────────────────────────────

async function generateCSV() {
  const selected = allSugs.filter(s => s.checked && s.actions && s.actions.length);
  if (!selected.length) return;

  try {
    const r = await fetch('/api/fast-analysis/bulk-csv', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ suggestions: selected }),
    });
    if (!r.ok) {
      const d = await r.json();
      throw new Error(d.error || 'CSV generation failed');
    }
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'bulk_actions.xlsx';
    a.click();
    URL.revokeObjectURL(url);

    const totalActions = selected.reduce((n, s) => n + s.actions.length, 0);
    showAlert(`Bulk Excel downloaded: ${totalActions} actions`);
  } catch(e) {
    showAlert('CSV error: ' + e.message, 'error');
  }
}

// ── Utility ─────────────────────────────────────────────────

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

// ── State persistence (survives tab navigation) ─────────────

function saveState() {
  try {
    if (analysisData) {
      sessionStorage.setItem('fa_analysisData', JSON.stringify(analysisData));
    }
    if (allSugs.length) {
      sessionStorage.setItem('fa_allSugs', JSON.stringify(allSugs));
    }
    sessionStorage.setItem('fa_activeSugCat', JSON.stringify(activeSugCat));
  } catch(e) {
    // sessionStorage quota exceeded or unavailable – silently ignore
  }
}

function restoreState() {
  try {
    const savedAnalysis = sessionStorage.getItem('fa_analysisData');
    const savedSugs = sessionStorage.getItem('fa_allSugs');

    if (savedAnalysis) {
      analysisData = JSON.parse(savedAnalysis);
      renderResults(analysisData);
      document.getElementById('fa-results').style.display = '';
    }
    if (savedSugs) {
      allSugs = JSON.parse(savedSugs);
      const savedCat = sessionStorage.getItem('fa_activeSugCat');
      activeSugCat = savedCat ? JSON.parse(savedCat) : null;

      if (allSugs.length) {
        document.getElementById('fa-suggestions-section').style.display = '';
        renderSugChips();
        renderSugs();
        document.getElementById('fa-sug-action-bar').style.display = '';
      }
    }
  } catch(e) {
    // corrupted or unavailable – start fresh
    clearSessionState();
  }
}

function clearSessionState() {
  sessionStorage.removeItem('fa_analysisData');
  sessionStorage.removeItem('fa_allSugs');
  sessionStorage.removeItem('fa_activeSugCat');
}

window.addEventListener('beforeunload', saveState);
