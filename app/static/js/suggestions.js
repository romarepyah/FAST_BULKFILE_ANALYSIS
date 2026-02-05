let allSuggestions = [], activeCategory = null;

document.addEventListener('DOMContentLoaded', async () => {
  await loadFilters();
  setDefaultDates();
  document.getElementById('sug-filter').addEventListener('submit', e => { e.preventDefault(); loadSuggestions(); });
  document.getElementById('select-all-btn').addEventListener('click', selectAllVisible);
  document.getElementById('deselect-all-btn').addEventListener('click', deselectAll);
  document.getElementById('generate-bulk-btn').addEventListener('click', generateBulk);
});

function setDefaultDates() {
  const today = new Date(), ago = new Date(today);
  ago.setDate(today.getDate() - 30);
  document.getElementById('sf-to').value = today.toISOString().slice(0,10);
  document.getElementById('sf-from').value = ago.toISOString().slice(0,10);
}

async function loadFilters() {
  try {
    const d = await api('/api/metrics/filters');
    const f = d.filters;
    const acct = document.getElementById('sf-account');
    (f.accounts||[]).forEach(a => { const o = document.createElement('option'); o.value=a.id; o.textContent=a.name||a.id; acct.appendChild(o); });
    const port = document.getElementById('sf-portfolio');
    (f.portfolios||[]).forEach(a => { const o = document.createElement('option'); o.value=a.id; o.textContent=a.name||a.id; port.appendChild(o); });
    if (d.date_range.min) {
      document.getElementById('sf-from').value = d.date_range.min;
      document.getElementById('sf-to').value = d.date_range.max;
    }
  } catch(e) { console.error(e); }
}

async function loadSuggestions() {
  const params = new URLSearchParams(new FormData(document.getElementById('sug-filter')));
  for (const [k,v] of [...params]) { if (!v) params.delete(k); }

  document.getElementById('loading').style.display = '';
  document.getElementById('suggestions-list').innerHTML = '';

  try {
    const d = await api('/api/suggestions?' + params);
    allSuggestions = (d.suggestions || []).map(s => ({...s, checked: false}));
    renderCategoryChips();
    renderSuggestions();
    document.getElementById('action-bar').style.display = allSuggestions.length ? '' : 'none';
    document.getElementById('category-chips').style.display = allSuggestions.length ? '' : 'none';
  } catch(e) { showAlert(e.message, 'error'); }
  finally { document.getElementById('loading').style.display = 'none'; }
}

function renderCategoryChips() {
  const cats = {};
  allSuggestions.forEach(s => { cats[s.category] = (cats[s.category]||0) + 1; });
  const el = document.getElementById('category-chips');
  el.innerHTML = '';
  const allChip = document.createElement('span');
  allChip.className = 'chip' + (activeCategory===null?' active':'');
  allChip.textContent = `All (${allSuggestions.length})`;
  allChip.onclick = () => { activeCategory = null; renderCategoryChips(); renderSuggestions(); };
  el.appendChild(allChip);

  Object.entries(cats).sort((a,b)=>b[1]-a[1]).forEach(([cat, count]) => {
    const chip = document.createElement('span');
    chip.className = 'chip' + (activeCategory===cat?' active':'');
    chip.textContent = `${cat} (${count})`;
    chip.onclick = () => { activeCategory = cat; renderCategoryChips(); renderSuggestions(); };
    el.appendChild(chip);
  });
}

function getVisible() {
  return allSuggestions.filter(s => !activeCategory || s.category === activeCategory);
}

function renderSuggestions() {
  const list = document.getElementById('suggestions-list');
  list.innerHTML = '';

  const visible = getVisible();
  if (!visible.length) {
    list.innerHTML = '<div class="panel" style="text-align:center;color:#888">No suggestions found. Upload reports and click Analyze.</div>';
    return;
  }

  // Group by category
  const groups = {};
  visible.forEach(s => { (groups[s.category] = groups[s.category] || []).push(s); });

  for (const [cat, items] of Object.entries(groups)) {
    const group = document.createElement('div');
    group.className = 'sug-group';

    const header = document.createElement('div');
    header.className = 'sug-group-header';
    const sev = items[0].severity;
    header.innerHTML = `<h2>${cat}</h2><span class="badge badge-${sev}">${items.length} suggestion${items.length>1?'s':''}</span>`;
    group.appendChild(header);

    items.forEach(s => {
      const card = document.createElement('div');
      card.className = 'sug-card';
      const hasActions = s.actions && s.actions.length > 0;
      card.innerHTML = `
        ${hasActions ? `<input type="checkbox" ${s.checked?'checked':''} data-id="${s.id}">` : '<div style="width:18px"></div>'}
        <div class="sug-body">
          <div class="sug-title">${s.title}</div>
          <div class="sug-detail">${s.detail}</div>
          <div class="sug-metrics">${Object.entries(s.metrics||{}).map(([k,v]) => {
            if (typeof v === 'number') v = k.includes('acos')||k.includes('ctr')||k.includes('cvr') ? fmt.pct(v,1) : k.includes('spend')||k.includes('sales')||k.includes('budget') ? fmt.usd(v) : fmt.num(v);
            return `<span><strong>${k}:</strong> ${v}</span>`;
          }).join('')}</div>
        </div>
      `;
      if (hasActions) {
        card.querySelector('input').addEventListener('change', e => {
          s.checked = e.target.checked;
          updateSelectedCount();
        });
      }
      group.appendChild(card);
    });

    list.appendChild(group);
  }
  updateSelectedCount();
}

function updateSelectedCount() {
  const count = allSuggestions.filter(s => s.checked).length;
  document.getElementById('selected-count').textContent = count + ' selected';
  document.getElementById('generate-bulk-btn').disabled = count === 0;
}

function selectAllVisible() {
  getVisible().forEach(s => { if (s.actions && s.actions.length) s.checked = true; });
  renderSuggestions();
}

function deselectAll() {
  allSuggestions.forEach(s => s.checked = false);
  renderSuggestions();
}

async function generateBulk() {
  const selected = allSuggestions.filter(s => s.checked && s.actions && s.actions.length);
  if (!selected.length) return;

  const actions = selected.flatMap(s => s.actions);
  const accountId = actions[0]?.__account_id_note || '';

  try {
    const d = await api('/api/bulk/generate', {
      method: 'POST',
      body: JSON.stringify({ actions, account_id: accountId }),
    });
    if (d.success) {
      const summary = d.summary || {};
      showAlert(`Bulk file generated: ${summary.total_actions} actions. Downloading...`);
      // Trigger download
      const a = document.createElement('a');
      a.href = `/api/bulk/jobs/${d.job_id}/download`;
      a.download = d.filename || 'bulk.xlsx';
      a.click();
    } else {
      showAlert('Error: ' + (d.error||'Unknown'), 'error');
    }
  } catch(e) { showAlert(e.message, 'error'); }
}
