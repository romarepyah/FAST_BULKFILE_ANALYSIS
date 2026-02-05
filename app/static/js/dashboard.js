let chart = null, metrics = [];

document.addEventListener('DOMContentLoaded', async () => {
  await loadFilters();
  setDefaultDates();
  await refresh();
  document.getElementById('filter-form').addEventListener('submit', e => { e.preventDefault(); refresh(); });
  document.querySelectorAll('.toggles input').forEach(cb => cb.addEventListener('change', renderChart));
  document.getElementById('export-csv').addEventListener('click', exportCSV);
});

function setDefaultDates() {
  const today = new Date(), ago = new Date(today);
  ago.setDate(today.getDate() - 30);
  document.getElementById('f-to').value = today.toISOString().slice(0,10);
  document.getElementById('f-from').value = ago.toISOString().slice(0,10);
}

async function loadFilters() {
  try {
    const d = await api('/api/metrics/filters');
    const f = d.filters;
    populateSelect('f-account', (f.accounts||[]).map(a => ({v:a.id,t:a.name||a.id})));
    populateSelect('f-portfolio', (f.portfolios||[]).map(a => ({v:a.id,t:a.name||a.id})));
    populateSelect('f-campaign', (f.campaigns||[]).map(a => ({v:a.id,t:a.name||a.id})));
    populateSelect('f-adproduct', (f.ad_products||[]).map(a => ({v:a,t:a})));
    populateSelect('f-matchtype', (f.match_types||[]).map(a => ({v:a,t:a})));
    if (d.date_range.min) {
      document.getElementById('f-from').value = d.date_range.min;
      document.getElementById('f-to').value = d.date_range.max;
    }
  } catch(e) { console.error(e); }
}

function populateSelect(id, items) {
  const sel = document.getElementById(id);
  items.forEach(({v,t}) => { const o = document.createElement('option'); o.value=v; o.textContent=t; sel.appendChild(o); });
}

async function refresh() {
  const params = new URLSearchParams(new FormData(document.getElementById('filter-form')));
  // remove empty params
  for (const [k,v] of [...params]) { if (!v) params.delete(k); }
  try {
    const [md, sd] = await Promise.all([
      api('/api/metrics/daily?' + params),
      api('/api/metrics/summary?' + params),
    ]);
    metrics = md.metrics || [];
    updateCards(sd.summary || {});
    renderChart();
    renderTable();
  } catch(e) { showAlert(e.message, 'error'); }
}

function updateCards(s) {
  document.getElementById('c-spend').textContent = fmt.usd(s.spend);
  document.getElementById('c-sales').textContent = fmt.usd(s.sales);
  document.getElementById('c-acos').textContent = fmt.pct(s.acos);
  document.getElementById('c-roas').textContent = fmt.num(s.roas, 2);
  document.getElementById('c-orders').textContent = fmt.num(s.orders);
  document.getElementById('c-ctr').textContent = fmt.pct(s.ctr);
}

const COLORS = {spend:'#e63946',sales:'#2ec4b6',acos:'#f4a261',orders:'#4361ee',impressions:'#7209b7',clicks:'#f72585',ctr:'#4cc9f0',cpc:'#560bad',roas:'#06d6a0'};

function renderChart() {
  const ctx = document.getElementById('chart');
  if (chart) chart.destroy();
  const labels = metrics.map(m => fmt.date(m.date));
  const datasets = [];
  document.querySelectorAll('.toggles input:checked').forEach(cb => {
    const key = cb.dataset.metric;
    const pctAxis = ['acos','ctr','cpc','roas'].includes(key);
    datasets.push({
      label: key.toUpperCase(), data: metrics.map(m => m[key]||0),
      borderColor: COLORS[key], backgroundColor: COLORS[key]+'22',
      yAxisID: pctAxis ? 'y1' : 'y', tension: .3, pointRadius: 2,
    });
  });
  chart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'top' } },
      scales: {
        y: { position: 'left', beginAtZero: true },
        y1: { position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } },
      },
    },
  });
}

function renderTable() {
  const tb = document.querySelector('#data-table tbody');
  tb.innerHTML = '';
  metrics.forEach(m => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${fmt.date(m.date)}</td><td>${fmt.num(m.impressions)}</td><td>${fmt.num(m.clicks)}</td><td>${fmt.pct(m.ctr)}</td><td>${fmt.usd(m.spend)}</td><td>${fmt.usd(m.sales)}</td><td>${fmt.num(m.orders)}</td><td>${fmt.pct(m.acos)}</td><td>${fmt.usd(m.cpc)}</td><td>${fmt.num(m.roas,2)}</td>`;
    tb.appendChild(tr);
  });
}

function exportCSV() {
  if (!metrics.length) return;
  let csv = 'Date,Impressions,Clicks,CTR,Spend,Sales,Orders,ACOS,CPC,ROAS\n';
  metrics.forEach(m => { csv += [m.date,m.impressions,m.clicks,m.ctr,m.spend,m.sales,m.orders,m.acos,m.cpc,m.roas].join(',') + '\n'; });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv], {type:'text/csv'}));
  a.download = 'metrics.csv'; a.click();
}
