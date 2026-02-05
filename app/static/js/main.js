// Shared utilities
const fmt = {
  usd: v => '$' + Number(v||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}),
  num: (v,d=0) => Number(v||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}),
  pct: (v,d=2) => Number(v||0).toFixed(d) + '%',
  date: v => v ? new Date(v+'T00:00:00').toLocaleDateString('en-US',{month:'short',day:'numeric'}) : '',
};

async function api(url, opts={}) {
  const r = await fetch(url, {headers:{'Content-Type':'application/json'}, ...opts});
  const d = await r.json();
  if (!r.ok) throw new Error(d.error || 'API error');
  return d;
}

function showAlert(msg, type='success') {
  const el = document.createElement('div');
  el.className = 'alert alert-' + type;
  el.textContent = msg;
  document.querySelector('.container').prepend(el);
  setTimeout(() => el.remove(), 6000);
}
