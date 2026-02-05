document.addEventListener('DOMContentLoaded', loadJobs);

async function loadJobs() {
  try {
    const d = await api('/api/bulk/jobs');
    const tb = document.querySelector('#jobs-table tbody');
    tb.innerHTML = '';
    (d.jobs||[]).forEach(j => {
      const tr = document.createElement('tr');
      const acts = j.summary_json?.total_actions || 0;
      tr.innerHTML = `
        <td>${j.created_at ? new Date(j.created_at).toLocaleString() : ''}</td>
        <td>${j.account_id || 'All'}</td>
        <td>${j.status}</td>
        <td>${acts}</td>
        <td>${j.output_file_path ? `<a href="/api/bulk/jobs/${j.id}/download" class="btn btn-sm btn-primary">Download</a>` : '-'}</td>
      `;
      tb.appendChild(tr);
    });
    if (!(d.jobs||[]).length) {
      tb.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#888">No bulk jobs yet</td></tr>';
    }
  } catch(e) { showAlert(e.message, 'error'); }
}
