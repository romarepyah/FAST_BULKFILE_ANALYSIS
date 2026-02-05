let selectedFiles = [];

document.addEventListener('DOMContentLoaded', () => {
  const zone = document.getElementById('drop-zone');
  const input = document.getElementById('file-input');
  const btn = document.getElementById('upload-btn');

  zone.addEventListener('click', () => input.click());
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('over'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('over');
    addFiles(e.dataTransfer.files);
  });
  input.addEventListener('change', () => addFiles(input.files));
  btn.addEventListener('click', upload);
});

function addFiles(fileList) {
  for (const f of fileList) {
    if (!selectedFiles.find(x => x.name === f.name)) selectedFiles.push(f);
  }
  renderFileList();
}

function renderFileList() {
  const el = document.getElementById('file-list');
  el.innerHTML = '';
  selectedFiles.forEach((f, i) => {
    const div = document.createElement('div');
    div.className = 'file-item';
    div.innerHTML = `<span>${f.name} (${(f.size/1024).toFixed(0)} KB)</span><button class="btn btn-sm btn-danger" onclick="removeFile(${i})">Remove</button>`;
    el.appendChild(div);
  });
  document.getElementById('upload-btn').disabled = selectedFiles.length === 0;
}

function removeFile(i) { selectedFiles.splice(i, 1); renderFileList(); }

async function upload() {
  if (!selectedFiles.length) return;
  const btn = document.getElementById('upload-btn');
  btn.disabled = true;
  document.getElementById('progress-panel').style.display = '';
  document.getElementById('result-panel').style.display = 'none';
  document.getElementById('progress-fill').style.width = '30%';

  const fd = new FormData();
  selectedFiles.forEach(f => fd.append('files[]', f));

  try {
    document.getElementById('progress-fill').style.width = '60%';
    const r = await fetch('/api/upload', { method: 'POST', body: fd });
    const d = await r.json();
    document.getElementById('progress-fill').style.width = '100%';

    const rp = document.getElementById('result-panel');
    rp.style.display = '';
    const s = d.summary || {};
    let html = `<div class="alert alert-success">
      <strong>${s.files || 0} file(s)</strong> processed.
      ${s.rows_parsed || 0} rows parsed, ${s.rows_inserted || 0} rows upserted.
    </div>`;
    if (s.errors && s.errors.length) {
      html += `<div class="alert alert-error">Errors: ${s.errors.join('; ')}</div>`;
    }
    document.getElementById('result-content').innerHTML = html;
    selectedFiles = [];
    renderFileList();
  } catch(e) {
    showAlert('Upload failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    setTimeout(() => { document.getElementById('progress-panel').style.display = 'none'; }, 2000);
  }
}
