// --- api client ---

const api = {
  async request(method, url, body) {
    const opts = { method, headers: {} };
    if (body && !(body instanceof FormData)) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    } else if (body instanceof FormData) {
      opts.body = body;
    }
    const res = await fetch(url, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  },
  getTasks()         { return this.request('GET', '/api/tasks'); },
  uploadTask(file)   { const fd = new FormData(); fd.append('file', file); return this.request('POST', '/api/tasks', fd); },
  getTask(name)      { return this.request('GET', '/api/tasks/' + encodeURIComponent(name)); },
  updateTask(name, runCmd, stopCmd, timeoutEnabled, timeoutSeconds, onTimeout, continueOnFailure, retryCount) {
    return this.request('PUT', '/api/tasks/' + encodeURIComponent(name), {
      run_command: runCmd,
      stop_command: stopCmd,
      timeout_enabled: timeoutEnabled,
      timeout_seconds: timeoutSeconds,
      on_timeout: onTimeout,
      continue_on_failure: continueOnFailure,
	      retry_count: retryCount,
    });
  },
  deleteTask(name)   { return this.request('DELETE', '/api/tasks/' + encodeURIComponent(name)); },
  getPipelines()     { return this.request('GET', '/api/pipelines'); },
  createPipeline(name, schedule, webhookUrl, loopCount) { return this.request('POST', '/api/pipelines', { name, schedule, webhook_url: webhookUrl, loop_count: loopCount }); },
  getPipeline(id)    { return this.request('GET', '/api/pipelines/' + id); },
  addTask(id, taskName) { return this.request('PUT', '/api/pipelines/' + id, { action: 'add_task', task_name: taskName }); },
  removeTask(id, taskIndex) { return this.request('PUT', '/api/pipelines/' + id, { action: 'remove_task', task_index: taskIndex }); },
  reorderTasks(id, indices) { return this.request('PUT', '/api/pipelines/' + id, { action: 'reorder', task_indices: indices }); },
  deletePipeline(id) { return this.request('DELETE', '/api/pipelines/' + id); },
  startPipeline(id)  { return this.request('POST', '/api/pipelines/' + id + '/start'); },
  stopPipeline(id)   { return this.request('POST', '/api/pipelines/' + id + '/stop'); },
  getRuns(pipelineId) { let url = '/api/runs'; if (pipelineId) url += '?pipeline_id=' + pipelineId; return this.request('GET', url); },
  getRun(id)         { return this.request('GET', '/api/runs/' + id); },
  getRunLog(runId, taskName, taskIdx) { return this.request('GET', '/api/runs/' + runId + '?log=1&task=' + encodeURIComponent(taskName) + '&task_idx=' + taskIdx); },
  getRunEvents(runId) { return this.request('GET', '/api/runs/' + runId + '/events'); },
  deleteRun(runId)   { return this.request('DELETE', '/api/runs/' + encodeURIComponent(runId)); },
  getState()         { return this.request('GET', '/api/state'); },
  retryRun(id, runId) { return this.request('POST', '/api/runs/' + encodeURIComponent(runId) + '/continue', { pipeline_id: id }); },
};

// --- state ---

let currentPipelineId = null;
let pollTimer = null;

// --- init ---

document.addEventListener('DOMContentLoaded', () => {
	  document.getElementById('task-config-sec').addEventListener('input', function() {
	    var v = parseInt(this.value, 10);
	    document.getElementById('task-config-retry').disabled = (v === 0 || isNaN(v));
	  });
  initTaskUpload();
  initTaskDetailButtons();
  initPipelineCreate();
  initRunButtons();
  refreshAll();
  startPolling();
});

function refreshAll() {
  renderTaskList();
  renderPipelineList();
  refreshCanvas();
}

// --- task list ---

async function renderTaskList() {
  const ul = document.getElementById('task-list');
  try {
    const tasks = await api.getTasks();
    // Cache task metas for pipeline task config modal
    window.taskMetas = {};
    tasks.forEach(t => { window.taskMetas[t.name] = t; });
    ul.innerHTML = '';
    tasks.forEach(t => {
      const li = document.createElement('li');
      li.textContent = t.name;
      li.dataset.taskName = t.name;
      li.addEventListener('click', () => showTaskDetail(t.name));
      ul.appendChild(li);
    });
    initTaskListSortable();
  } catch (e) {
    ul.innerHTML = '<li style="color:#d93025">加载失败</li>';
  }
}

async function showTaskDetail(name) {
  try {
    const data = await api.getTask(name);
    const meta = data.meta;
    const readme = data.readme || '(无 README)';

    const panel = document.getElementById('task-detail');
    panel.querySelector('.task-detail-name').textContent = meta.name;
    panel.querySelector('.task-detail-run-cmd').value = meta.run_command || '';
    panel.querySelector('.task-detail-stop-cmd').value = meta.stop_command || '';
    panel.querySelector('.task-detail-readme').textContent = readme;

    const timeoutEnable = panel.querySelector('.task-detail-timeout-enable');
    timeoutEnable.checked = meta.timeout_enabled || false;
    panel.querySelector('.task-detail-timeout-sec').value = meta.timeout_seconds;
    panel.querySelector('.task-detail-timeout-action').value = meta.on_timeout || 'fail';
    toggleTimeoutFields(meta.timeout_enabled || false);
    panel.querySelector('.task-detail-continue-on-failure').checked = meta.continue_on_failure || false;
	    panel.querySelector('.task-detail-retry-count').value = meta.retry_count || 0;

    panel.style.display = 'block';
    panel.dataset.taskName = name;
  } catch (e) {
    alert('加载失败: ' + e.message);
  }
}

function hideTaskDetail() {
  document.getElementById('task-detail').style.display = 'none';
}

function toggleTimeoutFields(enabled) {
  const panel = document.getElementById('task-detail');
  panel.querySelector('.task-detail-timeout-sec').disabled = !enabled;
  panel.querySelector('.task-detail-timeout-action').disabled = !enabled;
	  panel.querySelector('.task-detail-retry-count').disabled = !enabled;
}

function initTaskDetailButtons() {
  document.getElementById('task-save-btn').addEventListener('click', async () => {
    const panel = document.getElementById('task-detail');
    const name = panel.dataset.taskName;
    const runCmd = panel.querySelector('.task-detail-run-cmd').value;
    const stopCmd = panel.querySelector('.task-detail-stop-cmd').value;
    const timeoutEnabled = panel.querySelector('.task-detail-timeout-enable').checked;
    const timeoutSeconds = parseInt(panel.querySelector('.task-detail-timeout-sec').value, 10) || 0;
    const onTimeout = panel.querySelector('.task-detail-timeout-action').value;
    const continueOnFailure = panel.querySelector('.task-detail-continue-on-failure').checked;
	    const retryCount = parseInt(panel.querySelector('.task-detail-retry-count').value, 10) || 0;
    try {
      await api.updateTask(name, runCmd, stopCmd, timeoutEnabled, timeoutSeconds, onTimeout, continueOnFailure, retryCount);
      hideTaskDetail();
      renderTaskList();
    } catch (e) { alert('保存失败: ' + e.message); }
  });

  // Timeout checkbox toggle
  document.querySelector('.task-detail-timeout-enable').addEventListener('change', function() {
    toggleTimeoutFields(this.checked);
  });

  document.getElementById('task-delete-btn').addEventListener('click', async () => {
    const panel = document.getElementById('task-detail');
    const name = panel.dataset.taskName;
    if (!confirm('确定删除 task "' + name + '"？')) return;
    try {
      await api.deleteTask(name);
      hideTaskDetail();
      renderTaskList();
    } catch (e) { alert('删除失败: ' + e.message); }
  });

  document.getElementById('task-download-btn').addEventListener('click', () => {
    const panel = document.getElementById('task-detail');
    const name = panel.dataset.taskName;
    const a = document.createElement('a');
    a.href = '/api/tasks/' + encodeURIComponent(name) + '/download';
    a.download = name + '.tar';
    a.click();
  });

  document.getElementById('task-close-btn').addEventListener('click', hideTaskDetail);
}

function initTaskUpload() {
  document.getElementById('task-upload-btn').addEventListener('click', async () => {
    const input = document.getElementById('task-file');
    const file = input.files[0];
    if (!file) return;
    try {
      await api.uploadTask(file);
      input.value = '';
      renderTaskList();
    } catch (e) { alert('上传失败: ' + e.message); }
  });
}

// --- pipeline list ---

async function renderPipelineList() {
  const ul = document.getElementById('pipeline-list');
  try {
    const pipes = await api.getPipelines();
    ul.innerHTML = '';
    pipes.forEach(p => {
      const li = document.createElement('li');
      li.textContent = p.name + (p.status === 'running' ? ' ●' : '');
      li.addEventListener('click', () => selectPipeline(p.id));
      if (p.id === currentPipelineId) li.classList.add('active');
      ul.appendChild(li);
    });
  } catch (e) {
    ul.innerHTML = '<li style="color:#d93025">加载失败</li>';
  }
}

function initPipelineCreate() {
  document.getElementById('pipeline-create-btn').addEventListener('click', async () => {
    const nameInput = document.getElementById('pipeline-name');
    const name = nameInput.value.trim();
    if (!name) return;
    const schedule = document.getElementById('pipeline-schedule').value.trim() || undefined;
    const webhookUrl = document.getElementById('pipeline-webhook').value.trim() || undefined;
    const loopVal = document.getElementById('pipeline-loop').value.trim();
    const loopCount = loopVal !== '' ? parseInt(loopVal, 10) : undefined;
    try {
      await api.createPipeline(name, schedule, webhookUrl, loopCount);
      nameInput.value = '';
      document.getElementById('pipeline-schedule').value = '';
      document.getElementById('pipeline-webhook').value = '';
      document.getElementById('pipeline-loop').value = '';
      renderPipelineList();
    } catch (e) { alert('创建失败: ' + e.message); }
  });
}

// --- canvas ---

async function selectPipeline(id) {
  currentPipelineId = id;
  clearAutoRefresh();
  document.getElementById('run-detail').style.display = 'none';
  document.getElementById('log-viewer').style.display = 'none';
  renderPipelineList();
  refreshCanvas();
  renderRunHistory();
}

async function refreshCanvas() {
  if (!currentPipelineId) {
    document.getElementById('canvas-empty').style.display = 'block';
    document.getElementById('canvas-content').style.display = 'none';
    return;
  }
  document.getElementById('canvas-empty').style.display = 'none';
  document.getElementById('canvas-content').style.display = 'block';

  try {
    const [data, state] = await Promise.all([
      api.getPipeline(currentPipelineId),
      api.getState(),
    ]);
    let runningTask = null;
    let runningTaskIdx = -1;
    if (state && state.running_pipelines) {
      const rp = state.running_pipelines.find(p => p.pipeline_id === currentPipelineId);
      if (rp) {
        runningTask = rp.current_task;
        runningTaskIdx = rp.task_index;
      }
    }
    let lastRunStatus = '';
    let highlightIdx = -1;
    try {
      const runs = await api.getRuns(currentPipelineId);
      if (runs.length > 0) {
        lastRunStatus = runs[0].status;
        const instances = await api.getRun(runs[0].run_id);
        instances.forEach(inst => {
          if (inst.status !== 'success' && inst.status !== 'pending') {
            highlightIdx = inst.index;
          }
        });
      }
    } catch (e) { /* ignore */ }
    renderPipelineTasks(data.pipeline, data.tasks, runningTask, runningTaskIdx, highlightIdx, lastRunStatus);
    updateRunButtons(data.pipeline, lastRunStatus);
  } catch (e) {
    document.getElementById('pipeline-task-list').innerHTML = '<li>加载失败</li>';
  }
}

function renderPipelineTasks(pipeline, tasks, runningTask, runningTaskIdx, highlightIdx, lastRunStatus) {
  const ul = document.getElementById('pipeline-task-list');
  ul.innerHTML = '';
  // Show schedule info
  const scheduleInfo = document.getElementById('pipeline-schedule-info');
  if (pipeline.schedule) {
    scheduleInfo.innerHTML = '⏰ ' + pipeline.schedule +
      (pipeline.status !== 'running'
        ? ' <a href="#" id="schedule-edit" style="font-size:0.8rem;color:#1a73e8;text-decoration:none">[修改]</a>'
        : '');
    scheduleInfo.style.display = 'block';
  } else if (pipeline.status !== 'running') {
    scheduleInfo.innerHTML = '<a href="#" id="schedule-edit" style="font-size:0.8rem;color:#1a73e8;text-decoration:none">添加定时执行</a>';
    scheduleInfo.style.display = 'block';
  } else {
    scheduleInfo.style.display = 'none';
  }
  const editLink = document.getElementById('schedule-edit');
  if (editLink) {
    editLink.addEventListener('click', (e) => {
      e.preventDefault();
      const newSchedule = prompt('输入 cron 表达式（留空取消定时）：', pipeline.schedule || '');
      if (newSchedule === null) return;
      api.request('PUT', '/api/pipelines/' + currentPipelineId, {
        action: 'set_schedule',
        schedule: newSchedule,
      }).then(() => refreshCanvas()).catch(e => alert('修改失败: ' + e.message));
    });
  }

  // Show webhook info
  const loopInfo = document.getElementById('pipeline-loop-info');
  const webhookInfo = document.getElementById('pipeline-webhook-info');
  if (pipeline.webhook_url) {
    webhookInfo.innerHTML = '🔗 ' + pipeline.webhook_url +
      (pipeline.status !== 'running'
        ? ' <a href="#" id="webhook-edit" style="font-size:0.8rem;color:#1a73e8;text-decoration:none">[修改]</a>'
        : '');
    webhookInfo.style.display = 'block';
  } else if (pipeline.status !== 'running') {
    webhookInfo.innerHTML = '<a href="#" id="webhook-edit" style="font-size:0.8rem;color:#1a73e8;text-decoration:none">添加 Webhook</a>';
    webhookInfo.style.display = 'block';
  } else {
    webhookInfo.style.display = 'none';
  }
  const webhookEditLink = document.getElementById('webhook-edit');
  if (webhookEditLink) {
    webhookEditLink.addEventListener('click', (e) => {
      e.preventDefault();
      const newWebhook = prompt('输入 Webhook URL（留空取消通知）：', pipeline.webhook_url || '');
      if (newWebhook === null) return;
      api.request('PUT', '/api/pipelines/' + currentPipelineId, {
        action: 'set_webhook',
        webhook_url: newWebhook,
      }).then(() => refreshCanvas()).catch(e => alert('修改失败: ' + e.message));
    });
  }

  tasks.forEach((t, idx) => {
    const li = document.createElement('li');
    const span = document.createElement('span');
    span.textContent = t.name;
    li.appendChild(span);

    li.dataset.taskName = t.name;
    li.dataset.taskIndex = idx;
    if (runningTask && t.name === runningTask && idx === runningTaskIdx) li.classList.add('running');
    if (idx === highlightIdx && lastRunStatus !== 'success' && pipeline.status !== 'running') {
      li.classList.add('task-failed');
    }

    const isRunning = pipeline.status === 'running';
    if (isRunning) {
      li.classList.add('pipeline-running');
    } else {
      const rmBtn = document.createElement('button');
      rmBtn.textContent = '×';
      rmBtn.className = 'task-remove-btn';
      rmBtn.title = '移除';
      rmBtn.addEventListener('click', async (e) => {
        e.stopPropagation();
        if (confirm('将 "' + t.name + '" 从流水线中移除？')) {
          try {
            await api.removeTask(currentPipelineId, idx);
            refreshCanvas();
            renderRunHistory();
          } catch (e) { alert('移除失败: ' + e.message); }
        }
      });
      li.appendChild(rmBtn);
    }

    li.addEventListener('click', () => {
      configureTask(t, idx, pipeline.status === 'running');
    });
    ul.appendChild(li);
  });
  initPipelineSortable(pipeline.status === 'running');
}

// -- task config modal --

let currentConfigTask = null;
let currentConfigIndex = -1;

function configureTask(task, idx, readOnly) {
  currentConfigTask = task;
  currentConfigIndex = idx;
  const meta = window.taskMetas ? window.taskMetas[task.name] : null;

  // Resolve effective values: pipeline override > task default > platform default
  const effSec = (task.timeout_seconds !== null && task.timeout_seconds !== undefined)
    ? task.timeout_seconds
    : (meta ? (meta.timeout_enabled ? meta.timeout_seconds : 0) : null);
  const effAction = task.on_timeout || (meta ? meta.on_timeout : null) || 'fail';
  const effContinue = task.continue_on_failure !== null && task.continue_on_failure !== undefined
    ? task.continue_on_failure
    : (meta ? meta.continue_on_failure : false);

	  const effRetry = task.retry_count !== null && task.retry_count !== undefined
	    ? task.retry_count
	    : (meta ? (meta.retry_count || 0) : 0);
  document.getElementById('task-config-title').textContent = task.name;
  document.getElementById('task-config-sec').value = effSec !== null ? String(effSec) : '30';
  document.getElementById('task-config-action').value = effAction;
  document.getElementById('task-config-continue').value = effContinue ? 'true' : 'false';
	  document.getElementById('task-config-retry').value = effRetry;

	  const inputs = document.querySelectorAll('#task-config-modal input, #task-config-modal select');
	  inputs.forEach(el => { el.disabled = false; });
	  document.getElementById('task-config-confirm').disabled = false;
	  document.getElementById('task-config-reset').disabled = false;
	  document.getElementById('task-config-retry').disabled = (effSec === 0);
	  if (readOnly) {
	    inputs.forEach(el => { el.disabled = true; });
	    document.getElementById('task-config-confirm').disabled = true;
	    document.getElementById('task-config-reset').disabled = true;
	  }
	  document.getElementById('task-config-modal').classList.add('open');
}

function closeConfigModal() {
  document.getElementById('task-config-modal').classList.remove('open');
  currentConfigTask = null;
  currentConfigIndex = -1;
}

function resetConfig() {
  const task = currentConfigTask;
  const idx = currentConfigIndex;
  if (!task) return;
  if (!confirm('重置 "' + task.name + '" 的所有覆盖设置？')) return;
  closeConfigModal();
  api.request('PUT', '/api/pipelines/' + currentPipelineId, {
    action: 'set_task_config',
    task_index: idx,
    timeout_seconds: null,
    on_timeout: null,
    continue_on_failure: null,
	    retry_count: null,
  }).then(() => refreshCanvas()).catch(e => alert('重置失败: ' + e.message));
}

function confirmConfig() {
  const task = currentConfigTask;
  const idx = currentConfigIndex;
  if (!task) return;

  const secVal = document.getElementById('task-config-sec').value.trim();
  const actionVal = document.getElementById('task-config-action').value;
  const continueVal = document.getElementById('task-config-continue').value;

  if (secVal !== '') {
    const sec = parseInt(secVal, 10);
    if (isNaN(sec) || sec < 0) {
      alert('请输入有效的秒数');
      return;
    }
  }

  closeConfigModal();
  api.request('PUT', '/api/pipelines/' + currentPipelineId, {
    action: 'set_task_config',
    task_index: idx,
    timeout_seconds: secVal !== '' ? parseInt(secVal, 10) : null,
    on_timeout: actionVal || null,
    continue_on_failure: continueVal === 'true' ? true : (continueVal === 'false' ? false : null),
	    retry_count: parseRetryCount(),
  }).then(() => refreshCanvas()).catch(e => alert('设置失败: ' + e.message));
}

// Wire modal buttons
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('task-config-cancel').addEventListener('click', closeConfigModal);
  document.getElementById('task-config-confirm').addEventListener('click', confirmConfig);
  document.getElementById('task-config-reset').addEventListener('click', resetConfig);
  // Enter key in number field triggers confirm
  document.getElementById('task-config-sec').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') confirmConfig();
  });
});

function updateRunButtons(pipeline, lastRunStatus) {
  const runBtn = document.getElementById('pipeline-run-btn');
  const stopBtn = document.getElementById('pipeline-stop-btn');
  const retryBtn = document.getElementById('pipeline-retry-btn');
  const running = pipeline.status === 'running';
  runBtn.disabled = running;
  stopBtn.disabled = !running;
  const retryable = ['failed', 'timeout', 'stopped', 'crashed'];
  retryBtn.style.display = (!running && retryable.includes(lastRunStatus)) ? '' : 'none';
}

// --- sortable ---

let sortableInstance = null;

function initTaskListSortable() {
  const taskList = document.getElementById('task-list');
  Sortable.create(taskList, {
    group: { name: 'tasks', pull: 'clone', put: false },
    sort: false,
  });
}

function initPipelineSortable(running) {
  if (sortableInstance) sortableInstance.destroy();
  const pipelineList = document.getElementById('pipeline-task-list');
  sortableInstance = Sortable.create(pipelineList, {
    group: { name: 'tasks', pull: running ? false : true, put: running ? false : true },
    sort: !running,
    animation: 150,
    onAdd: async function(evt) {
      if (running) return;
      const taskName = evt.item.dataset.taskName || evt.item.textContent;
      try {
        await api.addTask(currentPipelineId, taskName);
        refreshCanvas();
        renderTaskList();
      } catch (e) {
        alert('添加失败: ' + e.message);
        refreshCanvas();
        renderTaskList();
      }
    },
    onUpdate: async function(evt) {
      if (running) return;
      const items = [...evt.from.querySelectorAll('li')].map(li => parseInt(li.dataset.taskIndex, 10));
      try {
        await api.reorderTasks(currentPipelineId, items);
      } catch (e) { alert('排序失败: ' + e.message); }
    },
  });
}

// --- run buttons ---

function initRunButtons() {
  document.getElementById('pipeline-run-btn').addEventListener('click', async () => {
    if (!currentPipelineId) return;
    try {
      await api.startPipeline(currentPipelineId);
      refreshAll();
    } catch (e) { alert('启动失败: ' + e.message); }
  });

  document.getElementById('pipeline-stop-btn').addEventListener('click', async () => {
    if (!currentPipelineId) return;
    try {
      await api.stopPipeline(currentPipelineId);
      refreshAll();
    } catch (e) { alert('停止失败: ' + e.message); }
  });

  document.getElementById('pipeline-delete-btn').addEventListener('click', async () => {
    if (!currentPipelineId) return;
    if (!confirm('确定删除此流水线及其所有运行数据？')) return;
    try {
      await api.deletePipeline(currentPipelineId);
      currentPipelineId = null;
      renderPipelineList();
      refreshCanvas();
    } catch (e) { alert('删除失败: ' + e.message); }
  });
  document.getElementById('pipeline-retry-btn').addEventListener('click', async () => {
    if (!currentPipelineId) return;
    try {
      const runs = await api.getRuns(currentPipelineId);
      if (runs.length === 0) { alert('没有可续跑的运行记录'); return; }
      const lastRun = runs[0];
      const retryable = ['failed', 'timeout', 'stopped', 'crashed'];
      if (!retryable.includes(lastRun.status)) {
        alert('上次运行已成功，无需续跑');
        return;
      }
      await api.retryRun(currentPipelineId, lastRun.run_id);
      refreshAll();
    } catch (e) { alert('续跑失败: ' + e.message); }
  });
}

// --- run history ---

async function renderRunHistory() {
  const table = document.getElementById('run-history-table');
  if (!currentPipelineId) { table.innerHTML = ''; return; }
  try {
    const runs = await api.getRuns(currentPipelineId);
    if (runs.length === 0) {
      table.innerHTML = '<tr><td style="color:#999">暂无运行记录</td></tr>';
      return;
    }
    // Fetch current running state for this pipeline
    let currentRunId = null;
    try {
      const state = await api.getState();
      if (state && state.running_pipelines) {
        const rp = state.running_pipelines.find(p => p.pipeline_id === currentPipelineId);
        if (rp) currentRunId = rp.current_run_id;
      }
    } catch (e) { /* ignore */ }

    table.innerHTML = '<tr><th>Run</th><th>状态</th><th>Size</th><th>操作</th></tr>';
    runs.forEach(r => {
      const tr = document.createElement('tr');
      if (r.run_id === currentRunId) tr.classList.add('run-active');
      const statusMap = {
        'running': '运行中',
        'success': '成功',
        'failed': '失败',
        'unknown': '未知',
      };
      const statusText = statusMap[r.status] || r.status;
      const statusColor = r.status === 'running' ? '#1a73e8' :
                          r.status === 'success' ? '#0d904f' :
                          r.status === 'failed' ? '#d93025' : '#888';
      const sizeStr = r.size > 1024*1024 ? (r.size/1024/1024).toFixed(1) + 'MB' :
                       r.size > 1024 ? (r.size/1024).toFixed(1) + 'KB' : r.size + 'B';
      tr.innerHTML = '<td>' + r.run_id + '</td>' +
        '<td style="color:' + statusColor + ';font-weight:600">' + statusText + '</td>' +
        '<td>' + sizeStr + '</td>' +
        '<td><button data-run="' + r.run_id + '" class="view-run-btn">查看</button> ' +
        '<button data-run="' + r.run_id + '" class="delete-run-btn" style="color:#d93025;margin-left:6px">删除</button></td>';
      table.appendChild(tr);
    })
    table.querySelectorAll('.view-run-btn').forEach(btn => {
      btn.addEventListener('click', () => showRunDetail(btn.dataset.run));
    });
    table.querySelectorAll('.delete-run-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const runId = btn.dataset.run;
        if (!confirm('确定删除 ' + runId + ' 吗？此操作不可撤销。')) return;
        try {
          await api.deleteRun(runId);
          renderRunHistory();
          refreshCanvas();
        } catch (e) { alert('删除失败: ' + e.message); }
      });
    });
  } catch (e) {
    table.innerHTML = '<tr><td>加载失败</td></tr>';
  }
}

async function showRunDetail(runId) {
  try {
    const instances = await api.getRun(runId);
    let html = '<h3>Run: ' + runId + '</h3><ul style="list-style:none;margin:8px 0">';
    instances.forEach(inst => {
      const color = inst.status === 'success' ? 'green' :
                    inst.status === 'failed' || inst.status === 'crashed' || inst.status === 'timeout' ? 'red' :
                    inst.status === 'running' ? 'blue' : '#888';
      html += '<li style="margin:4px 0;color:' + color + '">' +
        inst.task_name + ' — ' + inst.status +
        (inst.exit_code !== 0 && inst.exit_code !== -1 ? ' (exit ' + inst.exit_code + ')' : '') +
        ' <button data-run="' + runId + '" data-task="' + inst.task_name + '" data-task-idx="' + inst.index + '" class="view-log-btn">日志</button>' +
        '</li>';
    });
    html += '</ul><button id="show-events-btn" data-run="' + runId + '">事件日志</button> ' +
      '<button id="close-run-detail">关闭</button>';

    const div = document.getElementById('run-detail');
    div.innerHTML = html;
    div.style.display = 'block';
    div.querySelector('#close-run-detail').addEventListener('click', () => { div.style.display = 'none'; });
    div.querySelectorAll('.view-log-btn').forEach(btn => {
      btn.addEventListener('click', () => showLog(btn.dataset.run, btn.dataset.task, parseInt(btn.dataset.taskIdx, 10)));
    });
    const eventsBtn = div.querySelector('#show-events-btn');
    if (eventsBtn) {
      eventsBtn.addEventListener('click', () => showEventsLog(runId));
    }

  } catch (e) { alert('加载失败: ' + e.message); }
}

async function showLog(runId, taskName, taskIdx) {
  clearAutoRefresh();
  const div = document.getElementById('log-viewer');
  div.innerHTML = '<h3>' + taskName + ' — stdout/stderr' +
    ' <label style="font-weight:normal;font-size:14px;margin-left:12px">' +
    '<input type="checkbox" id="auto-refresh-toggle"> 自动刷新（每30秒）</label>' +
    ' <button id="refresh-log">刷新</button>' +
    ' <button id="close-log">关闭</button></h3>' +
    '<pre id="log-content" style="background:#1a1a2e;color:#e0e0e0;padding:12px;border-radius:4px;max-height:400px;overflow:auto;">加载中...</pre>';
  div.style.display = 'block';
  div.querySelector('#close-log').addEventListener('click', () => { clearAutoRefresh(); div.style.display = 'none'; });
  div.querySelector('#refresh-log').addEventListener('click', () => fetchLogContent(runId, taskName, taskIdx));
  div.querySelector('#auto-refresh-toggle').addEventListener('change', function() {
    if (this.checked) {
      logAutoRefresh = setInterval(() => fetchLogContent(runId, taskName, taskIdx), 30000);
    } else {
      clearAutoRefresh();
    }
  });
  fetchLogContent(runId, taskName, taskIdx);
}

async function fetchLogContent(runId, taskName, taskIdx) {
  try {
    const data = await api.getRunLog(runId, taskName, taskIdx);
    const pre = document.getElementById('log-content');
    if (pre) {
      pre.innerHTML = '<strong>stdout:</strong>\n' + escHtml(data.stdout || '(empty)') + '\n\n<strong>stderr:</strong>\n' + escHtml(data.stderr || '(empty)');
      pre.scrollTop = pre.scrollHeight;
    }
  } catch (e) {
    const pre = document.getElementById('log-content');
    if (pre) pre.textContent = '加载失败: ' + e.message;
    const toggle = document.getElementById('auto-refresh-toggle');
    if (toggle) toggle.checked = false;
    clearAutoRefresh();
  }
}

async function showEventsLog(runId) {
  clearAutoRefresh();
  const div = document.getElementById('log-viewer');
  div.innerHTML = '<h3>Run: ' + runId + ' — 事件日志' +
    ' <label style="font-weight:normal;font-size:14px;margin-left:12px">' +
    '<input type="checkbox" id="auto-refresh-toggle"> 自动刷新（每60秒）</label>' +
    ' <button id="refresh-log">刷新</button>' +
    ' <button id="close-log">关闭</button></h3>' +
    '<pre id="log-content" style="background:#1a1a2e;color:#e0e0e0;padding:12px;border-radius:4px;max-height:400px;overflow:auto;">加载中...</pre>';
  div.style.display = 'block';
  div.querySelector('#close-log').addEventListener('click', () => { clearAutoRefresh(); div.style.display = 'none'; });
  div.querySelector('#refresh-log').addEventListener('click', () => fetchEventsContent(runId));
  div.querySelector('#auto-refresh-toggle').addEventListener('change', function() {
    if (this.checked) {
      logAutoRefresh = setInterval(() => fetchEventsContent(runId), 60000);
    } else {
      clearAutoRefresh();
    }
  });
  fetchEventsContent(runId);
}

async function fetchEventsContent(runId) {
  try {
    const data = await api.getRunEvents(runId);
    const pre = document.getElementById('log-content');
    if (pre) {
      pre.textContent = data.events;
      pre.scrollTop = pre.scrollHeight;
    }
  } catch (e) {
    const pre = document.getElementById('log-content');
    if (pre) pre.textContent = '加载失败: ' + e.message;
    const toggle = document.getElementById('auto-refresh-toggle');
    if (toggle) toggle.checked = false;
    clearAutoRefresh();
  }
}

let logAutoRefresh = null;

function clearAutoRefresh() {
  if (logAutoRefresh) { clearInterval(logAutoRefresh); logAutoRefresh = null; }
}

function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// --- polling ---

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try {
      const state = await api.getState();
      const statusEl = document.getElementById('global-status');
      if (state && state.running_pipelines && state.running_pipelines.length > 0) {
        const count = state.running_pipelines.length;
        const first = state.running_pipelines[0];
        statusEl.textContent = count > 1 ? (count + ' 条流水线运行中') : ('运行中: ' + first.pipeline_id + ' / ' + first.current_task);
        statusEl.style.color = '#4caf50';
      } else {
        statusEl.textContent = '空闲';
        statusEl.style.color = '#888';
      }
    } catch(e) { /* ignore poll errors */ }

    renderPipelineList();
    if (currentPipelineId) {
      refreshCanvas();
      renderRunHistory();
    }
  }, 3000);
}

function parseRetryCount() {
  const v = document.getElementById('task-config-retry').value.trim();
  if (v === '') return null;
  const n = parseInt(v, 10);
  return isNaN(n) || n < 0 ? null : n;
}
