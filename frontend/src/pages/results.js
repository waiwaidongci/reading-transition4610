import {domain} from '../constants/domain.js';import {research} from '../stores/research.js';import {runsApi} from '../api/research.js';import {download} from '../api/client.js';import {runOptions,metrics} from '../components/common/business.js';import {status} from '../components/common/status.js';import {bars} from '../components/common/chart.js';import {table} from '../components/common/table.js';import {diagnostics} from './domain_result.js';import {esc,fmt} from '../utils/format.js';import {bind,toast} from '../hooks/actions.js';

const phaseLabels={intake:'受理冻结',compute:'独立计算',queued:'等待独立 worker','':'—'};
function memory(kb){return kb?fmt(Math.round(kb/1024))+' MB':'—'}
function progressPanel(run){
  const total=run.frozen_count||run.snapshot?.frozen_count||0;
  const processed=run.processed_count||0;
  const percent=Number.isFinite(run.progress)?run.progress:(run.state==='completed'?100:0);
  const rows=[
    ['冻结数量',total?fmt(total)+' 条':(run.phase==='intake'?'冻结中…':'0 条')],
    ['处理进度',percent+'% · 已处理 '+fmt(processed)+' / '+fmt(total||'—')],
    ['峰值内存',memory(run.peak_memory_kb)],
    ['当前阶段',phaseLabels[run.phase]||run.phase||(run.state==='completed'?'已完成':'—')],
  ];
  return '<dl class="progress-panel">'+rows.map(([k,v])=>'<div><dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd></div>').join('')+'</dl><div class="progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="'+percent+'"><i style="width:'+Math.min(100,percent)+'%"></i></div>';
}

export async function resultsPage(){
  const run=research.runId?await runsApi.get(research.runId):null;
  research.activeRun=run;
  const result=run?.result;
  const pending=run&&['queued','running'].includes(run.state);
  return '<section class="page-heading"><p class="eyebrow">04 / RESULT & INTERPRETATION</p><h1>'+esc(domain.result_headline)+'</h1><p>读取创建时冻结的来源、参数与样本，研究结果可以独立导出复算。</p></section><label class="compact">结果任务<select id="run-select">'+runOptions(research.runs,research.runId)+'</select></label>'+(run?'<div class="run-title"><h2>'+esc(run.name)+'</h2>'+status(run.state)+'<small>#'+run.id+' · 来源 #'+run.source_id+'</small>'+(run.state==='completed'?'<button id="export-run" class="secondary">下载结果 CSV ↗</button>':'')+'</div><p class="fine">参数：'+esc(JSON.stringify(run.parameters))+' · 冻结 '+fmt(run.frozen_count||0)+' 条输入 · '+esc(run.snapshot?.source?.filename||'')+'</p>'+progressPanel(run)+(run.state==='failed'?'<div class="failure"><h2>本次计算未完成</h2><p>'+esc(run.error||'未记录失败原因')+'</p><a href="#source">核对输入</a> · <a href="#config">修正参数后重新执行</a></div>':pending?'<div class="pending"><span class="spinner"></span><h2>研究正在分批处理</h2><p>受理先冻结已提交会话，独立 worker 再按批计算；页面自动刷新进度，离开本页不会中断。</p></div>':!result?'<p class="empty">还没有结果。</p>':metrics(result.summary)+'<div class="result-grid"><section><h2>业务结构</h2>'+bars(result.chart)+'</section><aside class="method-sheet"><p class="eyebrow">READING NOTES</p><p>'+esc(result.explanation)+'</p><small>随机种子 '+result.seed+' · 输入 SHA256<br><code>'+esc(result.checksum)+'</code></small></aside></div>'+diagnostics(result)+'<h2>完整计算明细 <small>共 '+result.rows.length+' 行，预览前30行；CSV含全部行</small></h2>'+table(result.rows,result.columns.map(c=>({key:c,label:domain.column_labels[c]||c})),{limit:30})+'<p><a class="text-link" href="#notes">据此提交业务说明 ↗</a></p>'):'<p class="empty">还没有任务，请先创建来源并执行一轮分析。</p>')}

export function bindResults(refresh){
  bind('#run-select','change',async e=>{research.runId=Number(e.target.value);await refresh()});
  bind('#export-run','click',async()=>{try{await download('/runs/'+research.runId+'/export','research-'+research.runId+'.csv')}catch(e){toast(e.message,true)}})
}
