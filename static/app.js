/**
 * app.js — Dashboard frontend logic.
 */

// ── State ────────────────────────────────────────────────────────────────────
let currentPage = 1;
let currentSort = "global_score";
let currentDir = "desc";
let sort_dir = "desc";
let searchQuery = "";
let searchTimeout = null;
let scoreChart = null;

// ── Formatting Helpers ───────────────────────────────────────────────────────
function fmt(n) {
  if (n == null) return "—";
  return n.toLocaleString();
}

function fmtScore(score) {
  if (score == null) return '<span class="score na">—</span>';
  const val = score.toFixed(3);
  if (score >= 0.85) return `<span class="score high">${val}</span>`;
  if (score >= 0.65) return `<span class="score medium">${val}</span>`;
  return `<span class="score low">${val}</span>`;
}

function fmtAddr(addr) {
  if (!addr) return "—";
  return addr.slice(0, 6) + "..." + addr.slice(-4);
}

function fmtDate(isoStr) {
  if (!isoStr) return '<span style="color:var(--text-muted);font-size:11px">Awaiting enrichment...</span>';
  const d = new Date(isoStr);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function fmtTime(isoStr) {
  if (!isoStr) return "";
  const d = new Date(isoStr);
  return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
}

function fmtUSD(n) {
  if (n == null) return "—";
  if (n >= 1_000_000) return "$" + (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return "$" + (n / 1_000).toFixed(1) + "K";
  return "$" + n.toFixed(0);
}

function sourceTag(insider, anomaly) {
  const rulesFlagged = insider != null && insider >= 0.65;
  const mlFlagged = anomaly != null && anomaly >= 0.85;
  if (rulesFlagged && mlFlagged) return '<span class="tag both">BOTH</span>';
  if (rulesFlagged) return '<span class="tag rules">RULES</span>';
  if (mlFlagged) return '<span class="tag ml">ML</span>';
  return "";
}

function barColor(score) {
  if (score >= 0.8) return "var(--red)";
  if (score >= 0.5) return "var(--amber)";
  return "var(--green)";
}

function getScoreClass(score) {
  if (score == null) return "na";
  if (score >= 0.8) return "high";
  if (score >= 0.5) return "medium";
  return "low";
}

function toggleAccordion(header) {
  header.classList.toggle("active");
  header.nextElementSibling.classList.toggle("active");
}

let currentPopoverSource = null;

function toggleInfoPopover(btn, event) {
  event.stopPropagation();
  
  let globalPopover = document.getElementById('global-info-popover');
  if (!globalPopover) {
    globalPopover = document.createElement('div');
    globalPopover.id = 'global-info-popover';
    globalPopover.className = 'info-popover';
    document.body.appendChild(globalPopover);
  }

  const isSame = currentPopoverSource === btn;
  const wasActive = globalPopover.classList.contains('active');

  // Close all
  globalPopover.classList.remove('active');
  currentPopoverSource = null;

  if (!wasActive || !isSame) {
    const source = btn.querySelector('.info-popover-content') || btn.querySelector('.info-popover');
    if (!source) return;
    
    globalPopover.innerHTML = source.innerHTML;
    globalPopover.classList.add('active');
    currentPopoverSource = btn;

    const rect = btn.getBoundingClientRect();
    const popoverW = 280;
    
    globalPopover.style.position = 'fixed';
    globalPopover.style.zIndex = '999999';
    globalPopover.style.right = 'auto';
    globalPopover.style.margin = '0';
    
    let left = rect.left + (rect.width / 2) - (popoverW / 2);
    if (left < 10) left = 10;
    if (left + popoverW > window.innerWidth - 10) left = window.innerWidth - popoverW - 10;
    
    globalPopover.style.left = left + 'px';
    globalPopover.style.top = 'auto';
    globalPopover.style.bottom = (window.innerHeight - rect.top + 8) + 'px';
    globalPopover.style.transform = 'none';
  }
}

// Close popovers on click outside or scroll
const closeAllPopovers = () => {
  const gp = document.getElementById('global-info-popover');
  if (gp) {
    gp.classList.remove('active');
    currentPopoverSource = null;
  }
  document.querySelectorAll('.info-popover.active').forEach(p => {
    p.classList.remove('active');
  });
};

document.addEventListener('click', closeAllPopovers);
document.addEventListener('scroll', closeAllPopovers, { capture: true, passive: true });

// ── Stats ────────────────────────────────────────────────────────────────────
async function loadStats() {
  try {
    const res = await fetch("/api/stats");
    const data = await res.json();
    document.getElementById("statWallets").textContent = fmt(data.total_wallets);
    document.getElementById("statTrades").textContent = fmt(data.total_trades);
    document.getElementById("statFlaggedWallets").textContent = fmt(data.flagged_wallets);
    document.getElementById("statHistCount").textContent = fmt(data.historical_trades);
    document.getElementById("statLiveCount").textContent = fmt(data.live_trades);
  } catch (e) {
    console.error("Stats fetch failed:", e);
  }
}

// ── Flagged Wallets Table ────────────────────────────────────────────────────
async function loadWallets() {
  const tbody = document.getElementById("walletTableBody");
  tbody.innerHTML = '<tr><td colspan="5" class="loading"><div class="spinner"></div> Loading...</td></tr>';
  try {
    const params = new URLSearchParams({
      page: currentPage,
      per_page: 25,
      sort_by: currentSort,
      sort_dir: currentDir
    });
    const res = await fetch(`/api/flagged?${params}`);
    const data = await res.json();

    if (!data.wallets || data.wallets.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" class="loading">No flagged wallets found.</td></tr>';
      document.getElementById("pageInfo").textContent = "0 results";
      document.getElementById("prevBtn").disabled = true;
      document.getElementById("nextBtn").disabled = true;
      return;
    }

    const si = (f) => currentSort === f ? (currentDir === 'asc' ? '↑' : '↓') : '';
    document.querySelector("#walletTable thead").innerHTML = `
      <tr>
        <th onclick="changeSort('address')">Wallet ${si('address')}</th>
        <th onclick="changeSort('global_score')">Global Score ${si('global_score')}</th>
        <th onclick="changeSort('insider_score')">Rule Score ${si('insider_score')}</th>
        <th onclick="changeSort('anomaly_score')">ML Score ${si('anomaly_score')}</th>
        <th>Primary Signal</th>
      </tr>`;

    tbody.innerHTML = data.wallets.map(w => `
      <tr onclick="openWallet('${w.address}')">
        <td><span class="addr">${fmtAddr(w.address)}</span></td>
        <td>${((w.global_score || 0) * 100).toFixed(1)}%</td>
        <td>${fmtScore(w.insider_score)}</td>
        <td>${fmtScore(w.anomaly_score)}</td>
        <td>${sourceTag(w.insider_score, w.anomaly_score)}</td>
      </tr>`).join("");

    const start = (data.page - 1) * data.per_page + 1;
    const end = Math.min(data.page * data.per_page, data.total);
    document.getElementById("pageInfo").textContent = `${start}–${end} of ${data.total}`;
    document.getElementById("prevBtn").disabled = data.page <= 1;
    document.getElementById("nextBtn").disabled = data.page >= data.pages;
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="5" class="loading">Failed to load data.</td></tr>';
    console.error("Wallets fetch failed:", e);
  }
}

function changePage(delta) { currentPage = Math.max(1, currentPage + delta); loadWallets(); }
function changeSort(field) {
  if (currentSort === field) { currentDir = currentDir === "asc" ? "desc" : "asc"; }
  else { currentSort = field; currentDir = currentDir === "asc" ? "desc" : "asc"; }
  currentPage = 1;
  loadWallets();
}

// ── Search (removed from UI) ─────────────────────────────────────────────────
// Search input removed — functionality was not working correctly.

// ── Info icon SVG helper ─────────────────────────────────────────────────────
const INFO_SVG = `<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>`;
const CHEVRON_SVG = `<svg class="chevron" fill="none" stroke="currentColor" stroke-width="3" viewBox="0 0 24 24"><path d="M19 9l-7 7-7-7"/></svg>`;

// Factor calculation logic descriptions
const FACTOR_LOGIC = {
  entry_timing: "<strong>Entry Timing</strong><br>Score = 1.0 if traded &lt; 2h before market resolution.<br>Score = 0.8 if &lt; 12h.<br>Score = 0.5 if &lt; 48h.<br>Score = 0.05 otherwise.",
  trade_concentration: "<strong>Concentration</strong><br>Score = 1.0 if &gt; 95% of volume in one outcome.<br>Score = 0.8 if &gt; 80%.<br>Score = 0.4 if &gt; 50%.<br>Score = 0.05 otherwise.",
  trade_size: "<strong>Trade Size</strong><br>Score = 1.0 if max single trade &gt; $20,000.<br>Score = 0.7 if &gt; $5,000.<br>Score = 0.3 if &gt; $1,000.<br>Score = 0.05 otherwise.",
  wallet_age: "<strong>Wallet Age</strong><br>Score = 1.0 if wallet created &lt; 24h before first trade.<br>Score = 0.75 if &lt; 7 days.<br>Score = 0.35 if &lt; 30 days.<br>Score = 0.05 otherwise.",
  market_count: "<strong>Market Count</strong><br>Score = 1.0 if wallet trades only 1 market.<br>Score = 0.7 if &le; 3 markets.<br>Score = 0.3 if &le; 10 markets.<br>Score = 0.05 otherwise.",
};

const FACTOR_META = [
  { key: "entry_timing", label: "Entry Timing", weight: "30%" },
  { key: "trade_concentration", label: "Concentration", weight: "25%" },
  { key: "trade_size", label: "Trade Size", weight: "20%" },
  { key: "wallet_age", label: "Wallet Age", weight: "15%" },
  { key: "market_count", label: "Market Count", weight: "10%" },
];

// ── Wallet Detail Modal ──────────────────────────────────────────────────────
async function openWallet(address) {
  const overlay = document.getElementById("modalOverlay");
  const body = document.getElementById("modalBody");
  overlay.classList.add("active");
  body.innerHTML = '<div class="loading"><div class="spinner"></div> Loading wallet...</div>';

  try {
    const res = await fetch(`/api/wallets/${address}`);
    if (!res.ok) throw new Error("Not found");
    const w = await res.json();
    const bd = w.breakdown || {};
    const gs = w.global_score || 0;
    const rs = w.insider_score || 0;
    const ms = w.anomaly_score || 0;

    // Build breakdown items for Rules accordion
    const breakdownHTML = FACTOR_META.map(f => {
      const val = bd[f.key] ?? 0;
      return `
        <div class="breakdown-item">
          <div class="breakdown-label">
            <div class="breakdown-label-left">
              <span>${f.label}</span>
              <span class="weight-tag">${f.weight}</span>
            </div>
            <div class="info-btn" onclick="toggleInfoPopover(this, event)">
              ${INFO_SVG}
              <div class="info-popover">${FACTOR_LOGIC[f.key]}</div>
            </div>
          </div>
          <div class="breakdown-value" style="color: ${barColor(val)}">${(val * 100).toFixed(0)}%</div>
          <div class="breakdown-bar"><div class="breakdown-fill" style="width:${val * 100}%;background:${barColor(val)}"></div></div>
        </div>`;
    }).join("");

    // Build trades table
    const tradesHTML = (w.trades || []).slice(0, 30).map(t => `
      <tr>
        <td><span class="addr">${t.tx_hash ? t.tx_hash.slice(0, 10) + "..." : "—"}</span></td>
        <td>${fmtUSD(t.usdc_amount)}</td>
        <td>${t.price != null ? (t.price * 100).toFixed(1) + "%" : "—"}</td>
        <td style="color:var(--text-secondary)">${fmtDate(t.traded_at)} ${fmtTime(t.traded_at)}</td>
      </tr>`).join("");

    // ML anomaly explanation
    const mlExplain = ms >= 0.85
      ? `This wallet is in the <strong style="color:var(--red)">top ${(100 - ms * 100).toFixed(1)}%</strong> most anomalous. The Isolation Forest model found its behavioral pattern to be highly unusual compared to the general trading population.`
      : ms >= 0.5
        ? `This wallet shows some unusual patterns. It sits in the <strong style="color:var(--amber)">top ${(100 - ms * 100).toFixed(1)}%</strong> of behavioral outliers.`
        : `This wallet's behavior is within normal statistical ranges. It ranks in the <strong style="color:var(--green)">top ${(100 - ms * 100).toFixed(1)}%</strong> — well within typical trading activity.`;

    body.innerHTML = `
      <div class="modal-addr">${w.address}</div>
      
      <div class="wallet-stats-summary">
        <div class="w-stat">
          <div class="w-stat-label">First Deposit</div>
          <div class="w-stat-value">${fmtDate(w.first_deposit_at)} <span style="font-size:10px;font-weight:400;color:var(--text-muted)">${fmtTime(w.first_deposit_at)}</span></div>
        </div>
        <div class="w-stat">
          <div class="w-stat-label">Total Activity</div>
          <div class="w-stat-value">${w.trade_count || 0} <span style="font-size:10px;font-weight:400;color:var(--text-muted)">Trades</span></div>
        </div>
        <div class="w-stat">
          <div class="w-stat-label">Diversification</div>
          <div class="w-stat-value">${w.unique_markets || 0} <span style="font-size:10px;font-weight:400;color:var(--text-muted)">Markets</span></div>
        </div>
        <div class="w-stat">
          <div class="w-stat-label">Max Exposure</div>
          <div class="w-stat-value">${fmtUSD(w.max_trade_usdc)}</div>
        </div>
      </div>

      <div class="verdict-box ${gs >= 0.65 ? 'high' : ''}">
        ${w.verdict || "Low-risk retail profile. Balanced trading activity and standard timing."}
      </div>

      <!-- FINAL SCORE -->
      <div class="final-score-banner">
        <div class="score-title">Final Unified Risk Score</div>
        <div class="score-number" style="color:${barColor(gs)}">${(gs * 100).toFixed(1)}%</div>
        <div class="score-formula">Weighted: 60% Rules + 40% ML Intelligence</div>
      </div>

      <!-- RULE-BASED SCORE (Collapsible) -->
      <div class="score-section">
        <div class="score-header" onclick="toggleAccordion(this)">
          <div class="score-header-left">
            <div class="score ${getScoreClass(rs)}" style="width:50px;text-align:center">${(rs * 100).toFixed(0)}%</div>
            <div>
              <div class="score-header-label">Rule-Based Score</div>
              <div class="score-header-sublabel">Deterministic analysis · 60% weight</div>
            </div>
          </div>
          ${CHEVRON_SVG}
        </div>
        <div class="score-content">
          <div class="breakdown-grid">${breakdownHTML}</div>
        </div>
      </div>

      <!-- ML SCORE (Collapsible) -->
      <div class="score-section">
        <div class="score-header" onclick="toggleAccordion(this)">
          <div class="score-header-left">
            <div class="score ${getScoreClass(ms)}" style="width:50px;text-align:center">${(ms * 100).toFixed(0)}%</div>
            <div>
              <div class="score-header-label">ML Anomaly Score</div>
              <div class="score-header-sublabel">Isolation Forest behavioral analysis · 40% weight</div>
            </div>
          </div>
          ${CHEVRON_SVG}
        </div>
        <div class="score-content">
          <div class="ml-explanation">${mlExplain}</div>

          <!-- Detailed feature list removed per user request -->


          <div style="margin-top:14px;padding:12px;background:var(--bg-secondary);border-radius:8px;border:1px solid var(--border);font-size:11px;color:var(--text-secondary);line-height:1.6">
            <strong style="color:var(--accent);display:block;margin-bottom:4px">How the scoring happened</strong>
            This wallet's behavioral fingerprint was analyzed using an <strong>Isolation Forest</strong> model trained on the entire population (7 feature dimensions, 200 trees, 5% contamination). 
            The model identifies "anomalies" by measuring how many random decision splits are needed to isolate a wallet; fewer splits indicate more unusual behavior. 
            The raw anomaly scores are then normalized to a 0.0–1.0 range, where 1.0 represents the most statistically extreme outliers in the system. 
            This unsupervised approach requires no historical "insider" labels and adapts dynamically as new trading patterns emerge.
          </div>
        </div>
      </div>

      <div class="trade-list-header">Recent Activity Audit</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Tx Hash</th><th>Amount</th><th>Price</th><th>Date</th></tr></thead>
          <tbody>${tradesHTML || '<tr><td colspan="4" class="loading">No trades</td></tr>'}</tbody>
        </table>
      </div>`;
  } catch (e) {
    body.innerHTML = '<div class="loading">Wallet not found or error loading data.</div>';
    console.error("Wallet detail error:", e);
  }
}

function closeModal(event) {
  if (event && event.target !== event.currentTarget) return;
  document.getElementById("modalOverlay").classList.remove("active");
}

// ── Systems Check ────────────────────────────────────────────────────────────
async function runSystemsCheck() {
  const modal = document.getElementById("healthModal");
  const body = document.getElementById("healthModalBody");
  modal.classList.add("active");
  body.innerHTML = '<div class="loading"><div class="spinner"></div> Running diagnostics...</div>';
  try {
    const res = await fetch("/api/admin/health-check");
    const data = await res.json();
    const services = [
      { key: 'postgres', label: 'Postgres DB', icon: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4' },
      { key: 'clickhouse', label: 'ClickHouse OLAP', icon: 'M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z' },
      { key: 'rabbitmq', label: 'RabbitMQ Broker', icon: 'M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4' },
      { key: 'redis', label: 'Redis Cache', icon: 'M13 10V3L4 14h7v7l9-11h-7z' },
      { key: 'alchemy', label: 'Alchemy RPC', icon: 'M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10' },
      { key: 'the_graph', label: 'The Graph Indexer', icon: 'M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z' }
    ];
    let html = '<div class="health-list">';
    services.forEach(s => {
      const st = data[s.key] || { status: 'unknown', message: 'No data' };
      const ok = st.status === 'ok';
      html += `<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;padding:10px;background:var(--bg-secondary);border-radius:8px;border:1px solid ${ok ? 'transparent' : 'var(--red)'}">
        <div style="background:var(--bg-card);padding:8px;border-radius:6px;color:${ok ? 'var(--accent)' : 'var(--red)'}">
          <svg width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="${s.icon}"/></svg>
        </div>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:600;color:var(--text-primary)">${s.label}</div>
          <div style="font-size:11px;color:var(--text-muted)">${st.message}</div>
        </div>
        <span style="color:${ok ? 'var(--accent)' : 'var(--red)'};font-size:11px;font-weight:700">${ok ? 'ONLINE' : 'OFFLINE'}</span>
      </div>`;
    });
    body.innerHTML = html + '</div>';
  } catch (e) {
    body.innerHTML = `<div style="color:var(--red)">Health check failed: ${e.message}</div>`;
  }
}

function closeHealthModal(event) {
  if (event && event.target !== event.currentTarget) return;
  document.getElementById("healthModal").classList.remove("active");
}

// ── Admin Actions ────────────────────────────────────────────────────────────
async function triggerSync() {
  const btn = document.querySelector(".btn-primary");
  const originalHTML = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner" style="margin:0"></div>';
  try {
    const res = await fetch("/api/admin/sync", { method: "POST" });
    const data = await res.json();
    addAlert("system", "Sync triggered: " + (data.status || "queued"));
  } catch (e) {
    addAlert("system", "Failed to trigger sync.");
  } finally {
    setTimeout(() => { btn.disabled = false; btn.innerHTML = originalHTML; }, 2000);
  }
}

async function confirmReset() {
  if (!confirm("⚠️ NUCLEAR OPTION: This will wipe ALL data. Are you sure?")) return;
  const pwd = prompt("Enter admin password to confirm system reset:");
  if (pwd !== "admin123") {
    alert("❌ Incorrect password. Reset cancelled.");
    return;
  }
  const btn = document.querySelector(".btn-danger");
  const originalHTML = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner" style="margin:0"></div> Resetting...';
  try {
    const res = await fetch("/api/admin/reset", { method: "POST" });
    const data = await res.json();
    alert(data.message || "System wiped successfully.");
    window.location.reload();
  } catch (e) {
    alert("Reset failed: " + e.message);
    btn.disabled = false;
    btn.innerHTML = originalHTML;
  }
}

async function refreshFlaggedWallets() {
  const btn = document.getElementById("refreshFlaggedBtn");
  const originalHTML = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner" style="margin:0;width:12px;height:12px;border-width:2px"></div>';
  try {
    const res = await fetch("/api/admin/rescore-all", { method: "POST" });
    const data = await res.json();
    addAlert("system", "Rescore started — " + (data.message || "wallets are being re-evaluated."));
    // Optimistically reload after a short delay to show progress
    setTimeout(() => { loadWallets(); updateChart(); }, 1500);
  } catch (e) {
    addAlert("warning", "Failed to trigger rescore: " + e.message);
  } finally {
    setTimeout(() => { btn.disabled = false; btn.innerHTML = originalHTML; }, 3000);
  }
}

let isPaused = false;
async function checkPauseStatus() {
  try {
    const res = await fetch("/api/admin/status");
    const data = await res.json();
    updatePauseUI(data.paused);
  } catch (e) { console.error("Pause status check failed:", e); }
}

async function togglePause() {
  const pwd = prompt("Enter admin password to toggle sync state:");
  if (pwd !== "admin123") {
    alert("❌ Incorrect password. Action cancelled.");
    return;
  }
  const btn = document.getElementById("pauseBtn");
  btn.disabled = true;
  try {
    console.log("Toggling pause state...");
    const res = await fetch("/api/admin/toggle-pause", { method: "POST" });
    const data = await res.json();
    console.log("New pause state:", data.paused);
    updatePauseUI(data.paused);
  } catch (e) {
    console.error("Toggle pause failed:", e);
    addAlert("warning", "Failed to toggle pause state.");
  } finally {
    btn.disabled = false;
  }
}

function updatePauseUI(paused) {
  isPaused = paused;
  const btn = document.getElementById("pauseBtn");
  const text = document.getElementById("pauseText");
  const icon = document.getElementById("pauseIcon");
  const badge = document.getElementById("statusBadge");

  if (paused) {
    btn.classList.add("btn-success");
    btn.classList.remove("btn-danger", "btn-secondary");
    text.textContent = "Resume Live Sync";
    icon.setAttribute("d", "M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z M21 12a9 9 0 11-18 0 9 9 0 0118 0z");
    badge.classList.remove("live");
    badge.classList.add("paused");
    document.getElementById("statusText").textContent = "Paused";
  } else {
    btn.classList.add("btn-danger");
    btn.classList.remove("btn-success", "btn-secondary");
    text.textContent = "Pause Live Sync";
    icon.setAttribute("d", "M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z");
    badge.classList.add("live");
    badge.classList.remove("paused");
    document.getElementById("statusText").textContent = "Live";
  }
}

// ── WebSocket ────────────────────────────────────────────────────────────────
let ws = null;
let wsReconnectDelay = 1000;

function connectWebSocket() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${protocol}//${location.host}/ws/alerts`);

  ws.onopen = () => {
    document.getElementById("statusText").textContent = "Live";
    wsReconnectDelay = 1000;
    addAlert("system", "Live Feed connection established. Monitoring backend events...");
  };

  ws.onmessage = async (event) => {
    try {
      const a = JSON.parse(event.data);

      addAlert(
        a.type || "info",
        a.message || JSON.stringify(a)
      );

      if (a.type === "pipeline_complete") {
        await Promise.all([
          loadWallets(),
          updateChart(),
          loadInitialTrades(),
          loadStats()
        ]);
      } else {
        await Promise.all([
          loadStats(),
          loadInitialTrades()
        ]);
      }

    } catch (e) {
      addAlert("info", event.data);
    }
  };

  ws.onclose = () => {
    document.getElementById("statusText").textContent = "Reconnecting...";
    setTimeout(connectWebSocket, wsReconnectDelay);
    wsReconnectDelay = Math.min(wsReconnectDelay * 2, 30000);
  };

  ws.onerror = () => {
    ws.close();
  };
}

function addAlert(type, message, forcedTime = null) {
  const targets = [];
  if (type === "historical_trade") {
    targets.push("historicalFeed");
  } else if (type === "live_trade" || type === "new_trades") {
    targets.push("liveTradeFeed");
  } else {
    targets.push("alertFeed");
  }

  const now = (forcedTime && !isNaN(new Date(forcedTime).getTime())) ? new Date(forcedTime) : new Date();
  const timeStr = now.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });

  targets.forEach(feedId => {
    const feed = document.getElementById(feedId);
    if (!feed) return;

    const empty = feed.querySelector(".alert-empty");
    if (empty) empty.remove();

    const item = document.createElement("div");
    item.className = "alert-item animate-in";

    let typeTag = "";
    if (type === "system") typeTag = '<span class="tag sys">SYS</span> ';
    if (type === "new_trades" || type === "live_trade") typeTag = '<span class="tag green">LIVE</span> ';
    if (type === "historical_trade") typeTag = '<span class="tag cyan">HIST</span> ';
    if (type === "pipeline_complete") typeTag = '<span class="tag both">PIPE</span> ';
    if (type === "info") typeTag = '<span class="tag ml">INFO</span> ';
    if (type === "warning") typeTag = '<span class="tag warn">WARN</span> ';

    item.innerHTML = `<div class="alert-time">${timeStr}</div><div class="alert-text">${typeTag}${message}</div>`;
    feed.prepend(item);

    if (feed.children.length > 100) feed.removeChild(feed.lastChild);
  });
}

async function loadInitialTrades() {
  try {
    const [hRes, lRes] = await Promise.all([
      fetch("/api/trades/historical?limit=50"),
      fetch("/api/trades/live?limit=50")
    ]);

    if (hRes.ok) {
      const hTrades = await hRes.json();
      const feed = document.getElementById("historicalFeed");
      if (feed) feed.innerHTML = "";
      hTrades.reverse().forEach(t => {
        addAlert("historical_trade", `Indexed: ${fmtAddr(t.maker)} bought ${fmtUSD(t.usdc_amount)}`, t.traded_at);
      });
    }

    if (lRes.ok) {
      const lTrades = await lRes.json();
      const feed = document.getElementById("liveTradeFeed");
      if (feed) feed.innerHTML = "";
      lTrades.reverse().forEach(t => {
        addAlert("live_trade", `Live: ${fmtAddr(t.maker)} traded ${fmtUSD(t.usdc_amount)}`, t.traded_at);
      });
    }
  } catch (e) {
    console.error("Failed to load initial trades:", e);
  }
}

async function loadAlertHistory() {
  try {
    const res = await fetch("/api/alerts/history?limit=100");
    const alerts = await res.json();
    // History comes in newest first, so we process in reverse to keep order or just clear and add
    const feed = document.getElementById("alertFeed");
    const empty = feed.querySelector(".alert-empty");
    if (alerts.length > 0 && empty) empty.remove();

    // Reverse to add oldest first so they end up at the bottom
    alerts.reverse().forEach(a => {
      addAlert(a.type, a.message, a.timestamp ? new Date(a.timestamp * 1000) : null);
    });
  } catch (e) {
    console.error("Failed to load alert history:", e);
  }
}

// ── Charting ─────────────────────────────────────────────────────────────────
async function initChart() {
  const ctx = document.getElementById('scoreChart').getContext('2d');
  scoreChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['0-10%', '10-20%', '20-30%', '30-40%', '40-50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%'],
      datasets: [{
        label: 'Wallets', data: new Array(10).fill(0),
        backgroundColor: 'rgba(99, 102, 241, 0.4)',
        borderColor: 'rgba(99, 102, 241, 1)',
        borderWidth: 1, borderRadius: 4
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#5a6478' } },
        x: { grid: { display: false }, ticks: { color: '#5a6478' } }
      },
      plugins: {
        legend: { display: false },
        tooltip: { backgroundColor: '#171c28', titleColor: '#e2e8f0', bodyColor: '#8892a6', borderColor: '#252d3d', borderWidth: 1 }
      }
    }
  });
  updateChart();
}

async function updateChart() {
  if (!scoreChart) return;

  try {
    const bins = new Array(10).fill(0);

    let page = 1;
    const perPage = 100;
    let totalFetched = 0;

    while (true) {
      const res = await fetch(
        `/api/flagged?per_page=${perPage}&page=${page}&sort_by=global_score`
      );

      const data = await res.json();
      const wallets = data.wallets || [];

      // Stop if no more rows returned
      if (wallets.length === 0) break;

      wallets.forEach(w => {
        const score = w.global_score ?? w.insider_score ?? 0;

        // Clamp score safely into bins 0–9
        const idx = Math.max(0, Math.min(9, Math.floor(score * 10)));

        bins[idx]++;
      });

      totalFetched += wallets.length;

      // If last page has fewer than perPage rows, we are done
      if (wallets.length < perPage) break;

      page++;
    }

    scoreChart.data.datasets[0].data = bins;
    scoreChart.update();

    console.log(`Chart updated using ${totalFetched} wallets`);
  } catch (e) {
    console.error("Chart update failed:", e);
  }
}


// ── Initialization ───────────────────────────────────────────────────────────
async function init() {
  checkPauseStatus();
  loadStats();
  loadWallets();
  initChart();
  await loadInitialTrades();
  await loadAlertHistory();
  connectWebSocket();
  setInterval(loadStats, 30000);
}

init();

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") { closeModal(); closeHealthModal(); }
});
