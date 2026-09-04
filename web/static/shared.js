// Shared across bot.html and trading.html: fetch helpers, formatters, the
// chart modal, and every card in the common header partial (_header.html —
// mode tabs, bot status/controls, Gateway Connection, Account, My Gateway,
// My IBKR Login). Each page's own inline <script> defines a page-specific
// refreshAll() that this file's setMode()/the periodic timer call by name —
// load this file BEFORE the page's own script.
const POLL_MS = 5000;
const CLOSE_LIVE_CONFIRM_PHRASE = "ok";
let currentMode = "live";
let nextCycleAtMs = null;
let isAdmin = false;

async function api(path, options) {
  const res = await fetch(path, Object.assign({ credentials: "same-origin" }, options || {}));
  if (res.status === 401) { window.location.href = "/login"; throw new Error("unauthenticated"); }
  if (!res.ok) {
    const text = await res.text();
    // err.status lets a caller distinguish WHY a request failed (e.g. 409
    // "needs the open-positions override prompt" vs any other rejection)
    // without parsing detail text - see btn-gw-disconnect's own handler.
    let err;
    try { err = new Error(JSON.parse(text).detail || text); } catch (e) { err = new Error(e.message || text); }
    err.status = res.status;
    throw err;
  }
  return res.status === 204 ? null : res.json();
}

function modeApi(path, options) {
  const sep = path.includes("?") ? "&" : "?";
  return api(`${path}${sep}mode=${currentMode}`, options);
}

function fmtMoney(v) { return (v === null || v === undefined || v === "") ? "-" : `$${Number(v).toFixed(2)}`; }
function fmtR(v) { return (v === null || v === undefined) ? "-" : `${Number(v).toFixed(2)}R`; }
function fmtPnl(v) {
  if (v === null || v === undefined) return "-";
  const cls = v > 0 ? "text-success" : (v < 0 ? "text-danger" : "");
  return `<span class="${cls}">${fmtMoney(v)}</span>`;
}
function symbolLink(symbol) { return `<a href="#" class="symbol-link" data-symbol="${symbol}">${symbol}</a>`; }
function sideBadge(side) {
  return side === "short"
    ? '<span class="badge bg-danger">SHORT</span>'
    : '<span class="badge bg-primary">LONG</span>';
}

// ---------------------------------------------------------------- mode ---
function setMode(mode) {
  currentMode = mode;
  document.getElementById("tab-paper").classList.toggle("active-paper", mode === "paper");
  document.getElementById("tab-live").classList.toggle("active-live", mode === "live");
  document.getElementById("mode-hint").textContent = mode.toUpperCase();
  document.getElementById("mode-hint").className = "fw-bold " + (mode === "live" ? "text-danger" : "text-info");
  refreshAll();  // defined by the current page's own script
}
document.getElementById("tab-paper").addEventListener("click", () => setMode("paper"));
document.getElementById("tab-live").addEventListener("click", () => setMode("live"));

// ------------------------------------------------------------- status ---
async function refreshStatus() {
  const s = await modeApi("/api/status");
  const badge = document.getElementById("status-badge");
  badge.textContent = s.bot_enabled ? "ENABLED" : "PAUSED";
  badge.className = "badge fs-6 " + (s.bot_enabled ? "bg-success" : "bg-warning text-dark");
  document.getElementById("last-cycle").textContent =
    s.last_cycle_timestamp ? `${s.last_cycle_timestamp} (${s.last_cycle_status})` : "no cycle data yet";
  nextCycleAtMs = s.next_cycle_at ? new Date(s.next_cycle_at).getTime() : null;
  updateCountdown();
  refreshEsFilterStatus();
  refreshYfinanceStatus();
}

// -------------------------------------------------------- market data ---
async function refreshYfinanceStatus() {
  const badge = document.getElementById("yfinance-status-badge");
  if (!badge) return;
  const s = await modeApi("/api/yfinance_status");
  if (s.degraded) {
    badge.textContent = `Data: degraded (${s.streak}x)`;
    badge.className = "badge bg-danger";
  } else if (s.streak > 0) {
    badge.textContent = `Data: OK (${s.streak} recent fail${s.streak === 1 ? "" : "s"})`;
    badge.className = "badge bg-warning text-dark";
  } else {
    badge.textContent = "Data: OK";
    badge.className = "badge bg-success";
  }
}

// ------------------------------------------------------- ES VWAP filter ---
async function refreshEsFilterStatus() {
  const toggle = document.getElementById("es-filter-toggle");
  if (!toggle) return;  // viewer role - _header.html's control cards aren't rendered
  const s = await modeApi("/api/es_filter_status");
  toggle.checked = s.enabled;
  document.getElementById("es-filter-toggle-label").textContent = s.enabled ? "Enabled" : "Disabled";
}
if (document.getElementById("es-filter-toggle")) {
  document.getElementById("es-filter-toggle").addEventListener("change", async (e) => {
    const enabled = e.target.checked;
    const errEl = document.getElementById("es-filter-error");
    errEl.textContent = "";
    try {
      const s = await modeApi("/api/es_filter_status", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled }),
      });
      document.getElementById("es-filter-toggle-label").textContent = s.enabled ? "Enabled" : "Disabled";
    } catch (err) {
      e.target.checked = !enabled;  // revert the switch - the request failed
      errEl.textContent = err.message || "Failed to update";
    }
  });
}

function updateCountdown() {
  const el = document.getElementById("next-cycle-countdown");
  if (!nextCycleAtMs) { el.textContent = "-"; return; }
  const remainingSec = Math.round((nextCycleAtMs - Date.now()) / 1000);
  if (remainingSec <= 0) { el.textContent = "any moment…"; return; }
  const mm = Math.floor(remainingSec / 60);
  const ss = remainingSec % 60;
  el.textContent = `${mm}:${String(ss).padStart(2, "0")}`;
}

document.getElementById("btn-enable").addEventListener("click", async () => { await modeApi("/api/control/enable", { method: "POST" }); refreshStatus(); });
document.getElementById("btn-disable").addEventListener("click", async () => { await modeApi("/api/control/disable", { method: "POST" }); refreshStatus(); });
document.getElementById("btn-flatten").addEventListener("click", async () => {
  if (!confirm(`This will market-sell every open ${currentMode.toUpperCase()} position immediately. Continue?`)) return;
  await modeApi("/api/control/flatten", { method: "POST" });
  refreshStatus();
});

// ------------------------------------------------------------ account ---
async function refreshAccount() {
  const a = await modeApi("/api/account");
  const el = document.getElementById("account-summary");
  if (!a.updated_at) {
    el.innerHTML = '<p class="text-muted mb-0">No account data yet — the service refreshes this every 5 minutes.</p>';
    return;
  }
  el.innerHTML = `
    <div class="row">
      <div class="col-md-4"><div class="text-muted small">Net Liquidation</div><div class="fs-5">${fmtMoney(a.net_liquidation)}</div></div>
      <div class="col-md-4"><div class="text-muted small">Cash Balance</div><div class="fs-5">${fmtMoney(a.cash_balance)}</div></div>
      <div class="col-md-4"><div class="text-muted small">Buying Power</div><div class="fs-5">${fmtMoney(a.buying_power)}</div></div>
    </div>
    <div class="text-muted small mt-2">Updated: ${a.updated_at}</div>`;
}

document.getElementById("btn-refresh-account").addEventListener("click", async () => {
  const btn = document.getElementById("btn-refresh-account");
  const errorEl = document.getElementById("refresh-account-error");
  errorEl.textContent = "";
  btn.disabled = true;
  btn.textContent = "Refreshing…";
  try {
    await modeApi("/api/account/refresh", { method: "POST" });
    await refreshAccount();
    // Only defined on pages (trading.html) that render the broker positions
    // table - bot.html shares this header/button but has no such table.
    if (typeof refreshOrders === "function") await refreshOrders();
    if (typeof refreshBrokerPositions === "function") await refreshBrokerPositions();
  } catch (e) {
    errorEl.textContent = e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Refresh now";
  }
});

// ----------------------------------------------------- gateway connection
async function refreshGatewayStatus() {
  const s = await modeApi("/api/gateway/status");
  const badge = document.getElementById("gw-status-badge");
  const detail = document.getElementById("gw-detail");

  let label, cls;
  if (s.gateway_active && s.port_listening && s.engine_active) {
    label = "Connected"; cls = "bg-success";
  } else if (s.gateway_active && !s.port_listening) {
    label = "Connecting… check your phone for a 2FA approval"; cls = "bg-warning text-dark";
  } else if (s.gateway_active && s.port_listening && !s.engine_active) {
    label = "Gateway up, trading paused"; cls = "bg-info text-dark";
  } else {
    label = "Disconnected"; cls = "bg-secondary";
  }
  badge.textContent = label;
  badge.className = "badge " + cls;
  detail.textContent = `gateway=${s.gateway_active} port_listening=${s.port_listening} engine=${s.engine_active}`;

  document.getElementById("btn-gw-disconnect").classList.toggle("d-none", !(s.gateway_active || s.engine_active));
  document.getElementById("btn-gw-reconnect").classList.toggle("d-none", s.gateway_active);
  document.getElementById("btn-gw-resume").classList.toggle("d-none", !(s.gateway_active && s.port_listening && !s.engine_active));
}

document.getElementById("btn-gw-disconnect").addEventListener("click", async () => {
  const errorEl = document.getElementById("gw-error");
  errorEl.textContent = "";
  const payload = {};
  if (currentMode === "live") {
    const typed = prompt(
      'This stops the LIVE Gateway AND trading engine so you can log into TWS/Mobile with the same account.\n' +
      'Type "ok" to confirm:'
    );
    if (typed !== "ok") { errorEl.textContent = "Not confirmed — nothing was disconnected."; return; }
    payload.confirm = typed;
  } else if (!confirm("Disconnect the PAPER Gateway and trading engine so you can log into TWS/Mobile?")) {
    return;
  }
  try {
    await modeApi("/api/gateway/disconnect", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    refreshGatewayStatus();
  } catch (e) {
    // 409 = an open position would be left unmanaged (see /api/gateway/
    // disconnect's own docstring) - a SECOND, separate typed confirmation
    // for that specific risk, then one retry with it attached. Any other
    // status (validation, LIVE confirm mismatch, 500) just surfaces as-is.
    if (e.status !== 409) { errorEl.textContent = "Disconnect failed: " + e.message; return; }
    const typed = prompt(
      e.message + '\n\nType "ok" to disconnect anyway and leave the position(s) unmanaged:'
    );
    if (typed !== "ok") { errorEl.textContent = "Not confirmed — nothing was disconnected."; return; }
    payload.confirm_open_positions = typed;
    try {
      await modeApi("/api/gateway/disconnect", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
      });
      refreshGatewayStatus();
    } catch (e2) {
      errorEl.textContent = "Disconnect failed: " + e2.message;
    }
  }
});

document.getElementById("btn-gw-reconnect").addEventListener("click", async () => {
  const errorEl = document.getElementById("gw-error");
  errorEl.textContent = "";
  try {
    await modeApi("/api/gateway/reconnect", { method: "POST" });
    refreshGatewayStatus();
  } catch (e) {
    errorEl.textContent = "Reconnect failed: " + e.message;
  }
});

document.getElementById("btn-gw-resume").addEventListener("click", async () => {
  const errorEl = document.getElementById("gw-error");
  errorEl.textContent = "";
  try {
    await modeApi("/api/gateway/resume_engine", { method: "POST" });
    refreshGatewayStatus();
  } catch (e) {
    errorEl.textContent = "Resume failed: " + e.message;
  }
});

// Always available (unlike btn-gw-reconnect, never hidden by the current
// gateway_active state) - stops the engine + Gateway and starts the
// Gateway back up in one click, for when the Gateway is stuck rather than
// cleanly stopped. Same safety gates as Disconnect (blocked on an open
// position, LIVE needs the typed confirmation) since it stops the engine too.
document.getElementById("btn-gw-reinit").addEventListener("click", async () => {
  const errorEl = document.getElementById("gw-error");
  errorEl.textContent = "";
  const payload = {};
  if (currentMode === "live") {
    const typed = prompt(
      'This fully reinitializes the LIVE Gateway: stops the engine + Gateway, then starts the Gateway back up.\n' +
      'Blocked while any LIVE position is open. Type "ok" to confirm:'
    );
    if (typed !== "ok") { errorEl.textContent = "Not confirmed — nothing was reinitialized."; return; }
    payload.confirm = typed;
  } else if (!confirm("Fully reinitialize the PAPER Gateway (stop engine + Gateway, then start the Gateway back up)?")) {
    return;
  }
  try {
    await modeApi("/api/gateway/reinitialize", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    refreshGatewayStatus();
  } catch (e) {
    errorEl.textContent = "Reinitialize failed: " + e.message;
  }
});

// --------------------------------------------------------- my gateway ---
async function refreshMe() {
  const me = await api("/api/me");
  isAdmin = me.is_admin;
  document.getElementById("my-gateway-row").classList.toggle("d-none", isAdmin);
}

function fmtGwLine(label, s) {
  return `${label}: gateway=${s.gateway_active} port=${s.port_listening} engine=${s.engine_active}`;
}

async function refreshMyGateway() {
  if (isAdmin) return;
  const errorEl = document.getElementById("my-gw-error");
  try {
    const s = await api("/api/my_gateway/status");
    const badge = document.getElementById("my-gw-badge");
    const bothTrading = s.paper.engine_active && s.live.engine_active;
    const bothConnected = s.paper.port_listening && s.live.port_listening;
    const anyStarting = s.paper.gateway_active || s.live.gateway_active;
    let label, cls;
    if (bothTrading) { label = "Trading"; cls = "bg-success"; }
    else if (bothConnected) { label = "Connected — click Resume Trading"; cls = "bg-info text-dark"; }
    else if (anyStarting) { label = "Connecting… check your phone for a 2FA approval"; cls = "bg-warning text-dark"; }
    else { label = "Not connected"; cls = "bg-secondary"; }
    badge.textContent = label;
    badge.className = "badge " + cls;
    document.getElementById("my-gw-detail").textContent =
      fmtGwLine("paper", s.paper) + " | " + fmtGwLine("live", s.live);
  } catch (e) {
    errorEl.textContent = "";  // stay quiet on transient poll errors, only show action-triggered ones
  }
}

document.getElementById("btn-my-gw-connect").addEventListener("click", async () => {
  const errorEl = document.getElementById("my-gw-error");
  errorEl.textContent = "";
  try {
    await api("/api/my_gateway/connect", { method: "POST" });
    refreshMyGateway();
  } catch (e) {
    errorEl.textContent = "Connect failed: " + e.message;
  }
});

document.getElementById("btn-my-gw-resume").addEventListener("click", async () => {
  const errorEl = document.getElementById("my-gw-error");
  errorEl.textContent = "";
  try {
    await api("/api/my_gateway/resume", { method: "POST" });
    refreshMyGateway();
  } catch (e) {
    errorEl.textContent = "Resume failed: " + e.message;
  }
});

document.getElementById("btn-my-gw-disconnect").addEventListener("click", async () => {
  const errorEl = document.getElementById("my-gw-error");
  errorEl.textContent = "";
  if (!confirm("Stop your Gateway and trading engine?")) return;
  try {
    await api("/api/my_gateway/disconnect", { method: "POST" });
    refreshMyGateway();
  } catch (e) {
    errorEl.textContent = "Disconnect failed: " + e.message;
  }
});

// ------------------------------------------------------- IBKR credentials
const IBKR_CREDS_INPUT_IDS = ["ibkr-username", "ibkr-password"];

async function refreshIbkrCredentials() {
  // Don't clobber what's being typed if the 5s poll lands mid-edit.
  if (IBKR_CREDS_INPUT_IDS.includes(document.activeElement && document.activeElement.id)) return;

  const c = await api("/api/ibkr_credentials");
  const badge = document.getElementById("ibkr-creds-badge");
  if (c.configured) {
    badge.textContent = `Connected: ${c.ibkr_username}`;
    badge.className = "badge bg-success";
    document.getElementById("ibkr-username").value = c.ibkr_username;
  } else {
    badge.textContent = "Not configured";
    badge.className = "badge bg-secondary";
  }
}

document.getElementById("btn-save-ibkr-creds").addEventListener("click", async () => {
  const errorEl = document.getElementById("ibkr-creds-error");
  errorEl.textContent = "";
  const ibkr_username = document.getElementById("ibkr-username").value.trim();
  const ibkr_password = document.getElementById("ibkr-password").value;
  if (!ibkr_username || !ibkr_password) {
    errorEl.textContent = "Both username and password are required to save.";
    return;
  }
  try {
    await api("/api/ibkr_credentials", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ibkr_username, ibkr_password }),
    });
    document.getElementById("ibkr-password").value = "";
    refreshIbkrCredentials();
  } catch (e) {
    errorEl.textContent = "Save failed: " + e.message;
  }
});

// ------------------------------------------------------------- chart ---
let chartModal = null, chartInstance = null, candleSeries = null, volumeSeries = null, rsiSeries = null;
let sma20LineSeries = null, sma200LineSeries = null;
let chartRefreshTimer = null, chartResizeHandler = null, pendingChartSymbol = null, chartSmaLines = [];
let currentChartInterval = "5m";
const CHART_INTERVAL_CAPTIONS = {
  "1m": "1-minute candles, last 5 trading days",
  "5m": "5-minute candles, last 5 trading days",
  "15m": "15-minute candles, last month",
  "30m": "30-minute candles, last month",
  "1h": "1-hour candles, last 3 months",
  "1d": "Daily candles, last 6 months",
};

function updateChartCaption() {
  const base = CHART_INTERVAL_CAPTIONS[currentChartInterval] || "";
  const prepost = currentChartInterval === "1d" ? "" : " (includes pre/post market)";
  document.getElementById("chart-caption").textContent =
    `${base}${prepost}. Volume and RSI(14) below the price; MA20/MA200 (solid lines) track the moving average over time; SMA50/SMA200 (dashed flat lines, when available) are the daily thresholds D2 actually checks. Refreshes every 30s while open.`;
}

async function loadChartData(symbol) {
  try {
    const data = await modeApi(`/api/candles?symbol=${encodeURIComponent(symbol)}&interval=${currentChartInterval}`);
    if (!candleSeries) return;
    const candles = data.candles || [];
    candleSeries.setData(candles);

    if (volumeSeries) {
      const candleByTime = new Map(candles.map(c => [c.time, c]));
      volumeSeries.setData((data.volume || []).map(v => {
        const c = candleByTime.get(v.time);
        const up = !c || c.close >= c.open;
        return { time: v.time, value: v.value, color: up ? "rgba(38,166,154,0.5)" : "rgba(239,83,80,0.5)" };
      }));
    }

    if (rsiSeries) rsiSeries.setData(data.rsi || []);
    if (sma20LineSeries) sma20LineSeries.setData(data.sma20_series || []);
    if (sma200LineSeries) sma200LineSeries.setData(data.sma200_series || []);

    // Reference price lines don't update in place - clear last load's and
    // redraw so a stale SMA doesn't linger after refresh/interval switch.
    chartSmaLines.forEach(l => candleSeries.removePriceLine(l));
    chartSmaLines = [];
    if (data.sma50 !== null && data.sma50 !== undefined) {
      chartSmaLines.push(candleSeries.createPriceLine({
        price: data.sma50, color: "#f39c12", lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: "SMA50",
      }));
    }
    if (data.sma200 !== null && data.sma200 !== undefined) {
      chartSmaLines.push(candleSeries.createPriceLine({
        price: data.sma200, color: "#2980b9", lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: "SMA200",
      }));
    }
  } catch (e) { /* keep showing the last good data on a transient fetch failure */ }
}

function openChart(symbol) {
  document.getElementById("chart-modal-title").textContent = `${symbol} — ${currentMode.toUpperCase()}`;
  pendingChartSymbol = symbol;
  currentChartInterval = "5m";
  document.querySelectorAll(".chart-interval-btn").forEach(btn => btn.classList.toggle("active", btn.dataset.interval === currentChartInterval));
  updateChartCaption();
  if (!chartModal) chartModal = new bootstrap.Modal(document.getElementById("chart-modal"));
  chartModal.show();
}

document.getElementById("chart-interval-group").addEventListener("click", (e) => {
  const btn = e.target.closest(".chart-interval-btn");
  if (!btn || !pendingChartSymbol) return;
  currentChartInterval = btn.dataset.interval;
  document.querySelectorAll(".chart-interval-btn").forEach(b => b.classList.toggle("active", b === btn));
  updateChartCaption();
  loadChartData(pendingChartSymbol);
});

// Bootstrap gives the modal display:block only once it's actually shown -
// creating the chart any earlier measures a 0-width/0-height container, so
// lightweight-charts silently renders nothing (a blank modal, no gridlines
// even). Wait for shown.bs.modal so the container has real dimensions.
document.getElementById("chart-modal").addEventListener("shown.bs.modal", () => {
  const symbol = pendingChartSymbol;
  if (!symbol) return;
  const container = document.getElementById("chart-container");
  container.innerHTML = "";
  if (typeof LightweightCharts === "undefined") {
    container.innerHTML = '<p class="text-danger">Could not load the charting library (blocked network request?) - try refreshing.</p>';
    return;
  }
  chartInstance = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 600,
    layout: { background: { color: "#ffffff" }, textColor: "#212529" },
    grid: { vertLines: { color: "#eee" }, horzLines: { color: "#eee" } },
    timeScale: { timeVisible: true, secondsVisible: false },
    rightPriceScale: { scaleMargins: { top: 0.05, bottom: 0.45 } },
  });
  // No true multi-pane support in this chart library version - price,
  // volume, and RSI share one pane/time-axis, each on its own price scale
  // pinned to a vertical band via scaleMargins (top band: price, middle:
  // volume, bottom: RSI).
  candleSeries = chartInstance.addCandlestickSeries();

  // Shares the price pane's own scale (no priceScaleId override) so both
  // moving averages overlay directly on the candles, not a separate band.
  sma20LineSeries = chartInstance.addLineSeries({ color: "#16a085", lineWidth: 2, priceLineVisible: false, lastValueVisible: true, title: "MA20" });
  sma200LineSeries = chartInstance.addLineSeries({ color: "#2980b9", lineWidth: 2, priceLineVisible: false, lastValueVisible: true, title: "MA200" });

  volumeSeries = chartInstance.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "volume" });
  chartInstance.priceScale("volume").applyOptions({ scaleMargins: { top: 0.58, bottom: 0.30 } });

  rsiSeries = chartInstance.addLineSeries({ color: "#8e44ad", lineWidth: 1.5, priceScaleId: "rsi" });
  chartInstance.priceScale("rsi").applyOptions({ scaleMargins: { top: 0.75, bottom: 0.02 } });
  rsiSeries.createPriceLine({ price: 50, color: "#999", lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Dotted, axisLabelVisible: false, title: "" });

  loadChartData(symbol);
  refreshPriceTriggers(symbol);
  if (chartRefreshTimer) clearInterval(chartRefreshTimer);
  chartRefreshTimer = setInterval(() => { loadChartData(symbol); refreshPriceTriggers(symbol); }, 30000);
  chartResizeHandler = () => chartInstance && chartInstance.applyOptions({ width: container.clientWidth });
  window.addEventListener("resize", chartResizeHandler);
});

document.getElementById("chart-modal").addEventListener("hidden.bs.modal", () => {
  if (chartRefreshTimer) { clearInterval(chartRefreshTimer); chartRefreshTimer = null; }
  if (chartResizeHandler) { window.removeEventListener("resize", chartResizeHandler); chartResizeHandler = null; }
  if (chartInstance) { chartInstance.remove(); chartInstance = null; }
  candleSeries = null;
  volumeSeries = null;
  rsiSeries = null;
  sma20LineSeries = null;
  sma200LineSeries = null;
  chartSmaLines = [];
  priceTriggerLines = [];
  pendingChartSymbol = null;
});

// ------------------------------------------------------ price triggers ---
// "Buy line"/"sell line" - a real native IBKR STP order placed from the
// chart (see place_price_trigger.py); once it fills, cycle.py starts
// managing the resulting position like any strategy-opened one.
let priceTriggerLines = [];
const PRICE_TRIGGER_LIVE_CONFIRM_PHRASE = "ok";

async function refreshPriceTriggers(symbol) {
  const body = document.getElementById("pt-body");
  if (!body) return;
  let rows;
  try {
    rows = (await modeApi("/api/price_triggers")).filter(t => t.symbol === symbol);
  } catch (e) { return; }

  body.innerHTML = rows.length ? rows.map(t => `
    <tr>
      <td>${t.side === "long" ? "קניה" : "מכירה"}</td>
      <td>${fmtMoney(t.trigger_price)}</td>
      <td>${fmtMoney(t.stop_price)}</td>
      <td>${t.qty}</td>
      <td><button type="button" class="btn btn-sm btn-outline-danger pt-cancel-btn" data-id="${t.id}">בטל</button></td>
    </tr>`).join("") : '<tr><td colspan="5" class="text-center text-muted">אין הוראות ממתינות</td></tr>';

  if (candleSeries) {
    priceTriggerLines.forEach(l => candleSeries.removePriceLine(l));
    priceTriggerLines = rows.map(t => candleSeries.createPriceLine({
      price: t.trigger_price, color: t.side === "long" ? "#16a085" : "#c0392b", lineWidth: 2,
      lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true,
      title: t.side === "long" ? "BUY" : "SELL",
    }));
  }
}

document.getElementById("pt-body").addEventListener("click", async (e) => {
  const btn = e.target.closest(".pt-cancel-btn");
  if (!btn || !pendingChartSymbol) return;
  btn.disabled = true;
  try {
    await modeApi(`/api/price_triggers/${btn.dataset.id}`, { method: "DELETE" });
    refreshPriceTriggers(pendingChartSymbol);
  } catch (e) {
    alert("ביטול ההוראה נכשל: " + e.message);
    btn.disabled = false;
  }
});

document.getElementById("pt-submit").addEventListener("click", async () => {
  const errorEl = document.getElementById("pt-error");
  errorEl.textContent = "";
  if (!pendingChartSymbol) return;
  const payload = {
    symbol: pendingChartSymbol,
    side: document.getElementById("pt-side").value,
    trigger_price: document.getElementById("pt-trigger-price").value,
    stop_price: document.getElementById("pt-stop-price").value,
    qty: document.getElementById("pt-qty").value,
  };
  if (currentMode === "live") {
    const typed = prompt(`זו הוראה אמיתית שתפעל בכסף אמת ברגע שהמחיר יגיע לטריגר.\nהקלד "${PRICE_TRIGGER_LIVE_CONFIRM_PHRASE}" לאישור:`);
    if (typed !== PRICE_TRIGGER_LIVE_CONFIRM_PHRASE) {
      errorEl.textContent = "לא אושר - ההוראה לא נשלחה.";
      return;
    }
    payload.confirm = typed;
  }
  try {
    await modeApi("/api/price_triggers", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    document.getElementById("pt-trigger-price").value = "";
    document.getElementById("pt-stop-price").value = "";
    document.getElementById("pt-qty").value = "";
    refreshPriceTriggers(pendingChartSymbol);
  } catch (e) {
    errorEl.textContent = "שליחת ההוראה נכשלה: " + e.message;
  }
});

document.addEventListener("click", (e) => {
  const link = e.target.closest(".symbol-link");
  if (!link) return;
  e.preventDefault();
  openChart(link.dataset.symbol);
});

// ---------------------------------------------------------------- init ---
document.addEventListener("DOMContentLoaded", async () => {
  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => new bootstrap.Tooltip(el));
  await refreshMe();
  setMode("live");  // triggers the first refreshAll() via setMode
  setInterval(() => refreshAll(), POLL_MS);
  setInterval(updateCountdown, 1000);
});
