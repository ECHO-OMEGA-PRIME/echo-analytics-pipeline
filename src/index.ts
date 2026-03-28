/**
 * ECHO ANALYTICS PIPELINE v1.0.0
 * Business Intelligence Engine for Echo Omega Prime
 *
 * Aggregates data from ALL workers into a unified analytics warehouse.
 * Computes KPIs, detects trends, identifies anomalies, generates reports.
 *
 * Data Sources: Bots (X/LinkedIn/Telegram/Reddit), Engine Runtime,
 *   Knowledge Forge, Fleet Commander, Daemon, Build Orchestrator, Shared Brain
 *
 * Outputs: Real-time dashboards, daily/weekly/monthly reports,
 *   anomaly alerts, growth forecasts, revenue signals
 */

export interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  SHARED_BRAIN: Fetcher;
  SWARM_BRAIN: Fetcher;
  ENGINE_RUNTIME: Fetcher;
  KNOWLEDGE_FORGE: Fetcher;
  FLEET_COMMANDER: Fetcher;
  DAEMON: Fetcher;
  BUILD_ORCHESTRATOR: Fetcher;
  X_BOT: Fetcher;
  LINKEDIN: Fetcher;
  TELEGRAM: Fetcher;
  REDDIT: Fetcher;
  OMNISYNC: Fetcher;
  ECHO_API_KEY: string;
}

// ── Logging ──────────────────────────────────────────────────────────────────

function log(level: string, msg: string, data?: Record<string, unknown>): void {
  const entry = { ts: new Date().toISOString(), level, service: 'echo-analytics-pipeline', msg, ...data };
  if (level === 'error') console.error(JSON.stringify(entry));
  else console.log(JSON.stringify(entry));
}

// ── Types ────────────────────────────────────────────────────────────────────

interface MetricSnapshot {
  source: string;
  metric: string;
  value: number;
  unit: string;
  tags: Record<string, string>;
  timestamp: string;
}

interface KPI {
  name: string;
  value: number;
  unit: string;
  trend: 'up' | 'down' | 'flat';
  changePercent: number;
  period: string;
}

interface AnomalyAlert {
  metric: string;
  source: string;
  expected: number;
  actual: number;
  severity: 'low' | 'medium' | 'high' | 'critical';
  message: string;
}

interface TrendPoint {
  date: string;
  value: number;
}

interface GrowthForecast {
  metric: string;
  currentValue: number;
  projectedValue: number;
  growthRate: number;
  confidence: number;
  horizon: string;
}

// ── Schema ───────────────────────────────────────────────────────────────────

async function ensureSchema(db: D1Database): Promise<void> {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS metrics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      metric TEXT NOT NULL,
      value REAL NOT NULL,
      unit TEXT DEFAULT '',
      tags TEXT DEFAULT '{}',
      recorded_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_metrics_source ON metrics(source, metric, recorded_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_metrics_time ON metrics(recorded_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_metrics_metric ON metrics(metric, recorded_at DESC)`,

    `CREATE TABLE IF NOT EXISTS daily_kpis (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      date TEXT NOT NULL,
      kpi_name TEXT NOT NULL,
      value REAL NOT NULL,
      previous_value REAL,
      change_percent REAL,
      trend TEXT DEFAULT 'flat',
      unit TEXT DEFAULT '',
      computed_at TEXT DEFAULT (datetime('now')),
      UNIQUE(date, kpi_name)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_kpis_date ON daily_kpis(date DESC, kpi_name)`,

    `CREATE TABLE IF NOT EXISTS anomalies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      metric TEXT NOT NULL,
      source TEXT NOT NULL,
      expected REAL,
      actual REAL,
      deviation REAL,
      severity TEXT DEFAULT 'medium',
      message TEXT,
      resolved INTEGER DEFAULT 0,
      detected_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(detected_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_anomalies_sev ON anomalies(severity, resolved)`,

    `CREATE TABLE IF NOT EXISTS reports (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_type TEXT NOT NULL,
      period TEXT NOT NULL,
      period_start TEXT NOT NULL,
      period_end TEXT NOT NULL,
      data TEXT NOT NULL,
      generated_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_reports_type ON reports(report_type, generated_at DESC)`,

    `CREATE TABLE IF NOT EXISTS funnels (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      funnel_name TEXT NOT NULL,
      step_name TEXT NOT NULL,
      step_order INTEGER NOT NULL,
      count INTEGER DEFAULT 0,
      date TEXT NOT NULL,
      UNIQUE(funnel_name, step_name, date)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_funnels_name ON funnels(funnel_name, date DESC)`,

    `CREATE TABLE IF NOT EXISTS cohorts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cohort_name TEXT NOT NULL,
      cohort_date TEXT NOT NULL,
      period_offset INTEGER NOT NULL,
      metric TEXT NOT NULL,
      value REAL NOT NULL,
      UNIQUE(cohort_name, cohort_date, period_offset, metric)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_cohorts_name ON cohorts(cohort_name, cohort_date DESC)`,

    `CREATE TABLE IF NOT EXISTS forecasts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      metric TEXT NOT NULL,
      current_value REAL,
      projected_value REAL,
      growth_rate REAL,
      confidence REAL,
      horizon TEXT,
      model TEXT DEFAULT 'linear',
      computed_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_forecasts_metric ON forecasts(metric, computed_at DESC)`,

    `CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_type TEXT NOT NULL,
      source TEXT NOT NULL,
      data TEXT DEFAULT '{}',
      recorded_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, recorded_at DESC)`,
  ];

  for (const sql of stmts) {
    try { await db.prepare(sql).run(); } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      if (!msg.includes('already exists')) log('error', 'Schema error', { sql: sql.slice(0, 80), error: msg });
    }
  }
  log('info', 'Schema ensured', { tables: 8 });
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

function today(): string { return new Date().toISOString().split('T')[0]; }
function nowISO(): string { return new Date().toISOString(); }

async function safeFetch(binding: Fetcher, url: string, opts?: RequestInit): Promise<unknown | null> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    const resp = await binding.fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(timer);
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

// ── Data Collection ──────────────────────────────────────────────────────────

async function collectBotMetrics(env: Env): Promise<MetricSnapshot[]> {
  const metrics: MetricSnapshot[] = [];
  const ts = nowISO();

  const botSources: Array<{ name: string; binding: Fetcher; statsPath: string }> = [
    { name: 'x_bot', binding: env.X_BOT, statsPath: 'https://x-bot/stats' },
    { name: 'linkedin', binding: env.LINKEDIN, statsPath: 'https://linkedin/stats' },
    { name: 'telegram', binding: env.TELEGRAM, statsPath: 'https://telegram/stats' },
    { name: 'reddit', binding: env.REDDIT, statsPath: 'https://reddit/stats' },
  ];

  const results = await Promise.allSettled(
    botSources.map(async (bot) => {
      const data = await safeFetch(bot.binding, bot.statsPath) as Record<string, unknown> | null;
      if (!data) return;

      // Extract common bot metrics
      const m = data.messages as Record<string, number> | undefined;
      const p = data.posts as Record<string, number> | undefined;

      if (m) {
        if (m.total_messages != null) metrics.push({ source: bot.name, metric: 'messages_total', value: m.total_messages, unit: 'count', tags: {}, timestamp: ts });
        if (m.incoming != null) metrics.push({ source: bot.name, metric: 'messages_incoming', value: m.incoming, unit: 'count', tags: {}, timestamp: ts });
        if (m.outgoing != null) metrics.push({ source: bot.name, metric: 'messages_outgoing', value: m.outgoing, unit: 'count', tags: {}, timestamp: ts });
        if (m.unique_users != null) metrics.push({ source: bot.name, metric: 'unique_users', value: m.unique_users, unit: 'count', tags: {}, timestamp: ts });
        if (m.avg_latency_ms != null) metrics.push({ source: bot.name, metric: 'avg_latency_ms', value: m.avg_latency_ms, unit: 'ms', tags: {}, timestamp: ts });
      }

      // Post counts
      if (p) {
        const posted = (p as Record<string, unknown>).posted as number | undefined;
        if (posted != null) metrics.push({ source: bot.name, metric: 'posts_count', value: posted, unit: 'count', tags: {}, timestamp: ts });
      }

      // Subscribers
      const subs = data.subscribers as number | undefined;
      if (subs != null) metrics.push({ source: bot.name, metric: 'subscribers', value: subs, unit: 'count', tags: {}, timestamp: ts });

      // Engagement
      const eng = data.engagement as Record<string, number> | undefined;
      if (eng) {
        if (eng.likes != null) metrics.push({ source: bot.name, metric: 'engagement_likes', value: eng.likes, unit: 'count', tags: {}, timestamp: ts });
        if (eng.replies != null) metrics.push({ source: bot.name, metric: 'engagement_replies', value: eng.replies, unit: 'count', tags: {}, timestamp: ts });
        if (eng.shares != null) metrics.push({ source: bot.name, metric: 'engagement_shares', value: eng.shares, unit: 'count', tags: {}, timestamp: ts });
      }
    })
  );

  const failures = results.filter(r => r.status === 'rejected').length;
  if (failures > 0) log('warn', 'Bot metric collection failures', { failures });
  log('info', 'Bot metrics collected', { count: metrics.length });
  return metrics;
}

async function collectEngineMetrics(env: Env): Promise<MetricSnapshot[]> {
  const metrics: MetricSnapshot[] = [];
  const ts = nowISO();

  const data = await safeFetch(env.ENGINE_RUNTIME, 'https://engine/health') as Record<string, unknown> | null;
  if (data) {
    const s = data.stats as Record<string, number> | undefined;
    if (s) {
      if (s.engines != null) metrics.push({ source: 'engine_runtime', metric: 'total_engines', value: s.engines, unit: 'count', tags: {}, timestamp: ts });
      if (s.doctrines != null) metrics.push({ source: 'engine_runtime', metric: 'total_doctrines', value: s.doctrines, unit: 'count', tags: {}, timestamp: ts });
      if (s.queries != null) metrics.push({ source: 'engine_runtime', metric: 'total_queries', value: s.queries, unit: 'count', tags: {}, timestamp: ts });
      if (s.domains != null) metrics.push({ source: 'engine_runtime', metric: 'total_domains', value: s.domains, unit: 'count', tags: {}, timestamp: ts });
    }
  }

  const kfData = await safeFetch(env.KNOWLEDGE_FORGE, 'https://kf/health') as Record<string, unknown> | null;
  if (kfData) {
    const s = kfData.stats as Record<string, number> | undefined;
    if (s) {
      if (s.documents != null) metrics.push({ source: 'knowledge_forge', metric: 'total_documents', value: s.documents, unit: 'count', tags: {}, timestamp: ts });
      if (s.chunks != null) metrics.push({ source: 'knowledge_forge', metric: 'total_chunks', value: s.chunks, unit: 'count', tags: {}, timestamp: ts });
      if (s.categories != null) metrics.push({ source: 'knowledge_forge', metric: 'total_categories', value: s.categories, unit: 'count', tags: {}, timestamp: ts });
    }
  }

  log('info', 'Engine metrics collected', { count: metrics.length });
  return metrics;
}

async function collectFleetMetrics(env: Env): Promise<MetricSnapshot[]> {
  const metrics: MetricSnapshot[] = [];
  const ts = nowISO();

  const data = await safeFetch(env.FLEET_COMMANDER, 'https://fleet/health') as Record<string, unknown> | null;
  if (data) {
    metrics.push({ source: 'fleet', metric: 'fleet_size', value: (data.fleetSize as number) || 0, unit: 'count', tags: {}, timestamp: ts });
    metrics.push({ source: 'fleet', metric: 'critical_workers', value: (data.criticalWorkers as number) || 0, unit: 'count', tags: {}, timestamp: ts });

    const tiers = data.tiers as Record<string, number> | undefined;
    if (tiers) {
      for (const [tier, count] of Object.entries(tiers)) {
        metrics.push({ source: 'fleet', metric: `fleet_tier_${tier.toLowerCase()}`, value: count, unit: 'count', tags: { tier }, timestamp: ts });
      }
    }
  }

  // Fleet scores from last scan
  const scan = await safeFetch(env.FLEET_COMMANDER, 'https://fleet/dashboard') as Record<string, unknown> | null;
  if (scan) {
    const scores = scan.scores as Record<string, unknown> | undefined;
    if (scores) {
      if (scores.fleetScore != null) metrics.push({ source: 'fleet', metric: 'fleet_score', value: scores.fleetScore as number, unit: 'score', tags: {}, timestamp: ts });
      if (scores.healthy != null) metrics.push({ source: 'fleet', metric: 'workers_healthy', value: scores.healthy as number, unit: 'count', tags: {}, timestamp: ts });
      if (scores.down != null) metrics.push({ source: 'fleet', metric: 'workers_down', value: scores.down as number, unit: 'count', tags: {}, timestamp: ts });
    }
  }

  log('info', 'Fleet metrics collected', { count: metrics.length });
  return metrics;
}

async function collectDaemonMetrics(env: Env): Promise<MetricSnapshot[]> {
  const metrics: MetricSnapshot[] = [];
  const ts = nowISO();

  const data = await safeFetch(env.DAEMON, 'https://daemon/health') as Record<string, unknown> | null;
  if (data) {
    metrics.push({ source: 'daemon', metric: 'capabilities', value: (data.capabilities as number) || 0, unit: 'count', tags: {}, timestamp: ts });
    metrics.push({ source: 'daemon', metric: 'version', value: parseFloat(String(data.version || '0').replace(/[^0-9.]/g, '')) || 0, unit: 'version', tags: {}, timestamp: ts });

    const cycles = data.cycles as Record<string, number> | undefined;
    if (cycles) {
      for (const [k, v] of Object.entries(cycles)) {
        metrics.push({ source: 'daemon', metric: `daemon_cycle_${k}`, value: v, unit: 'count', tags: {}, timestamp: ts });
      }
    }
  }

  log('info', 'Daemon metrics collected', { count: metrics.length });
  return metrics;
}

async function collectBrainMetrics(env: Env): Promise<MetricSnapshot[]> {
  const metrics: MetricSnapshot[] = [];
  const ts = nowISO();

  const data = await safeFetch(env.SHARED_BRAIN, 'https://brain/health') as Record<string, unknown> | null;
  if (data) {
    const stats = data.stats as Record<string, number> | undefined;
    if (stats) {
      if (stats.total_messages != null) metrics.push({ source: 'shared_brain', metric: 'brain_messages', value: stats.total_messages, unit: 'count', tags: {}, timestamp: ts });
      if (stats.instances != null) metrics.push({ source: 'shared_brain', metric: 'brain_instances', value: stats.instances, unit: 'count', tags: {}, timestamp: ts });
    }
  }

  // MoltBook metrics from Swarm Brain
  const swarm = await safeFetch(env.SWARM_BRAIN, 'https://swarm/health') as Record<string, unknown> | null;
  if (swarm) {
    const moltbook = swarm.moltbook as Record<string, number> | undefined;
    if (moltbook) {
      if (moltbook.total_posts != null) metrics.push({ source: 'swarm_brain', metric: 'moltbook_posts', value: moltbook.total_posts, unit: 'count', tags: {}, timestamp: ts });
    }
  }

  log('info', 'Brain metrics collected', { count: metrics.length });
  return metrics;
}

// ── Store Metrics ────────────────────────────────────────────────────────────

async function storeMetrics(db: D1Database, metrics: MetricSnapshot[]): Promise<number> {
  if (metrics.length === 0) return 0;

  // Batch insert in chunks of 50
  const chunkSize = 50;
  let stored = 0;

  for (let i = 0; i < metrics.length; i += chunkSize) {
    const chunk = metrics.slice(i, i + chunkSize);
    const stmts = chunk.map(m =>
      db.prepare(
        `INSERT INTO metrics (source, metric, value, unit, tags, recorded_at) VALUES (?, ?, ?, ?, ?, ?)`
      ).bind(m.source, m.metric, m.value, m.unit, JSON.stringify(m.tags), m.timestamp)
    );
    try {
      await db.batch(stmts);
      stored += chunk.length;
    } catch (e: unknown) {
      log('error', 'Failed to store metrics batch', { error: e instanceof Error ? e.message : String(e), batchStart: i });
    }
  }

  log('info', 'Metrics stored', { total: stored });
  return stored;
}

// ── KPI Computation ──────────────────────────────────────────────────────────

async function computeDailyKPIs(db: D1Database, env: Env): Promise<KPI[]> {
  const d = today();
  const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
  const kpis: KPI[] = [];

  // Define KPI queries — each computes today's value and compares to yesterday
  const kpiDefs: Array<{ name: string; metric: string; source?: string; unit: string }> = [
    { name: 'total_bot_messages', metric: 'messages_total', unit: 'count' },
    { name: 'total_bot_posts', metric: 'posts_count', unit: 'count' },
    { name: 'unique_bot_users', metric: 'unique_users', unit: 'count' },
    { name: 'total_engine_queries', metric: 'total_queries', source: 'engine_runtime', unit: 'count' },
    { name: 'engine_count', metric: 'total_engines', source: 'engine_runtime', unit: 'count' },
    { name: 'doctrine_count', metric: 'total_doctrines', source: 'engine_runtime', unit: 'count' },
    { name: 'knowledge_documents', metric: 'total_documents', source: 'knowledge_forge', unit: 'count' },
    { name: 'knowledge_chunks', metric: 'total_chunks', source: 'knowledge_forge', unit: 'count' },
    { name: 'fleet_score', metric: 'fleet_score', source: 'fleet', unit: 'score' },
    { name: 'workers_healthy', metric: 'workers_healthy', source: 'fleet', unit: 'count' },
    { name: 'brain_messages', metric: 'brain_messages', source: 'shared_brain', unit: 'count' },
    { name: 'avg_bot_latency', metric: 'avg_latency_ms', unit: 'ms' },
    { name: 'total_engagement_likes', metric: 'engagement_likes', unit: 'count' },
    { name: 'total_subscribers', metric: 'subscribers', unit: 'count' },
    { name: 'moltbook_posts', metric: 'moltbook_posts', source: 'swarm_brain', unit: 'count' },
  ];

  for (const kpiDef of kpiDefs) {
    try {
      // Get latest value for today
      let query = `SELECT value FROM metrics WHERE metric = ? AND recorded_at >= ? ORDER BY recorded_at DESC LIMIT 1`;
      const params: unknown[] = [kpiDef.metric, `${d}T00:00:00`];
      if (kpiDef.source) {
        query = `SELECT value FROM metrics WHERE metric = ? AND source = ? AND recorded_at >= ? ORDER BY recorded_at DESC LIMIT 1`;
        params.splice(1, 0, kpiDef.source);
      }

      const todayRow = await db.prepare(query).bind(...params).first<{ value: number }>();

      // Get yesterday's latest value
      let yQuery = `SELECT value FROM metrics WHERE metric = ? AND recorded_at >= ? AND recorded_at < ? ORDER BY recorded_at DESC LIMIT 1`;
      const yParams: unknown[] = [kpiDef.metric, `${yesterday}T00:00:00`, `${d}T00:00:00`];
      if (kpiDef.source) {
        yQuery = `SELECT value FROM metrics WHERE metric = ? AND source = ? AND recorded_at >= ? AND recorded_at < ? ORDER BY recorded_at DESC LIMIT 1`;
        yParams.splice(1, 0, kpiDef.source);
      }

      const yRow = await db.prepare(yQuery).bind(...yParams).first<{ value: number }>();

      const currentVal = todayRow?.value ?? 0;
      const prevVal = yRow?.value ?? 0;
      const change = prevVal > 0 ? ((currentVal - prevVal) / prevVal) * 100 : 0;
      const trend: 'up' | 'down' | 'flat' = change > 1 ? 'up' : change < -1 ? 'down' : 'flat';

      kpis.push({
        name: kpiDef.name,
        value: currentVal,
        unit: kpiDef.unit,
        trend,
        changePercent: Math.round(change * 100) / 100,
        period: d,
      });

      // Upsert to daily_kpis
      await db.prepare(
        `INSERT INTO daily_kpis (date, kpi_name, value, previous_value, change_percent, trend, unit)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(date, kpi_name) DO UPDATE SET
           value = excluded.value, previous_value = excluded.previous_value,
           change_percent = excluded.change_percent, trend = excluded.trend,
           computed_at = datetime('now')`
      ).bind(d, kpiDef.name, currentVal, prevVal, change, trend, kpiDef.unit).run();

    } catch (e: unknown) {
      log('error', 'KPI computation error', { kpi: kpiDef.name, error: e instanceof Error ? e.message : String(e) });
    }
  }

  log('info', 'Daily KPIs computed', { count: kpis.length, date: d });
  return kpis;
}

// ── Anomaly Detection ────────────────────────────────────────────────────────

async function detectAnomalies(db: D1Database): Promise<AnomalyAlert[]> {
  const alerts: AnomalyAlert[] = [];

  // Check each key metric against its rolling 7-day average + 2 stddev
  const keyMetrics = [
    { metric: 'fleet_score', source: 'fleet', threshold: 2 },
    { metric: 'avg_latency_ms', source: '%', threshold: 2 },
    { metric: 'messages_total', source: '%', threshold: 2.5 },
    { metric: 'total_queries', source: 'engine_runtime', threshold: 2 },
  ];

  for (const km of keyMetrics) {
    try {
      const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString();

      const stats = await db.prepare(
        `SELECT AVG(value) as avg_val,
                AVG(value * value) - AVG(value) * AVG(value) as variance
         FROM metrics
         WHERE metric = ? AND source LIKE ? AND recorded_at >= ?`
      ).bind(km.metric, km.source, sevenDaysAgo).first<{ avg_val: number; variance: number }>();

      if (!stats || stats.avg_val == null) continue;

      const mean = stats.avg_val;
      const stddev = Math.sqrt(Math.max(0, stats.variance || 0));
      if (stddev === 0) continue;

      // Get latest value
      const latest = await db.prepare(
        `SELECT value FROM metrics WHERE metric = ? AND source LIKE ? ORDER BY recorded_at DESC LIMIT 1`
      ).bind(km.metric, km.source).first<{ value: number }>();

      if (!latest) continue;

      const deviation = Math.abs(latest.value - mean) / stddev;
      if (deviation > km.threshold) {
        const severity: AnomalyAlert['severity'] = deviation > 4 ? 'critical' : deviation > 3 ? 'high' : deviation > 2 ? 'medium' : 'low';
        const direction = latest.value > mean ? 'above' : 'below';

        const alert: AnomalyAlert = {
          metric: km.metric,
          source: km.source,
          expected: Math.round(mean * 100) / 100,
          actual: latest.value,
          severity,
          message: `${km.metric} is ${deviation.toFixed(1)} stddev ${direction} 7-day avg (${mean.toFixed(1)} vs ${latest.value})`,
        };
        alerts.push(alert);

        await db.prepare(
          `INSERT INTO anomalies (metric, source, expected, actual, deviation, severity, message) VALUES (?, ?, ?, ?, ?, ?, ?)`
        ).bind(km.metric, km.source, mean, latest.value, deviation, severity, alert.message).run();
      }
    } catch (e: unknown) {
      log('error', 'Anomaly detection error', { metric: km.metric, error: e instanceof Error ? e.message : String(e) });
    }
  }

  if (alerts.length > 0) log('warn', 'Anomalies detected', { count: alerts.length, severities: alerts.map(a => a.severity) });
  return alerts;
}

// ── Trend Analysis ───────────────────────────────────────────────────────────

async function computeTrends(db: D1Database, metric: string, source: string, days: number = 30): Promise<TrendPoint[]> {
  const since = new Date(Date.now() - days * 86400000).toISOString();

  const rows = await db.prepare(
    `SELECT DATE(recorded_at) as date, AVG(value) as avg_value
     FROM metrics
     WHERE metric = ? AND source LIKE ? AND recorded_at >= ?
     GROUP BY DATE(recorded_at)
     ORDER BY date ASC`
  ).bind(metric, source, since).all<{ date: string; avg_value: number }>();

  return (rows.results || []).map(r => ({ date: r.date, value: Math.round(r.avg_value * 100) / 100 }));
}

// ── Growth Forecasting ───────────────────────────────────────────────────────

async function computeForecasts(db: D1Database): Promise<GrowthForecast[]> {
  const forecasts: GrowthForecast[] = [];
  const forecastMetrics = [
    { metric: 'total_engines', source: 'engine_runtime', horizon: '30d' },
    { metric: 'total_doctrines', source: 'engine_runtime', horizon: '30d' },
    { metric: 'brain_messages', source: 'shared_brain', horizon: '30d' },
    { metric: 'total_documents', source: 'knowledge_forge', horizon: '30d' },
    { metric: 'posts_count', source: '%', horizon: '7d' },
    { metric: 'subscribers', source: '%', horizon: '30d' },
  ];

  for (const fm of forecastMetrics) {
    try {
      const trend = await computeTrends(db, fm.metric, fm.source, 14);
      if (trend.length < 3) continue;

      // Simple linear regression
      const n = trend.length;
      const xs = trend.map((_, i) => i);
      const ys = trend.map(t => t.value);
      const sumX = xs.reduce((a, b) => a + b, 0);
      const sumY = ys.reduce((a, b) => a + b, 0);
      const sumXY = xs.reduce((acc, x, i) => acc + x * ys[i], 0);
      const sumXX = xs.reduce((acc, x) => acc + x * x, 0);

      const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
      const intercept = (sumY - slope * sumX) / n;

      const currentValue = ys[n - 1];
      const horizonDays = parseInt(fm.horizon) || 30;
      const projectedValue = Math.max(0, intercept + slope * (n + horizonDays - 1));
      const growthRate = currentValue > 0 ? ((projectedValue - currentValue) / currentValue) * 100 : 0;

      // R-squared for confidence
      const yMean = sumY / n;
      const ssRes = ys.reduce((acc, y, i) => acc + Math.pow(y - (intercept + slope * xs[i]), 2), 0);
      const ssTot = ys.reduce((acc, y) => acc + Math.pow(y - yMean, 2), 0);
      const rSquared = ssTot > 0 ? 1 - ssRes / ssTot : 0;

      const forecast: GrowthForecast = {
        metric: fm.metric,
        currentValue: Math.round(currentValue * 100) / 100,
        projectedValue: Math.round(projectedValue * 100) / 100,
        growthRate: Math.round(growthRate * 100) / 100,
        confidence: Math.round(Math.max(0, Math.min(1, rSquared)) * 100) / 100,
        horizon: fm.horizon,
      };
      forecasts.push(forecast);

      await db.prepare(
        `INSERT INTO forecasts (metric, current_value, projected_value, growth_rate, confidence, horizon, model)
         VALUES (?, ?, ?, ?, ?, ?, 'linear')`
      ).bind(fm.metric, forecast.currentValue, forecast.projectedValue, forecast.growthRate, forecast.confidence, fm.horizon).run();

    } catch (e: unknown) {
      log('error', 'Forecast error', { metric: fm.metric, error: e instanceof Error ? e.message : String(e) });
    }
  }

  log('info', 'Forecasts computed', { count: forecasts.length });
  return forecasts;
}

// ── Report Generation ────────────────────────────────────────────────────────

async function generateDailyReport(db: D1Database, env: Env): Promise<Record<string, unknown>> {
  const d = today();
  const kpis = await computeDailyKPIs(db, env);
  const anomalies = await detectAnomalies(db);
  const forecasts = await computeForecasts(db);

  // Bot platform breakdown
  const botPlatforms = ['x_bot', 'linkedin', 'telegram', 'reddit'];
  const platformStats: Record<string, Record<string, number>> = {};

  for (const platform of botPlatforms) {
    const msgs = await db.prepare(
      `SELECT value FROM metrics WHERE source = ? AND metric = 'messages_total' AND recorded_at >= ? ORDER BY recorded_at DESC LIMIT 1`
    ).bind(platform, `${d}T00:00:00`).first<{ value: number }>();

    const posts = await db.prepare(
      `SELECT value FROM metrics WHERE source = ? AND metric = 'posts_count' AND recorded_at >= ? ORDER BY recorded_at DESC LIMIT 1`
    ).bind(platform, `${d}T00:00:00`).first<{ value: number }>();

    platformStats[platform] = {
      messages: msgs?.value ?? 0,
      posts: posts?.value ?? 0,
    };
  }

  // Metric totals for the day
  const dayMetrics = await db.prepare(
    `SELECT COUNT(*) as metric_count, COUNT(DISTINCT source) as sources, COUNT(DISTINCT metric) as unique_metrics
     FROM metrics WHERE recorded_at >= ?`
  ).bind(`${d}T00:00:00`).first<{ metric_count: number; sources: number; unique_metrics: number }>();

  const report = {
    type: 'daily',
    date: d,
    generated_at: nowISO(),
    kpis,
    anomalies,
    forecasts,
    platform_breakdown: platformStats,
    collection_stats: dayMetrics,
    highlights: {
      trending_up: kpis.filter(k => k.trend === 'up').map(k => k.name),
      trending_down: kpis.filter(k => k.trend === 'down').map(k => k.name),
      critical_anomalies: anomalies.filter(a => a.severity === 'critical' || a.severity === 'high'),
      high_confidence_forecasts: forecasts.filter(f => f.confidence > 0.7),
    },
  };

  // Store report
  await db.prepare(
    `INSERT INTO reports (report_type, period, period_start, period_end, data) VALUES (?, ?, ?, ?, ?)`
  ).bind('daily', d, `${d}T00:00:00`, `${d}T23:59:59`, JSON.stringify(report)).run();

  // Cache for fast retrieval
  await env.CACHE.put('report:daily:latest', JSON.stringify(report), { expirationTtl: 86400 });

  log('info', 'Daily report generated', { kpis: kpis.length, anomalies: anomalies.length, forecasts: forecasts.length });
  return report;
}

async function generateWeeklyReport(db: D1Database, env: Env): Promise<Record<string, unknown>> {
  const now = new Date();
  const weekStart = new Date(now.getTime() - 7 * 86400000).toISOString().split('T')[0];
  const weekEnd = today();

  // Week-over-week KPI comparison
  const thisWeekKPIs = await db.prepare(
    `SELECT kpi_name, AVG(value) as avg_value, MAX(value) as max_value, MIN(value) as min_value
     FROM daily_kpis WHERE date >= ? AND date <= ? GROUP BY kpi_name`
  ).bind(weekStart, weekEnd).all<{ kpi_name: string; avg_value: number; max_value: number; min_value: number }>();

  const prevWeekStart = new Date(now.getTime() - 14 * 86400000).toISOString().split('T')[0];
  const prevWeekKPIs = await db.prepare(
    `SELECT kpi_name, AVG(value) as avg_value
     FROM daily_kpis WHERE date >= ? AND date < ? GROUP BY kpi_name`
  ).bind(prevWeekStart, weekStart).all<{ kpi_name: string; avg_value: number }>();

  const prevMap = new Map((prevWeekKPIs.results || []).map(r => [r.kpi_name, r.avg_value]));

  const weeklyKPIs = (thisWeekKPIs.results || []).map(kpi => {
    const prev = prevMap.get(kpi.kpi_name) || 0;
    const change = prev > 0 ? ((kpi.avg_value - prev) / prev) * 100 : 0;
    return {
      name: kpi.kpi_name,
      avg: Math.round(kpi.avg_value * 100) / 100,
      max: Math.round(kpi.max_value * 100) / 100,
      min: Math.round(kpi.min_value * 100) / 100,
      wow_change: Math.round(change * 100) / 100,
    };
  });

  // Week's anomalies
  const weekAnomalies = await db.prepare(
    `SELECT severity, COUNT(*) as count FROM anomalies WHERE detected_at >= ? GROUP BY severity ORDER BY count DESC`
  ).bind(`${weekStart}T00:00:00`).all<{ severity: string; count: number }>();

  // Top events
  const topEvents = await db.prepare(
    `SELECT event_type, source, COUNT(*) as count FROM events WHERE recorded_at >= ? GROUP BY event_type, source ORDER BY count DESC LIMIT 20`
  ).bind(`${weekStart}T00:00:00`).all<{ event_type: string; source: string; count: number }>();

  const report = {
    type: 'weekly',
    period: `${weekStart} to ${weekEnd}`,
    period_start: weekStart,
    period_end: weekEnd,
    generated_at: nowISO(),
    kpis: weeklyKPIs,
    anomaly_summary: weekAnomalies.results || [],
    top_events: topEvents.results || [],
    recommendations: generateRecommendations(weeklyKPIs),
  };

  await db.prepare(
    `INSERT INTO reports (report_type, period, period_start, period_end, data) VALUES (?, ?, ?, ?, ?)`
  ).bind('weekly', `${weekStart}_${weekEnd}`, weekStart, weekEnd, JSON.stringify(report)).run();

  await env.CACHE.put('report:weekly:latest', JSON.stringify(report), { expirationTtl: 604800 });

  log('info', 'Weekly report generated', { period: `${weekStart} to ${weekEnd}` });
  return report;
}

function generateRecommendations(kpis: Array<{ name: string; wow_change: number }>): string[] {
  const recs: string[] = [];

  for (const kpi of kpis) {
    if (kpi.name === 'fleet_score' && kpi.wow_change < -5) {
      recs.push('Fleet health declining — investigate failing workers and dependency issues');
    }
    if (kpi.name === 'total_bot_messages' && kpi.wow_change > 20) {
      recs.push('Bot engagement surging — consider expanding content categories and posting frequency');
    }
    if (kpi.name === 'total_bot_messages' && kpi.wow_change < -20) {
      recs.push('Bot engagement dropping — review content quality, check for API/posting issues');
    }
    if (kpi.name === 'avg_bot_latency' && kpi.wow_change > 30) {
      recs.push('Bot response latency increasing — check AI provider status and consider caching');
    }
    if (kpi.name === 'engine_count' && kpi.wow_change > 0) {
      recs.push(`Engine fleet growing at ${kpi.wow_change.toFixed(1)}% WoW — monitor doctrine quality`);
    }
    if (kpi.name === 'total_subscribers' && kpi.wow_change > 10) {
      recs.push('Subscriber growth accelerating — optimize onboarding and retention flows');
    }
  }

  if (recs.length === 0) recs.push('All KPIs within normal range — continue current operations');
  return recs;
}

// ── Funnel Analysis ──────────────────────────────────────────────────────────

async function recordFunnelStep(db: D1Database, funnelName: string, stepName: string, stepOrder: number, count: number): Promise<void> {
  const d = today();
  await db.prepare(
    `INSERT INTO funnels (funnel_name, step_name, step_order, count, date)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(funnel_name, step_name, date) DO UPDATE SET count = excluded.count`
  ).bind(funnelName, stepName, stepOrder, count, d).run();
}

async function getFunnelData(db: D1Database, funnelName: string, days: number = 7): Promise<Record<string, unknown>> {
  const since = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];

  const steps = await db.prepare(
    `SELECT step_name, step_order, SUM(count) as total
     FROM funnels WHERE funnel_name = ? AND date >= ?
     GROUP BY step_name, step_order ORDER BY step_order ASC`
  ).bind(funnelName, since).all<{ step_name: string; step_order: number; total: number }>();

  const funnelSteps = steps.results || [];
  const conversions: Array<{ from: string; to: string; rate: number }> = [];

  for (let i = 1; i < funnelSteps.length; i++) {
    const prev = funnelSteps[i - 1];
    const curr = funnelSteps[i];
    conversions.push({
      from: prev.step_name,
      to: curr.step_name,
      rate: prev.total > 0 ? Math.round((curr.total / prev.total) * 10000) / 100 : 0,
    });
  }

  return {
    funnel: funnelName,
    period: `${since} to ${today()}`,
    steps: funnelSteps,
    conversions,
    overall_conversion: funnelSteps.length >= 2 && funnelSteps[0].total > 0
      ? Math.round((funnelSteps[funnelSteps.length - 1].total / funnelSteps[0].total) * 10000) / 100
      : 0,
  };
}

// ── Revenue Intelligence ─────────────────────────────────────────────────────

async function computeRevenueSignals(db: D1Database, env: Env): Promise<Record<string, unknown>> {
  // Aggregate signals that indicate revenue potential
  const d = today();

  // Bot engagement = audience building
  const botMessages = await db.prepare(
    `SELECT SUM(value) as total FROM metrics WHERE metric = 'messages_total' AND recorded_at >= ?`
  ).bind(`${d}T00:00:00`).first<{ total: number }>();

  // Engine queries = product usage
  const engineQueries = await db.prepare(
    `SELECT value FROM metrics WHERE metric = 'total_queries' AND source = 'engine_runtime' ORDER BY recorded_at DESC LIMIT 1`
  ).first<{ value: number }>();

  // Knowledge docs = content depth
  const knowledgeDocs = await db.prepare(
    `SELECT value FROM metrics WHERE metric = 'total_documents' AND source = 'knowledge_forge' ORDER BY recorded_at DESC LIMIT 1`
  ).first<{ value: number }>();

  // Subscriber growth
  const subscribers = await db.prepare(
    `SELECT SUM(value) as total FROM metrics WHERE metric = 'subscribers' AND recorded_at >= ?`
  ).bind(`${d}T00:00:00`).first<{ total: number }>();

  // Revenue readiness score (0-100)
  const scores = {
    audience: Math.min(100, ((botMessages?.total || 0) / 1000) * 100),
    product: Math.min(100, ((engineQueries?.value || 0) / 500) * 100),
    content: Math.min(100, ((knowledgeDocs?.value || 0) / 5000) * 100),
    subscribers: Math.min(100, ((subscribers?.total || 0) / 100) * 100),
  };

  const overallScore = Math.round(
    (scores.audience * 0.3 + scores.product * 0.3 + scores.content * 0.2 + scores.subscribers * 0.2)
  );

  return {
    date: d,
    revenue_readiness_score: overallScore,
    components: scores,
    signals: {
      bot_messages_today: botMessages?.total || 0,
      engine_queries: engineQueries?.value || 0,
      knowledge_docs: knowledgeDocs?.value || 0,
      subscribers: subscribers?.total || 0,
    },
    recommendations: overallScore < 30
      ? ['Focus on audience growth — increase bot posting frequency and engagement']
      : overallScore < 60
        ? ['Audience building. Consider launching paid tiers for high-value engine queries']
        : ['Revenue-ready. Activate Stripe live mode and launch marketing campaigns'],
  };
}

// ── Full Collection Cycle ────────────────────────────────────────────────────

async function runCollectionCycle(env: Env): Promise<Record<string, unknown>> {
  await ensureSchema(env.DB);

  // Collect from all sources in parallel
  const [botMetrics, engineMetrics, fleetMetrics, daemonMetrics, brainMetrics] = await Promise.allSettled([
    collectBotMetrics(env),
    collectEngineMetrics(env),
    collectFleetMetrics(env),
    collectDaemonMetrics(env),
    collectBrainMetrics(env),
  ]);

  const allMetrics: MetricSnapshot[] = [
    ...(botMetrics.status === 'fulfilled' ? botMetrics.value : []),
    ...(engineMetrics.status === 'fulfilled' ? engineMetrics.value : []),
    ...(fleetMetrics.status === 'fulfilled' ? fleetMetrics.value : []),
    ...(daemonMetrics.status === 'fulfilled' ? daemonMetrics.value : []),
    ...(brainMetrics.status === 'fulfilled' ? brainMetrics.value : []),
  ];

  const stored = await storeMetrics(env.DB, allMetrics);

  // Record visitor funnel steps from bot data
  const totalMessages = allMetrics.filter(m => m.metric === 'messages_total').reduce((sum, m) => sum + m.value, 0);
  const totalPosts = allMetrics.filter(m => m.metric === 'posts_count').reduce((sum, m) => sum + m.value, 0);
  const totalSubs = allMetrics.filter(m => m.metric === 'subscribers').reduce((sum, m) => sum + m.value, 0);

  await recordFunnelStep(env.DB, 'audience', 'posts_published', 1, totalPosts);
  await recordFunnelStep(env.DB, 'audience', 'messages_received', 2, totalMessages);
  await recordFunnelStep(env.DB, 'audience', 'subscribers', 3, totalSubs);

  // Cache summary
  const summary = {
    timestamp: nowISO(),
    metrics_collected: allMetrics.length,
    metrics_stored: stored,
    sources: {
      bots: botMetrics.status === 'fulfilled' ? botMetrics.value.length : 0,
      engines: engineMetrics.status === 'fulfilled' ? engineMetrics.value.length : 0,
      fleet: fleetMetrics.status === 'fulfilled' ? fleetMetrics.value.length : 0,
      daemon: daemonMetrics.status === 'fulfilled' ? daemonMetrics.value.length : 0,
      brain: brainMetrics.status === 'fulfilled' ? brainMetrics.value.length : 0,
    },
    failures: [botMetrics, engineMetrics, fleetMetrics, daemonMetrics, brainMetrics]
      .filter(r => r.status === 'rejected').length,
  };

  await env.CACHE.put('collection:latest', JSON.stringify(summary), { expirationTtl: 900 });
  log('info', 'Collection cycle complete', summary);
  return summary;
}

// ── Cron Handler ─────────────────────────────────────────────────────────────

async function handleScheduled(event: ScheduledEvent, env: Env): Promise<void> {
  const dt = new Date(event.scheduledTime);
  const hour = dt.getUTCHours();
  const minute = dt.getUTCMinutes();
  const day = dt.getUTCDay();
  const date = dt.getUTCDate();

  await ensureSchema(env.DB);

  // Every 15 min — real-time collection
  if (minute % 15 === 0) {
    await runCollectionCycle(env);
  }

  // Hourly — full aggregation + KPIs
  if (minute === 0) {
    await runCollectionCycle(env);
    await computeDailyKPIs(env.DB, env);
    await detectAnomalies(env.DB);
  }

  // Every 6h — trend computation + forecasts
  if (hour % 6 === 0 && minute === 0) {
    await computeForecasts(env.DB);
    await computeRevenueSignals(env.DB, env);
  }

  // Daily 8am UTC — daily report
  if (hour === 8 && minute === 0) {
    const report = await generateDailyReport(env.DB, env);

    // Post summary to MoltBook
    try {
      const kpiUp = (report.highlights as Record<string, unknown[]>).trending_up?.length || 0;
      const kpiDown = (report.highlights as Record<string, unknown[]>).trending_down?.length || 0;
      const anomCount = ((report.anomalies || []) as unknown[]).length;

      await env.SWARM_BRAIN.fetch('https://swarm/moltbook/post', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          author_id: 'analytics-pipeline',
          author_name: 'Analytics Pipeline',
          author_type: 'agent',
          content: `DAILY ANALYTICS REPORT (${today()}): ${kpiUp} KPIs trending up, ${kpiDown} down, ${anomCount} anomalies detected. Fleet score: ${((report.kpis as KPI[])?.find(k => k.name === 'fleet_score')?.value || 'N/A')}/100.`,
          mood: anomCount > 0 ? 'alert' : 'reporting',
          tags: ['analytics', 'daily-report', 'kpi'],
        }),
      });
    } catch { /* non-critical */ }
  }

  // Weekly Monday 7am UTC — weekly report
  if (day === 1 && hour === 7 && minute === 0) {
    await generateWeeklyReport(env.DB, env);
  }

  // Monthly 1st 6am UTC — monthly aggregate
  if (date === 1 && hour === 6 && minute === 0) {
    // Generate monthly summary from daily KPIs
    const monthStart = new Date(dt.getFullYear(), dt.getMonth() - 1, 1).toISOString().split('T')[0];
    const monthEnd = new Date(dt.getFullYear(), dt.getMonth(), 0).toISOString().split('T')[0];

    const monthlyKPIs = await env.DB.prepare(
      `SELECT kpi_name, AVG(value) as avg_value, MAX(value) as peak, MIN(value) as trough, COUNT(*) as days
       FROM daily_kpis WHERE date >= ? AND date <= ? GROUP BY kpi_name`
    ).bind(monthStart, monthEnd).all();

    const monthlyAnomalies = await env.DB.prepare(
      `SELECT severity, COUNT(*) as count FROM anomalies WHERE detected_at >= ? AND detected_at <= ? GROUP BY severity`
    ).bind(`${monthStart}T00:00:00`, `${monthEnd}T23:59:59`).all();

    const report = {
      type: 'monthly',
      period: `${monthStart} to ${monthEnd}`,
      kpis: monthlyKPIs.results,
      anomaly_summary: monthlyAnomalies.results,
      generated_at: nowISO(),
    };

    await env.DB.prepare(
      `INSERT INTO reports (report_type, period, period_start, period_end, data) VALUES (?, ?, ?, ?, ?)`
    ).bind('monthly', `${monthStart}_${monthEnd}`, monthStart, monthEnd, JSON.stringify(report)).run();

    await env.CACHE.put('report:monthly:latest', JSON.stringify(report), { expirationTtl: 2592000 });
  }

  // Prune old metrics (keep 90 days)
  if (hour === 3 && minute === 0) {
    const cutoff = new Date(Date.now() - 90 * 86400000).toISOString();
    await env.DB.prepare(`DELETE FROM metrics WHERE recorded_at < ?`).bind(cutoff).run();
    await env.DB.prepare(`DELETE FROM events WHERE recorded_at < ?`).bind(cutoff).run();
    log('info', 'Pruned old data', { cutoff });
  }
}

// ── HTTP Handler ─────────────────────────────────────────────────────────────

async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-Echo-API-Key',
      },
    });
  }

  // Health — no auth required
    if (path === '/') return json({ service: 'echo-analytics-pipeline', status: 'operational' });
if (path === '/health') {
    const lastCollection = await env.CACHE.get('collection:latest', 'json') as Record<string, unknown> | null;
    return json({
      status: 'ok',
      service: 'echo-analytics-pipeline',
      version: '1.0.0',
      timestamp: nowISO(),
      lastCollection: lastCollection?.timestamp || 'never',
      metricsCollected: lastCollection?.metrics_collected || 0,
      endpoints: 18,
    });
  }

  // Auth check for all other endpoints
  const apiKey = request.headers.get('X-Echo-API-Key');
  if (apiKey !== env.ECHO_API_KEY) {
    return json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  switch (path) {
    // ── Data Collection ──
    case '/collect': {
      const result = await runCollectionCycle(env);
      return json(result);
    }

    // ── KPIs ──
    case '/kpis': {
      const date = url.searchParams.get('date') || today();
      const rows = await env.DB.prepare(
        `SELECT * FROM daily_kpis WHERE date = ? ORDER BY kpi_name ASC`
      ).bind(date).all();
      return json({ date, kpis: rows.results });
    }

    case '/kpis/history': {
      const kpiName = url.searchParams.get('name');
      const days = parseInt(url.searchParams.get('days') || '30');
      if (!kpiName) return json({ error: 'name parameter required' }, 400);

      const since = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
      const rows = await env.DB.prepare(
        `SELECT date, value, change_percent, trend FROM daily_kpis WHERE kpi_name = ? AND date >= ? ORDER BY date ASC`
      ).bind(kpiName, since).all();
      return json({ kpi: kpiName, days, history: rows.results });
    }

    // ── Metrics ──
    case '/metrics': {
      const source = url.searchParams.get('source');
      const metric = url.searchParams.get('metric');
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 1000);

      let query = 'SELECT * FROM metrics';
      const conditions: string[] = [];
      const params: unknown[] = [];

      if (source) { conditions.push('source = ?'); params.push(source); }
      if (metric) { conditions.push('metric = ?'); params.push(metric); }
      if (conditions.length) query += ' WHERE ' + conditions.join(' AND ');
      query += ` ORDER BY recorded_at DESC LIMIT ?`;
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all();
      return json({ count: rows.results?.length || 0, metrics: rows.results });
    }

    case '/metrics/latest': {
      // Latest value for each unique source+metric combo
      const rows = await env.DB.prepare(
        `SELECT source, metric, value, unit, recorded_at
         FROM metrics m1
         WHERE recorded_at = (SELECT MAX(recorded_at) FROM metrics m2 WHERE m2.source = m1.source AND m2.metric = m1.metric)
         ORDER BY source, metric`
      ).all();
      return json({ count: rows.results?.length || 0, metrics: rows.results });
    }

    // ── Trends ──
    case '/trends': {
      const metric = url.searchParams.get('metric');
      const source = url.searchParams.get('source') || '%';
      const days = parseInt(url.searchParams.get('days') || '30');
      if (!metric) return json({ error: 'metric parameter required' }, 400);

      const trend = await computeTrends(env.DB, metric, source, days);
      return json({ metric, source, days, dataPoints: trend.length, trend });
    }

    // ── Anomalies ──
    case '/anomalies': {
      const severity = url.searchParams.get('severity');
      const resolved = url.searchParams.get('resolved');
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 500);

      let query = 'SELECT * FROM anomalies';
      const conditions: string[] = [];
      const params: unknown[] = [];

      if (severity) { conditions.push('severity = ?'); params.push(severity); }
      if (resolved != null) { conditions.push('resolved = ?'); params.push(resolved === 'true' ? 1 : 0); }
      if (conditions.length) query += ' WHERE ' + conditions.join(' AND ');
      query += ` ORDER BY detected_at DESC LIMIT ?`;
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all();
      return json({ count: rows.results?.length || 0, anomalies: rows.results });
    }

    // ── Forecasts ──
    case '/forecasts': {
      const cached = await env.CACHE.get('forecasts:latest', 'json');
      if (cached) return json(cached);

      const forecasts = await computeForecasts(env.DB);
      const result = { computed_at: nowISO(), forecasts };
      await env.CACHE.put('forecasts:latest', JSON.stringify(result), { expirationTtl: 21600 });
      return json(result);
    }

    // ── Reports ──
    case '/reports/daily': {
      const cached = await env.CACHE.get('report:daily:latest', 'json');
      if (cached) return json(cached);

      const report = await generateDailyReport(env.DB, env);
      return json(report);
    }

    case '/reports/weekly': {
      const cached = await env.CACHE.get('report:weekly:latest', 'json');
      if (cached) return json(cached);

      const report = await generateWeeklyReport(env.DB, env);
      return json(report);
    }

    case '/reports/history': {
      const type = url.searchParams.get('type') || 'daily';
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '10'), 100);

      const rows = await env.DB.prepare(
        `SELECT id, report_type, period, period_start, period_end, generated_at FROM reports WHERE report_type = ? ORDER BY generated_at DESC LIMIT ?`
      ).bind(type, limit).all();
      return json({ type, reports: rows.results });
    }

    case '/reports/detail': {
      const id = url.searchParams.get('id');
      if (!id) return json({ error: 'id parameter required' }, 400);

      const row = await env.DB.prepare(`SELECT * FROM reports WHERE id = ?`).bind(id).first();
      if (!row) return json({ error: 'Report not found' }, 404);
      return json({ ...row, data: JSON.parse(row.data as string) });
    }

    // ── Revenue Intelligence ──
    case '/revenue': {
      const cached = await env.CACHE.get('revenue:latest', 'json');
      if (cached) return json(cached);

      const revenue = await computeRevenueSignals(env.DB, env);
      await env.CACHE.put('revenue:latest', JSON.stringify(revenue), { expirationTtl: 3600 });
      return json(revenue);
    }

    // ── Funnels ──
    case '/funnels': {
      const name = url.searchParams.get('name') || 'audience';
      const days = parseInt(url.searchParams.get('days') || '7');
      const funnel = await getFunnelData(env.DB, name, days);
      return json(funnel);
    }

    // ── Dashboard ──
    case '/dashboard': {
      const cached = await env.CACHE.get('dashboard:latest', 'json');
      if (cached) return json(cached);

      const [kpiRows, anomalyRows, latestReport, revenue, funnel, lastCollect] = await Promise.allSettled([
        env.DB.prepare(`SELECT * FROM daily_kpis WHERE date = ? ORDER BY kpi_name`).bind(today()).all(),
        env.DB.prepare(`SELECT * FROM anomalies WHERE resolved = 0 ORDER BY detected_at DESC LIMIT 10`).all(),
        env.DB.prepare(`SELECT * FROM reports WHERE report_type = 'daily' ORDER BY generated_at DESC LIMIT 1`).first(),
        computeRevenueSignals(env.DB, env),
        getFunnelData(env.DB, 'audience', 7),
        env.CACHE.get('collection:latest', 'json'),
      ]);

      const dashboard = {
        timestamp: nowISO(),
        kpis: kpiRows.status === 'fulfilled' ? kpiRows.value.results : [],
        active_anomalies: anomalyRows.status === 'fulfilled' ? anomalyRows.value.results : [],
        latest_report_date: latestReport.status === 'fulfilled' && latestReport.value ? (latestReport.value as Record<string, string>).period : 'none',
        revenue: revenue.status === 'fulfilled' ? revenue.value : null,
        audience_funnel: funnel.status === 'fulfilled' ? funnel.value : null,
        last_collection: lastCollect.status === 'fulfilled' ? lastCollect.value : null,
      };

      await env.CACHE.put('dashboard:latest', JSON.stringify(dashboard), { expirationTtl: 300 });
      return json(dashboard);
    }

    // ── Record Event ──
    case '/event': {
      if (request.method !== 'POST') return json({ error: 'POST required' }, 405);
      const body = await request.json() as { event_type: string; source: string; data?: Record<string, unknown> };
      if (!body.event_type || !body.source) return json({ error: 'event_type and source required' }, 400);

      await env.DB.prepare(
        `INSERT INTO events (event_type, source, data) VALUES (?, ?, ?)`
      ).bind(body.event_type, body.source, JSON.stringify(body.data || {})).run();

      return json({ stored: true });
    }

    // ── Ingest External Metric ──
    case '/ingest': {
      if (request.method !== 'POST') return json({ error: 'POST required' }, 405);
      const body = await request.json() as { metrics: MetricSnapshot[] };
      if (!body.metrics?.length) return json({ error: 'metrics array required' }, 400);

      const stored = await storeMetrics(env.DB, body.metrics);
      return json({ stored });
    }

    default:
      return json({
        error: 'Not found',
        endpoints: [
          'GET  /health', 'POST /collect', 'GET  /kpis', 'GET  /kpis/history',
          'GET  /metrics', 'GET  /metrics/latest', 'GET  /trends',
          'GET  /anomalies', 'GET  /forecasts', 'GET  /reports/daily',
          'GET  /reports/weekly', 'GET  /reports/history', 'GET  /reports/detail',
          'GET  /revenue', 'GET  /funnels', 'GET  /dashboard',
          'POST /event', 'POST /ingest',
        ],
      }, 404);
  }
}

// ── Worker Export ─────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      return await handleRequest(request, env);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      log('error', 'Unhandled error', { error: msg, path: new URL(request.url).pathname });
      return json({ error: 'Internal error', message: msg }, 500);
    }
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleScheduled(event, env));
  },
};
