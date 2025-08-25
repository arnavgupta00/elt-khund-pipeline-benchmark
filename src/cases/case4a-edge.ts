import axios, { AxiosInstance } from 'axios';
import http from 'http';
import https from 'https';
import { gzipSync } from 'zlib';
import { sourcePool, targetPool, initializeTargetSchema, INPUT_TABLE_NAME } from '../config/database';
import { ResearchLogger } from '../core/logger';
import { SourceUser, TransformedUser, PerformanceMetrics } from '../core/types';

/**
 * Case 4a (Edge): Fast pipeline with
 * - Keyset pagination (ORDER BY pk WHERE pk > last)
 * - Gzipped HTTP payloads to Cloudflare Worker
 * - Chunked multi-row INSERTs
 * - Tuned concurrency backpressure
 */
export class Case4aEdge {
  private logger: ResearchLogger;
  private workerUrl: string;

  // Tunable pipeline knobs
  private readonly BATCH_SIZE: number;          // batch size per worker call
  private readonly WORKER_CONCURRENCY: number;  // number of parallel edge requests in-flight
  private readonly DB_CONCURRENCY: number;      // parallel DB writers (<= pool max)
  private readonly INSERT_CHUNK_SIZE: number;   // rows per multi-VALUES insert
  private readonly SOURCE_PK: string;           // keyset column (e.g. id or user_id)

  private httpClient: AxiosInstance;

  // DB queue to respect DB_CONCURRENCY
  private dbQueue: Array<() => Promise<any>> = [];
  private activeDbConnections = 0;

  constructor() {
    this.logger = new ResearchLogger(4, 'Edge Computing (Cloudflare Workers)');
    this.workerUrl = process.env.CLOUDFLARE_WORKER_URL || 'https://edge-transformer.adv-dep-test.workers.dev';

    // --- sensible defaults tuned for paid Workers + Postgres ---
    this.BATCH_SIZE = parseInt(process.env.CASE4A_BATCH_SIZE || '2000', 10);
    this.WORKER_CONCURRENCY = parseInt(process.env.CASE4A_WORKER_CONCURRENCY || '32', 10);
    this.DB_CONCURRENCY = parseInt(process.env.CASE4A_DB_CONCURRENCY || '14', 10);
    this.INSERT_CHUNK_SIZE = parseInt(process.env.CASE4A_INSERT_CHUNK_SIZE || '1000', 10);

    // Prefer a monotonic bigint primary key; override if your source pk differs
    this.SOURCE_PK = process.env.CASE4A_SOURCE_PK || 'id';

    // --- keep-alive axios client with gzip request bodies ---
    const token = process.env.CLOUDFLARE_WORKER_TOKEN || process.env.WORKER_AUTH_TOKEN || '';
    const timeoutMs = parseInt(process.env.CASE4A_HTTP_TIMEOUT_MS || '', 10) || 60000;

    const httpAgent = new http.Agent({
      keepAlive: true,
      maxSockets: Math.max(this.WORKER_CONCURRENCY * 2, 64), // enough sockets to multiplex requests
      timeout: timeoutMs,
      keepAliveMsecs: 30000
    });
    const httpsAgent = new https.Agent({
      keepAlive: true,
      maxSockets: Math.max(this.WORKER_CONCURRENCY * 2, 64),
      timeout: timeoutMs,
      keepAliveMsecs: 30000
    });

    this.httpClient = axios.create({
      baseURL: this.workerUrl,
      headers: {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
        ...(token ? { Authorization: `Bearer ${token}` } : {})
      },
      timeout: timeoutMs,
      // pass Node agents
      ...( { httpAgent, httpsAgent } as any ),
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      // Gzip the body we send to Workers
      transformRequest: [(data: any, headers: any) => {
        const gz = gzipSync(Buffer.from(JSON.stringify(data)));
        headers['Content-Encoding'] = 'gzip';
        headers['Content-Type'] = 'application/json';
        return gz;
      }],
      // Accept compressed responses
      decompress: true
    });
  }

  /**
   * Execute the pipeline.
   * - Initializes schema
   * - Deletes previous case data
   * - Streams source via keyset pagination
   * - Transforms on Workers
   * - Bulk inserts into target
   */
  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;
    let hardCapRemaining = typeof limit === 'number' && limit > 0 ? limit : Number.POSITIVE_INFINITY;

    try {
      await initializeTargetSchema();
      await targetPool.query('DELETE FROM transformed_users WHERE etl_case_number = 4');

      this.logger.logPhaseStart('Edge Processing');

      let lastPk: any = null;
      const inFlight = new Set<Promise<void>>();

      // Produce → transform → insert with backpressure
      while (hardCapRemaining > 0) {
        const take = Math.min(this.BATCH_SIZE, hardCapRemaining);
        const rows = await this.fetchBatchAfterPk(lastPk, take);
        if (rows.length === 0) break;
        lastPk = rows[rows.length - 1][this.SOURCE_PK];
        hardCapRemaining -= rows.length;

        // throttle worker concurrency
        while (inFlight.size >= this.WORKER_CONCURRENCY) {
          await Promise.race(inFlight);
        }

        const p = (async () => {
          const t0 = Date.now();
          try {
            const { transformedUsers } = await this.postWithRetry<{ transformedUsers: TransformedUser[] }>({
              users: rows,
              caseNumber: 4
            });
            const t1 = Date.now();

            await this.queueBatchInsert(transformedUsers);
            const t2 = Date.now();

            processedCount += transformedUsers.length;
            this.logger.logPhaseEnd(
              `Batch Timing | worker=${t1 - t0}ms insert=${t2 - t1}ms total=${t2 - t0}ms`,
              transformedUsers.length
            );
            // No total known now; show processed only
            this.logger.logProgress(processedCount, processedCount, 'Edge Processing');

          } catch (e) {
            errorCount += rows.length;
            this.logger.logError(e as Error, { lastPk, batchSize: rows.length });
          }
        })();

        inFlight.add(p);
        p.finally(() => inFlight.delete(p));
      }

      await Promise.all(inFlight);
      this.logger.logPhaseEnd('Edge Processing', processedCount);

      // Wait for DB queue to finish all inserts
      await this.waitForDbQueue();

    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    }

    const metrics = await this.logger.finalizeMetrics(processedCount, errorCount);
    metrics.worker_count = this.WORKER_CONCURRENCY;
    metrics.batch_count = Math.ceil(processedCount / this.BATCH_SIZE);
    return metrics;
  }

  /**
   * Fetches the next batch using keyset pagination.
   * Requires SOURCE_PK to be a sortable column (e.g., bigint id).
   */
  private async fetchBatchAfterPk(lastPk: any, limit: number): Promise<SourceUser[]> {
    // Note: parameter positions differ depending on whether lastPk is present.
    if (lastPk == null) {
      const sql = `
        SELECT * FROM ${INPUT_TABLE_NAME}
        ORDER BY ${this.SOURCE_PK}
        LIMIT $1
      `;
      const res = await sourcePool.query(sql, [limit]);
      return res.rows as SourceUser[];
    } else {
      const sql = `
        SELECT * FROM ${INPUT_TABLE_NAME}
        WHERE ${this.SOURCE_PK} > $1
        ORDER BY ${this.SOURCE_PK}
        LIMIT $2
      `;
      const res = await sourcePool.query(sql, [lastPk, limit]);
      return res.rows as SourceUser[];
    }
  }

  /**
   * POST to the Worker with retry/backoff on transient errors.
   * Body is gzipped by axios transformRequest.
   */
  private async postWithRetry<T>(payload: any, retries = 3, baseBackoffMs = 500): Promise<T> {
    let attempt = 0;
    let lastErr: any;
    while (attempt <= retries) {
      try {
        const res = await this.httpClient.post('/', payload);
        return res.data as T;
      } catch (err: any) {
        lastErr = err;
        const status = err?.response?.status as number | undefined;
        const code = err?.code as string | undefined;
        const msg = typeof err?.message === 'string' ? err.message : '';

        const transient =
          (status !== undefined && (status >= 500 || status === 429)) ||
          ['ECONNRESET', 'ECONNABORTED', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ENOTFOUND', 'EAI_AGAIN'].includes(code || '') ||
          msg.toLowerCase().includes('socket hang up');

        if (!transient || attempt === retries) {
          throw err;
        }
        const delay = baseBackoffMs * Math.pow(2, attempt) + Math.floor(Math.random() * 250);
        await new Promise(r => setTimeout(r, delay));
        attempt++;
      }
    }
    throw lastErr;
  }

  /**
   * Queue a batch insert respecting DB_CONCURRENCY.
   */
  private async queueBatchInsert(users: TransformedUser[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const task = async () => {
        try {
          await this.batchInsert(users);
          resolve();
        } catch (error) {
          reject(error);
        } finally {
          this.activeDbConnections--;
          this.processDbQueue();
        }
      };

      if (this.activeDbConnections < this.DB_CONCURRENCY) {
        this.activeDbConnections++;
        task();
      } else {
        this.dbQueue.push(task);
      }
    });
  }

  private processDbQueue(): void {
    if (this.dbQueue.length > 0 && this.activeDbConnections < this.DB_CONCURRENCY) {
      const task = this.dbQueue.shift();
      if (task) {
        this.activeDbConnections++;
        task();
      }
    }
  }

  private async waitForDbQueue(): Promise<void> {
    while (this.activeDbConnections > 0 || this.dbQueue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  /**
   * Chunked multi-row INSERT with ON CONFLICT DO NOTHING.
   * ~1000 rows per VALUES keeps param counts within limits.
   * If you want the absolute ceiling, swap this to COPY → INSERT ... SELECT.
   */
  private async batchInsert(users: TransformedUser[]): Promise<void> {
    if (!users.length) return;

    const cols = [
      'user_id','username','reputation_score','reputation_tier','reputation_percentile',
      'registered_at','last_login','account_age_days','activity_status','is_active','is_veteran',
      'location_original','location_country','location_city','location_normalized',
      'bio_original','bio_summary','bio_wordcount','bio_has_content',
      'website_url','website_domain','website_valid',
      'profile_views','positive_votes','negative_votes','vote_ratio','engagement_score',
      'metadata','etl_timestamp','etl_case_number','etl_batch_id'
    ];

    const maxRetries = 3;
    let attempt = 0;

    while (attempt <= maxRetries) {
      let client: any;
      try {
        client = await Promise.race([
          targetPool.connect(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('Connection timeout')), 5000))
        ]);

        await client.query('BEGIN');

        for (let i = 0; i < users.length; i += this.INSERT_CHUNK_SIZE) {
          const chunk = users.slice(i, i + this.INSERT_CHUNK_SIZE);

          const placeholders: string[] = [];
          const values: any[] = [];
          let p = 1;

          for (const u of chunk) {
            placeholders.push(`(${Array.from({ length: cols.length }, () => `$${p++}`).join(',')})`);
            values.push(
              u.user_id, u.username, u.reputation_score, u.reputation_tier, u.reputation_percentile,
              u.registered_at, u.last_login, u.account_age_days, u.activity_status, u.is_active, u.is_veteran,
              u.location_original, u.location_country, u.location_city, u.location_normalized,
              u.bio_original, u.bio_summary, u.bio_wordcount, u.bio_has_content,
              u.website_url, u.website_domain, u.website_valid,
              u.profile_views, u.positive_votes, u.negative_votes, u.vote_ratio, u.engagement_score,
              JSON.stringify(u.metadata), u.etl_timestamp, u.etl_case_number, u.etl_batch_id
            );
          }

          const sql = `
            INSERT INTO transformed_users (${cols.join(',')})
            VALUES ${placeholders.join(',')}
            ON CONFLICT (user_id, etl_case_number) DO NOTHING
          `;

          await client.query(sql, values);
        }

        await client.query('COMMIT');
        return;

      } catch (error) {
        if (client) {
          try { await client.query('ROLLBACK'); } catch {}
        }
        attempt++;
        if (attempt > maxRetries) throw error;
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt - 1)));
      } finally {
        if (client) client.release();
      }
    }
  }
}
