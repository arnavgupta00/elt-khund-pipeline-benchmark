import axios from 'axios';
import http from 'http';
import https from 'https';
import { sourcePool, targetPool, initializeTargetSchema, INPUT_TABLE_NAME } from '../config/database';
import { ResearchLogger } from '../core/logger';
import { SourceUser, TransformedUser, PerformanceMetrics } from '../core/types';

export class Case4aEdge {
  private logger: ResearchLogger;
  private workerUrl: string;
  private readonly BATCH_SIZE: number; // batch size per worker call
  private readonly WORKER_CONCURRENCY: number; // number of parallel edge requests
  private readonly DB_CONCURRENCY: number; // number of parallel database operations
  private httpClient: any;
  private dbQueue: Array<() => Promise<any>> = [];
  private activeDbConnections = 0;

  constructor() {
    this.logger = new ResearchLogger(4, 'Edge Computing (Cloudflare Workers)');
    this.workerUrl = process.env.CLOUDFLARE_WORKER_URL || 'https://edge-transformer.adv-dep-test.workers.dev';

    // Optimize for unlimited workers but limited DB connections
    this.BATCH_SIZE = parseInt(process.env.CASE4A_BATCH_SIZE || '6600', 10); // Smaller batches for faster processing
    this.WORKER_CONCURRENCY = parseInt(process.env.CASE4A_WORKER_CONCURRENCY || '100', 10); // High worker concurrency
    this.DB_CONCURRENCY = 18; // Keep below the pool max of 20 to avoid exhaustion (reserve 2 for other operations)

    // Build a keep-alive axios client with auth header
    const token = process.env.CLOUDFLARE_WORKER_TOKEN || process.env.WORKER_AUTH_TOKEN || '';
    const timeoutMs = parseInt(process.env.CASE4A_HTTP_TIMEOUT_MS || '', 10) || 60000; // Increased default timeout
    const httpAgent = new http.Agent({ 
      keepAlive: true, 
      maxSockets: 200, // Increased for high worker concurrency
      timeout: timeoutMs,
      keepAliveMsecs: 30000 // Keep connections alive for 30s
    });
    const httpsAgent = new https.Agent({ 
      keepAlive: true, 
      maxSockets: 200, // Increased for high worker concurrency
      timeout: timeoutMs,
      keepAliveMsecs: 30000 // Keep connections alive for 30s
    });
    this.httpClient = axios.create({
      baseURL: this.workerUrl,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {})
      },
      timeout: timeoutMs,
      // Cast to any to pass Node agent options even if types complain in some versions
      ...( { httpAgent, httpsAgent } as any ),
      // allow large payloads when sending big batches
      maxBodyLength: Infinity,
      maxContentLength: Infinity
    });
  }

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      // Initialize target schema
      await initializeTargetSchema();
      
      // Clear previous data for this case
      await targetPool.query('DELETE FROM transformed_users WHERE etl_case_number = 4');

      // Get total count
      const countResult = await sourcePool.query(`SELECT COUNT(*) FROM ${INPUT_TABLE_NAME}`);
      const totalCount = Math.min(
        parseInt(countResult.rows[0].count),
        limit || parseInt(countResult.rows[0].count)
      );

      this.logger.logPhaseStart('Edge Processing');

      // Calculate batches for edge processing
      const batchCount = Math.ceil(totalCount / this.BATCH_SIZE);
      const batches: Array<{ offset: number; limit: number }> = [];
      
      for (let i = 0; i < batchCount; i++) {
        batches.push({
          offset: i * this.BATCH_SIZE,
          limit: Math.min(this.BATCH_SIZE, totalCount - (i * this.BATCH_SIZE))
        });
      }

      // Process batches in controlled parallel groups through edge workers
      for (let i = 0; i < batches.length; i += this.WORKER_CONCURRENCY) {
        const batchGroup = batches.slice(i, i + this.WORKER_CONCURRENCY);
        const groupPromises = batchGroup.map(batch => this.processEdgeBatch(batch.offset, batch.limit));
        
        const results = await Promise.all(groupPromises);
        
        for (const result of results) {
          processedCount += result.processed;
          errorCount += result.errors;
        }
        
        this.logger.logProgress(processedCount, totalCount, 'Edge Processing');
      }

      this.logger.logPhaseEnd('Edge Processing', processedCount);

      // Wait for all database operations to complete
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

  private async processEdgeBatch(offset: number, limit: number): Promise<{ processed: number; errors: number }> {
    let processed = 0;
    let errors = 0;

    const t0 = Date.now();
    let t1 = t0, t2 = t0, t3 = t0;

    try {

      // Extract batch
      const query = `SELECT * FROM ${INPUT_TABLE_NAME} OFFSET ${offset} LIMIT ${limit}`;
      const result = await sourcePool.query(query);
      t1 = Date.now();

      // Send to edge worker for transformation with retry/backoff
      const response = await this.postWithRetry<{ transformedUsers: TransformedUser[] }>({
        users: result.rows,
        caseNumber: 4
      });
      t2 = Date.now();
      const transformedUsers: TransformedUser[] = response.transformedUsers;

      // Batch insert transformed data with connection pool management
      await this.queueBatchInsert(transformedUsers);
      t3 = Date.now();

      processed = transformedUsers.length;

      // Timing summary to identify the slowest step
      const extractMs = t1 - t0;
      const workerMs = t2 - t1;
      const insertMs = t3 - t2;
      const totalMs = t3 - t0;
      const maxVal = Math.max(extractMs, workerMs, insertMs);
      const maxStep =
        maxVal === extractMs ? 'extract' :
        maxVal === workerMs ? 'worker' :
        'insert';

      this.logger.logPhaseStart(`Batch Timing Summary (offset=${offset}, limit=${limit})`);
      this.logger.logPhaseEnd(
        `Batch Timing Summary (offset=${offset}, limit=${limit}) | extract=${extractMs}ms, worker=${workerMs}ms, insert=${insertMs}ms, total=${totalMs}ms | max=${maxStep}`,
        processed
      );

    } catch (error) {
      errors = limit; // Assume all records in batch failed
      const err = error as any;
      const context: Record<string, any> = { offset, limit };
      if (err && (err as any).isAxiosError) {
        const ax = err as any;
        context.axios = {
          code: ax.code,
          status: ax.response?.status,
          statusText: ax.response?.statusText,
          url: ax.config?.url || this.workerUrl
        };
        if (typeof ax.message === 'string') context.message = ax.message;
      }
      this.logger.logError(error as Error, context);
    }

    return { processed, errors };
  }

  private async postWithRetry<T>(payload: any, retries = 3, baseBackoffMs = 500): Promise<T> {
    let attempt = 0;
    let lastErr: any;
  while (attempt <= retries) {
      try {
  const res = await this.httpClient.post('/', payload);
  return res.data as T;
      } catch (err) {
    lastErr = err;
    const ax = err as any;
    const status = ax?.response?.status as number | undefined;
    const code = (ax as any)?.code as string | undefined;
    const msg = (ax && typeof ax.message === 'string') ? ax.message : '';
        const transient =
          // HTTP status-based retries
          (status !== undefined && (status >= 500 || status === 429)) ||
          // network / transport errors
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
    // Wait for all active connections and queue to be processed
    while (this.activeDbConnections > 0 || this.dbQueue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  private async batchInsert(users: TransformedUser[]): Promise<void> {
    const maxRetries = 3;
    let attempt = 0;
    
    while (attempt <= maxRetries) {
      let client;
      try {
        // Use a shorter timeout for getting a connection
        client = await Promise.race([
          targetPool.connect(),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Connection timeout')), 5000)
          )
        ]) as any;
        
        await client.query('BEGIN');
        
        for (const user of users) {
          const query = `
            INSERT INTO transformed_users (
              user_id, username, reputation_score, reputation_tier, reputation_percentile,
              registered_at, last_login, account_age_days, activity_status, is_active, is_veteran,
              location_original, location_country, location_city, location_normalized,
              bio_original, bio_summary, bio_wordcount, bio_has_content,
              website_url, website_domain, website_valid,
              profile_views, positive_votes, negative_votes, vote_ratio, engagement_score,
              metadata, etl_timestamp, etl_case_number, etl_batch_id
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
              $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
            )
            ON CONFLICT (user_id, etl_case_number) DO NOTHING
          `;

          const values = [
            user.user_id, user.username, user.reputation_score, user.reputation_tier, user.reputation_percentile,
            user.registered_at, user.last_login, user.account_age_days, user.activity_status, user.is_active, user.is_veteran,
            user.location_original, user.location_country, user.location_city, user.location_normalized,
            user.bio_original, user.bio_summary, user.bio_wordcount, user.bio_has_content,
            user.website_url, user.website_domain, user.website_valid,
            user.profile_views, user.positive_votes, user.negative_votes, user.vote_ratio, user.engagement_score,
            JSON.stringify(user.metadata), user.etl_timestamp, user.etl_case_number, user.etl_batch_id
          ];

          await client.query(query, values);
        }
        
        await client.query('COMMIT');
        return; // Success, exit the retry loop
        
      } catch (error) {
        if (client) {
          try {
            await client.query('ROLLBACK');
          } catch (rollbackError) {
            // Ignore rollback errors
          }
        }
        
        attempt++;
        if (attempt > maxRetries) {
          throw error;
        }
        
        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt - 1)));
        
      } finally {
        if (client) {
          client.release();
        }
      }
    }
  }
}