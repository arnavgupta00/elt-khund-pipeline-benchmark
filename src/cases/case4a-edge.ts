import axios from 'axios';
import { sourcePool, targetPool, initializeTargetSchema, INPUT_TABLE_NAME } from '../config/database';
import { ResearchLogger } from '../core/logger';
import { SourceUser, TransformedUser, PerformanceMetrics } from '../core/types';

export class Case4aEdge {
  private logger: ResearchLogger;
  private workerUrl: string;
  private readonly BATCH_SIZE = 5000; // Larger batches for edge processing
  private readonly WORKER_CONCURRENCY = 10; // Number of parallel edge workers

  constructor() {
    this.logger = new ResearchLogger(4, 'Edge Computing (Cloudflare Workers)');
    this.workerUrl = process.env.CLOUDFLARE_WORKER_URL || 'https://etl-processor.your-subdomain.workers.dev';
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

      // Process batches in parallel through edge workers
      const processingPromises: Promise<{ processed: number; errors: number }>[] = [];
      
      for (let i = 0; i < batches.length; i += this.WORKER_CONCURRENCY) {
        const batchGroup = batches.slice(i, i + this.WORKER_CONCURRENCY);
        const groupPromises = batchGroup.map(batch => 
          this.processEdgeBatch(batch.offset, batch.limit)
        );
        
        const results = await Promise.all(groupPromises);
        
        for (const result of results) {
          processedCount += result.processed;
          errorCount += result.errors;
        }
        
        this.logger.logProgress(processedCount, totalCount, 'Edge Processing');
      }

      this.logger.logPhaseEnd('Edge Processing', processedCount);

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

    try {
      // Extract batch
      const query = `SELECT * FROM ${INPUT_TABLE_NAME} OFFSET ${offset} LIMIT ${limit}`;
      const result = await sourcePool.query(query);
      
      // Send to edge worker for transformation
      const response = await axios.post(this.workerUrl, {
        users: result.rows,
        caseNumber: 4
      }, {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.CLOUDFLARE_WORKER_TOKEN}`
        },
        timeout: 30000 // 30 second timeout
      });

      const transformedUsers: TransformedUser[] = response.data.transformedUsers;
      
      // Batch insert transformed data
      await this.batchInsert(transformedUsers);
      processed = transformedUsers.length;
      
    } catch (error) {
      errors = limit; // Assume all records in batch failed
      this.logger.logError(error as Error, { offset, limit });
    }

    return { processed, errors };
  }

  private async batchInsert(users: TransformedUser[]): Promise<void> {
    const client = await targetPool.connect();
    
    try {
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
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}