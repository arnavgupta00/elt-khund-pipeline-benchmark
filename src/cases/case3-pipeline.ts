import { Worker } from "worker_threads";
import {
  sourcePool,
  targetPool,
  initializeTargetSchema,
  INPUT_TABLE_NAME,
} from "../config/database";
import { UserTransformer } from "../core/transformer";
import { ResearchLogger } from "../core/logger";
import { SourceUser, TransformedUser, PerformanceMetrics } from "../core/types";
import PQueue from "p-queue";
import * as path from "path";

export class Case3Pipeline {
  private logger: ResearchLogger;
  private extractQueue: PQueue;
  private transformQueue: PQueue;
  private loadQueue: PQueue;
  private transformer: UserTransformer;

  private readonly BATCH_SIZE = 1000;
  private readonly EXTRACT_CONCURRENCY = 2;
  private readonly TRANSFORM_CONCURRENCY = 8;
  private readonly LOAD_CONCURRENCY = 2;

  constructor() {
    this.logger = new ResearchLogger(3, "Multi-threaded Pipeline");
    this.transformer = new UserTransformer(3);

    this.extractQueue = new PQueue({ concurrency: this.EXTRACT_CONCURRENCY });
    this.transformQueue = new PQueue({
      concurrency: this.TRANSFORM_CONCURRENCY,
    });
    this.loadQueue = new PQueue({ concurrency: this.LOAD_CONCURRENCY });
  }

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      // Initialize target schema
      await initializeTargetSchema();

      // Clear previous data for this case
      await targetPool.query(
        "DELETE FROM transformed_users WHERE etl_case_number = 3"
      );

      // Get total count
      const countResult = await sourcePool.query(
        `SELECT COUNT(*) FROM ${INPUT_TABLE_NAME}`
      );
      const totalCount = Math.min(
        parseInt(countResult.rows[0].count),
        limit || parseInt(countResult.rows[0].count)
      );

      this.logger.logPhaseStart("Pipeline");

      // Calculate batches
      const batchCount = Math.ceil(totalCount / this.BATCH_SIZE);
      const batches: Array<{ offset: number; limit: number }> = [];

      for (let i = 0; i < batchCount; i++) {
        batches.push({
          offset: i * this.BATCH_SIZE,
          limit: Math.min(this.BATCH_SIZE, totalCount - i * this.BATCH_SIZE),
        });
      }

      // Process batches in pipeline
      const processingPromises = batches.map((batch) =>
        this.processBatch(batch.offset, batch.limit)
      );

      const results = await Promise.all(processingPromises);

      // Aggregate results
      for (const result of results) {
        processedCount += result.processed;
        errorCount += result.errors;
      }

      this.logger.logPhaseEnd("Pipeline", processedCount);
    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    }

    const metrics = await this.logger.finalizeMetrics(
      processedCount,
      errorCount
    );
    metrics.thread_count =
      this.EXTRACT_CONCURRENCY +
      this.TRANSFORM_CONCURRENCY +
      this.LOAD_CONCURRENCY;
    metrics.batch_count = Math.ceil(processedCount / this.BATCH_SIZE);

    return metrics;
  }

  private async processBatch(
    offset: number,
    limit: number
  ): Promise<{ processed: number; errors: number }> {
    let processed = 0;
    let errors = 0;

    return new Promise((resolve) => {
      // Extract
      this.extractQueue.add(async () => {
        const query = `SELECT * FROM ${INPUT_TABLE_NAME} OFFSET ${offset} LIMIT ${limit}`;
        const result = await sourcePool.query(query);

        // Transform in parallel
        const transformPromises = result.rows.map((row) =>
          this.transformQueue.add(async () => {
            try {
              const transformed = this.transformer.transform(row as SourceUser);

              // Load
              await this.loadQueue.add(async () => {
                await this.insertUser(transformed);
                processed++;

                if (processed % 100 === 0) {
                  this.logger.logProgress(
                    processed,
                    limit,
                    `Batch ${offset / this.BATCH_SIZE + 1}`
                  );
                }
              });
            } catch (error) {
              errors++;
              this.logger.logError(error as Error, { userId: row.Id });
            }
          })
        );

        await Promise.all(transformPromises);
        resolve({ processed, errors });
      });
    });
  }

  private async insertUser(user: TransformedUser): Promise<void> {
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
      user.user_id,
      user.username,
      user.reputation_score,
      user.reputation_tier,
      user.reputation_percentile,
      user.registered_at,
      user.last_login,
      user.account_age_days,
      user.activity_status,
      user.is_active,
      user.is_veteran,
      user.location_original,
      user.location_country,
      user.location_city,
      user.location_normalized,
      user.bio_original,
      user.bio_summary,
      user.bio_wordcount,
      user.bio_has_content,
      user.website_url,
      user.website_domain,
      user.website_valid,
      user.profile_views,
      user.positive_votes,
      user.negative_votes,
      user.vote_ratio,
      user.engagement_score,
      JSON.stringify(user.metadata),
      user.etl_timestamp,
      user.etl_case_number,
      user.etl_batch_id,
    ];

    await targetPool.query(query, values);
  }
}
