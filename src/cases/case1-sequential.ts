import { sourcePool, targetPool, initializeTargetSchema, INPUT_TABLE_NAME } from '../config/database';
import { UserTransformer } from '../core/transformer';
import { ResearchLogger } from '../core/logger';
import { SourceUser, TransformedUser, PerformanceMetrics } from '../core/types';

export class Case1Sequential {
  private transformer: UserTransformer;
  private logger: ResearchLogger;

  constructor() {
    this.transformer = new UserTransformer(1);
    this.logger = new ResearchLogger(1, 'Sequential Row-by-Row');
  }

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      // Initialize target schema
      await initializeTargetSchema();
      
      // Clear previous data for this case
      await targetPool.query('DELETE FROM transformed_users WHERE etl_case_number = 1');
      
      this.logger.logPhaseStart('Extract');

      // Get total count
      const countResult = await sourcePool.query(`SELECT COUNT(*) FROM ${INPUT_TABLE_NAME}`);
      const totalCount = Math.min(
        parseInt(countResult.rows[0].count),
        limit || parseInt(countResult.rows[0].count)
      );

      this.logger.logPhaseEnd('Count', totalCount);

      // Process row by row
      const query = limit 
        ? `SELECT * FROM ${INPUT_TABLE_NAME} LIMIT ${limit}`
        : `SELECT * FROM ${INPUT_TABLE_NAME}`;

      const result = await sourcePool.query(query);
      
      this.logger.logPhaseStart('Process');

      for (let i = 0; i < result.rows.length; i++) {
        const sourceUser = result.rows[i] as SourceUser;
        
        try {
          // Transform
          const startTransform = Date.now();
          const transformedUser = this.transformer.transform(sourceUser);
          const transformTime = Date.now() - startTransform;

          // Load
          const startLoad = Date.now();
          await this.insertUser(transformedUser);
          const loadTime = Date.now() - startLoad;

          processedCount++;

          // Log progress every 100 records
          if (processedCount % 100 === 0) {
            this.logger.logProgress(processedCount, totalCount, 'Processing');
          }

        } catch (error) {
          errorCount++;
          this.logger.logError(error as Error, { userId: sourceUser.Id });
        }
      }

      this.logger.logPhaseEnd('Process', processedCount);

    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    }

    return await this.logger.finalizeMetrics(processedCount, errorCount);
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

    await targetPool.query(query, values);
  }
}