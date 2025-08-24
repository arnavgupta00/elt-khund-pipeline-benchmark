import { sourcePool, targetPool, initializeTargetSchema, INPUT_TABLE_NAME } from '../config/database';
import { UserTransformer } from '../core/transformer';
import { ResearchLogger } from '../core/logger';
import { SourceUser, TransformedUser, PerformanceMetrics } from '../core/types';
import * as fs from 'fs';
import * as path from 'path';
import * as csvWriter from 'csv-writer';
import * as copyFrom from 'pg-copy-streams';

export class Case2Bulk {
  private transformer: UserTransformer;
  private logger: ResearchLogger;
  private tempDir: string;

  constructor() {
    this.transformer = new UserTransformer(2);
    this.logger = new ResearchLogger(2, 'Bulk File Export/Import');
    this.tempDir = path.join(process.cwd(), 'temp');
    
    if (!fs.existsSync(this.tempDir)) {
      fs.mkdirSync(this.tempDir, { recursive: true });
    }
  }

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;
    const exportFile = path.join(this.tempDir, 'users_export.csv');
    const transformedFile = path.join(this.tempDir, 'users_transformed.csv');

    try {
      // Initialize target schema
      await initializeTargetSchema();
      
      // Clear previous data for this case
      await targetPool.query('DELETE FROM transformed_users WHERE etl_case_number = 2');

      // Phase 1: Export
      this.logger.logPhaseStart('Export');
      const extractStartTime = Date.now();
      
      const query = limit 
        ? `SELECT * FROM ${INPUT_TABLE_NAME} LIMIT ${limit}`
        : `SELECT * FROM ${INPUT_TABLE_NAME}`;
      
      const result = await sourcePool.query(query);
      processedCount = result.rows.length;
      
      this.logger.logPhaseEnd('Export', processedCount);

      // Phase 2: Transform and write to file
      this.logger.logPhaseStart('Transform');
      const transformStartTime = Date.now();
      
      // Transform all data in memory
      const transformedUsers: TransformedUser[] = [];
      for (const row of result.rows) {
        try {
          const transformed = this.transformer.transform(row as SourceUser);
          transformedUsers.push(transformed);
        } catch (error) {
          errorCount++;
          this.logger.logError(error as Error, { userId: row.Id });
        }
      }

      // Write transformed data to CSV
      await this.writeTransformedCSV(transformedFile, transformedUsers);
      
      this.logger.logPhaseEnd('Transform', transformedUsers.length);

      // Phase 3: Bulk Load
      this.logger.logPhaseStart('Load');
      const loadStartTime = Date.now();
      
      await this.bulkLoad(transformedFile);
      
      this.logger.logPhaseEnd('Load', transformedUsers.length);

      // Clean up temp files
      if (fs.existsSync(exportFile)) fs.unlinkSync(exportFile);
      if (fs.existsSync(transformedFile)) fs.unlinkSync(transformedFile);

    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    }

    return await this.logger.finalizeMetrics(processedCount, errorCount);
  }

  private async writeTransformedCSV(filePath: string, users: TransformedUser[]): Promise<void> {
    const writer = csvWriter.createObjectCsvWriter({
      path: filePath,
      header: [
        { id: 'user_id', title: 'user_id' },
        { id: 'username', title: 'username' },
        { id: 'reputation_score', title: 'reputation_score' },
        { id: 'reputation_tier', title: 'reputation_tier' },
        { id: 'reputation_percentile', title: 'reputation_percentile' },
        { id: 'registered_at', title: 'registered_at' },
        { id: 'last_login', title: 'last_login' },
        { id: 'account_age_days', title: 'account_age_days' },
        { id: 'activity_status', title: 'activity_status' },
        { id: 'is_active', title: 'is_active' },
        { id: 'is_veteran', title: 'is_veteran' },
        { id: 'location_original', title: 'location_original' },
        { id: 'location_country', title: 'location_country' },
        { id: 'location_city', title: 'location_city' },
        { id: 'location_normalized', title: 'location_normalized' },
        { id: 'bio_original', title: 'bio_original' },
        { id: 'bio_summary', title: 'bio_summary' },
        { id: 'bio_wordcount', title: 'bio_wordcount' },
        { id: 'bio_has_content', title: 'bio_has_content' },
        { id: 'website_url', title: 'website_url' },
        { id: 'website_domain', title: 'website_domain' },
        { id: 'website_valid', title: 'website_valid' },
        { id: 'profile_views', title: 'profile_views' },
        { id: 'positive_votes', title: 'positive_votes' },
        { id: 'negative_votes', title: 'negative_votes' },
        { id: 'vote_ratio', title: 'vote_ratio' },
        { id: 'engagement_score', title: 'engagement_score' },
        { id: 'metadata', title: 'metadata' },
        { id: 'etl_timestamp', title: 'etl_timestamp' },
        { id: 'etl_case_number', title: 'etl_case_number' },
        { id: 'etl_batch_id', title: 'etl_batch_id' }
      ]
    });

    // Prepare records for CSV
    const records = users.map(user => ({
      ...user,
      metadata: JSON.stringify(user.metadata),
      registered_at: user.registered_at.toISOString(),
      last_login: user.last_login.toISOString(),
      etl_timestamp: user.etl_timestamp.toISOString()
    }));

    await writer.writeRecords(records);
  }

  private async bulkLoad(filePath: string): Promise<void> {
    const client = await targetPool.connect();
    
    try {
      await client.query('BEGIN');

      // Load into a temporary staging table to enable ON CONFLICT upsert
      await client.query(`
        CREATE TEMP TABLE tmp_transformed_users AS
        SELECT * FROM transformed_users WITH NO DATA;
      `);

      const stream = client.query(copyFrom.from(`
        COPY tmp_transformed_users (
          user_id, username, reputation_score, reputation_tier, reputation_percentile,
          registered_at, last_login, account_age_days, activity_status, is_active, is_veteran,
          location_original, location_country, location_city, location_normalized,
          bio_original, bio_summary, bio_wordcount, bio_has_content,
          website_url, website_domain, website_valid,
          profile_views, positive_votes, negative_votes, vote_ratio, engagement_score,
          metadata, etl_timestamp, etl_case_number, etl_batch_id
        ) FROM STDIN WITH CSV HEADER
      `));

      const fileStream = fs.createReadStream(filePath);
      await new Promise((resolve, reject) => {
        fileStream.on('error', reject);
        stream.on('error', reject);
        stream.on('finish', resolve);
        fileStream.pipe(stream);
      });

      // Upsert from staging into target
      await client.query(`
        INSERT INTO transformed_users AS t (
          user_id, username, reputation_score, reputation_tier, reputation_percentile,
          registered_at, last_login, account_age_days, activity_status, is_active, is_veteran,
          location_original, location_country, location_city, location_normalized,
          bio_original, bio_summary, bio_wordcount, bio_has_content,
          website_url, website_domain, website_valid,
          profile_views, positive_votes, negative_votes, vote_ratio, engagement_score,
          metadata, etl_timestamp, etl_case_number, etl_batch_id
        )
        SELECT 
          user_id, username, reputation_score, reputation_tier, reputation_percentile,
          registered_at, last_login, account_age_days, activity_status, is_active, is_veteran,
          location_original, location_country, location_city, location_normalized,
          bio_original, bio_summary, bio_wordcount, bio_has_content,
          website_url, website_domain, website_valid,
          profile_views, positive_votes, negative_votes, vote_ratio, engagement_score,
          metadata, etl_timestamp, etl_case_number, etl_batch_id
        FROM tmp_transformed_users
        ON CONFLICT (user_id, etl_case_number) DO NOTHING;
      `);

      await client.query('COMMIT');
      
    } finally {
      client.release();
    }
  }
}