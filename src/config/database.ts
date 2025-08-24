import { Pool, PoolConfig } from 'pg';
import * as dotenv from 'dotenv';

dotenv.config();

export interface DatabaseConfig {
  source: PoolConfig;
  target: PoolConfig;
}

export const dbConfig: DatabaseConfig = {
  source: {
    host: process.env.SOURCE_DB_HOST || 'localhost',
    port: parseInt(process.env.SOURCE_DB_PORT || '5432'),
    database: process.env.SOURCE_DB_NAME || 'stackoverflow',
    user: process.env.SOURCE_DB_USER || 'postgres',
    password: process.env.SOURCE_DB_PASSWORD || 'password',
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  },
  target: {
    host: process.env.TARGET_DB_HOST || 'localhost',
    port: parseInt(process.env.TARGET_DB_PORT || '5433'),
    database: process.env.TARGET_DB_NAME || 'target_db',
    user: process.env.TARGET_DB_USER || 'postgres',
    password: process.env.TARGET_DB_PASSWORD || 'password',
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  }
};

export const sourcePool = new Pool(dbConfig.source);
export const targetPool = new Pool(dbConfig.target);

export const INPUT_TABLE_NAME = process.env.INPUT_TABLE_NAME || 'users';

// Initialize target database schema
export async function initializeTargetSchema(): Promise<void> {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS transformed_users (
      user_id BIGINT PRIMARY KEY,
      username VARCHAR(255),
      reputation_score INTEGER,
      reputation_tier VARCHAR(20),
      reputation_percentile DECIMAL(5,2),
      
      registered_at TIMESTAMP,
      last_login TIMESTAMP,
      account_age_days INTEGER,
      activity_status VARCHAR(20),
      is_active BOOLEAN,
      is_veteran BOOLEAN,
      
      location_original VARCHAR(500),
      location_country VARCHAR(100),
      location_city VARCHAR(100),
      location_normalized VARCHAR(500),
      
      bio_original TEXT,
      bio_summary TEXT,
      bio_wordcount INTEGER,
      bio_has_content BOOLEAN,
      
      website_url TEXT,
      website_domain VARCHAR(255),
      website_valid BOOLEAN,
      
      profile_views INTEGER,
      positive_votes INTEGER,
      negative_votes INTEGER,
      vote_ratio DECIMAL(10,2),
      engagement_score DECIMAL(10,6),
      
      metadata JSONB,
      etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      etl_case_number INTEGER,
      etl_batch_id VARCHAR(50)
    );

    CREATE INDEX IF NOT EXISTS idx_reputation_tier ON transformed_users(reputation_tier);
    CREATE INDEX IF NOT EXISTS idx_activity_status ON transformed_users(activity_status);
    CREATE INDEX IF NOT EXISTS idx_is_active ON transformed_users(is_active);
    CREATE INDEX IF NOT EXISTS idx_etl_case ON transformed_users(etl_case_number);
  `;

  await targetPool.query(createTableQuery);
}