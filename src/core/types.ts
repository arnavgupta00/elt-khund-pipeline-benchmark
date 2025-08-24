export interface SourceUser {
  Id: number;
  Reputation: number;
  CreationDate: string;
  DisplayName: string;
  LastAccessDate: string;
  WebsiteUrl: string | null;
  Location: string | null;
  AboutMe: string | null;
  Views: number;
  UpVotes: number;
  DownVotes: number;
  ProfileImageUrl: string | null;
  EmailHash: string | null;
  AccountId: number | null;
}

export interface TransformedUser {
  user_id: number;
  username: string;
  reputation_score: number;
  reputation_tier: string;
  reputation_percentile: number;
  
  registered_at: Date;
  last_login: Date;
  account_age_days: number;
  activity_status: string;
  is_active: boolean;
  is_veteran: boolean;
  
  location_original: string | null;
  location_country: string | null;
  location_city: string | null;
  location_normalized: string | null;
  
  bio_original: string | null;
  bio_summary: string | null;
  bio_wordcount: number;
  bio_has_content: boolean;
  
  website_url: string | null;
  website_domain: string | null;
  website_valid: boolean;
  
  profile_views: number;
  positive_votes: number;
  negative_votes: number;
  vote_ratio: number;
  engagement_score: number;
  
  metadata: any;
  etl_timestamp: Date;
  etl_case_number: number;
  etl_batch_id: string;
}

export interface PerformanceMetrics {
  avg_response_time: number;
  case_number: number;
  case_name: string;
  start_time: Date;
  end_time: Date;
  total_duration_ms: number;
  total_records: number;
  records_per_second: number;
  
  extract_duration_ms: number;
  transform_duration_ms: number;
  load_duration_ms: number;
  
  memory_start_mb: number;
  memory_peak_mb: number;
  memory_end_mb: number;
  
  cpu_user_ms: number;
  cpu_system_ms: number;
  
  error_count: number;
  batch_count?: number;
  thread_count?: number;
  worker_count?: number;
  
  detailed_timings: any[];
}