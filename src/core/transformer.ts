import { SourceUser, TransformedUser } from './types';
import * as crypto from 'crypto';

export class UserTransformer {
  private reputationTiers = [
    { min: 0, max: 100, tier: 'bronze' },
    { min: 101, max: 1000, tier: 'silver' },
    { min: 1001, max: 10000, tier: 'gold' },
    { min: 10001, max: 100000, tier: 'platinum' },
    { min: 100001, max: Infinity, tier: 'legendary' }
  ];

  private caseNumber: number;
  private batchId: string;

  constructor(caseNumber: number) {
    this.caseNumber = caseNumber;
    this.batchId = this.generateBatchId();
  }

  private generateBatchId(): string {
    return `case${this.caseNumber}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
  }

  public transform(source: SourceUser): TransformedUser {
    const now = new Date();
    const creationDate = new Date(source.CreationDate);
    const lastAccessDate = new Date(source.LastAccessDate);
    
    return {
      // IDs and basic info
      user_id: source.Id,
      username: this.sanitizeUsername(source.DisplayName),
      
      // Reputation analysis
      reputation_score: source.Reputation,
      reputation_tier: this.calculateReputationTier(source.Reputation),
      reputation_percentile: this.estimatePercentile(source.Reputation),
      
      // Temporal data
      registered_at: creationDate,
      last_login: lastAccessDate,
      account_age_days: this.daysBetween(creationDate, now),
      activity_status: this.determineActivityStatus(lastAccessDate),
      is_active: this.daysBetween(lastAccessDate, now) < 365,
      is_veteran: this.daysBetween(creationDate, now) > 1825,
      
      // Location processing
      location_original: source.Location,
      location_country: this.extractCountry(source.Location),
      location_city: this.extractCity(source.Location),
      location_normalized: this.normalizeLocation(source.Location),
      
      // Bio processing
      bio_original: source.AboutMe,
      bio_summary: this.extractBioSummary(source.AboutMe),
      bio_wordcount: this.countWords(source.AboutMe),
      bio_has_content: !!source.AboutMe && source.AboutMe.length > 10,
      
      // Website processing
      website_url: source.WebsiteUrl,
      website_domain: this.extractDomain(source.WebsiteUrl),
      website_valid: this.validateUrl(source.WebsiteUrl),
      
      // Engagement metrics
      profile_views: source.Views || 0,
      positive_votes: source.UpVotes || 0,
      negative_votes: source.DownVotes || 0,
      vote_ratio: this.calculateVoteRatio(source.UpVotes, source.DownVotes),
      engagement_score: this.calculateEngagementScore(source),
      
      // Metadata
      metadata: {
        original_id: source.Id,
        import_timestamp: now.toISOString(),
        etl_version: '1.0.0',
        processing_case: this.caseNumber,
        has_email: !!source.EmailHash,
        has_avatar: !!source.ProfileImageUrl
      },
      
      etl_timestamp: now,
      etl_case_number: this.caseNumber,
      etl_batch_id: this.batchId
    };
  }

  public transformBatch(sources: SourceUser[]): TransformedUser[] {
    return sources.map(source => this.transform(source));
  }

  // Helper methods for transformation
  private sanitizeUsername(name: string): string {
    if (!name) return 'anonymous';
    return name.substring(0, 255).replace(/[^a-zA-Z0-9_.-]/g, '_');
  }

  private calculateReputationTier(reputation: number): string {
    const tier = this.reputationTiers.find(t => reputation >= t.min && reputation <= t.max);
    return tier?.tier || 'bronze';
  }

  private estimatePercentile(reputation: number): number {
    // Simplified percentile calculation based on Stack Overflow distribution
    if (reputation <= 1) return 0;
    if (reputation <= 10) return 20;
    if (reputation <= 100) return 50;
    if (reputation <= 1000) return 75;
    if (reputation <= 10000) return 90;
    if (reputation <= 100000) return 99;
    return 99.9;
  }

  private daysBetween(date1: Date, date2: Date): number {
    const diffTime = Math.abs(date2.getTime() - date1.getTime());
    return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
  }

  private determineActivityStatus(lastAccess: Date): string {
    const daysSinceAccess = this.daysBetween(lastAccess, new Date());
    if (daysSinceAccess < 30) return 'active';
    if (daysSinceAccess < 90) return 'regular';
    if (daysSinceAccess < 365) return 'occasional';
    return 'inactive';
  }

  private extractCountry(location: string | null): string | null {
    if (!location) return null;
    // Simple country extraction logic
    const countries = ['USA', 'United States', 'UK', 'United Kingdom', 'Canada', 'Germany', 'France', 'India', 'China', 'Japan', 'Brazil', 'Australia'];
    for (const country of countries) {
      if (location.toLowerCase().includes(country.toLowerCase())) {
        return country;
      }
    }
    // Check for last part after comma (often country)
    const parts = location.split(',');
    return parts.length > 1 ? parts[parts.length - 1].trim() : null;
  }

  private extractCity(location: string | null): string | null {
    if (!location) return null;
    const parts = location.split(',');
    return parts.length > 0 ? parts[0].trim() : null;
  }

  private normalizeLocation(location: string | null): string | null {
    if (!location) return null;
    return location.replace(/[^a-zA-Z0-9, ]/g, '').substring(0, 500);
  }

  private extractBioSummary(bio: string | null): string | null {
    if (!bio) return null;
    // Remove HTML tags
    const cleaned = bio.replace(/<[^>]*>/g, '');
    // Get first 200 characters
    return cleaned.substring(0, 200) + (cleaned.length > 200 ? '...' : '');
  }

  private countWords(text: string | null): number {
    if (!text) return 0;
    const cleaned = text.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ');
    return cleaned.split(' ').filter(word => word.length > 0).length;
  }

  private extractDomain(url: string | null): string | null {
    if (!url) return null;
    try {
      const urlObj = new URL(url.startsWith('http') ? url : `http://${url}`);
      return urlObj.hostname;
    } catch {
      return null;
    }
  }

  private validateUrl(url: string | null): boolean {
    if (!url) return false;
    try {
      new URL(url.startsWith('http') ? url : `http://${url}`);
      return true;
    } catch {
      return false;
    }
  }

  private calculateVoteRatio(upVotes: number, downVotes: number): number {
    if (downVotes === 0) return upVotes;
    return Number((upVotes / downVotes).toFixed(2));
  }

  private calculateEngagementScore(user: SourceUser): number {
    const totalVotes = user.UpVotes + user.DownVotes;
    const views = Math.max(user.Views, 1);
    return Number((totalVotes / views).toFixed(6));
  }
}