
interface SourceUser {
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
}

interface TransformedUser {
  user_id: number;
  username: string;
  reputation_score: number;
  reputation_tier: string;
  reputation_percentile: number;
  registered_at: string;
  last_login: string;
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
  etl_timestamp: string;
  etl_case_number: number;
  etl_batch_id: string;
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    try {
      let text: string;
      const enc = (request.headers.get('content-encoding') || '').toLowerCase();
      if (enc.includes('gzip')) {
        // @ts-ignore - DecompressionStream is available in Workers runtime
        const ds = new DecompressionStream('gzip');
        const decompressed = (request.body as ReadableStream).pipeThrough(ds);
        text = await new Response(decompressed).text();
      } else {
        text = await request.text();
      }

      const { users, caseNumber } = JSON.parse(text) as { users: SourceUser[], caseNumber: number };

      const transformedUsers = users.map(user => transformUser(user, caseNumber));
      
      return new Response(JSON.stringify({ 
        transformedUsers,
        processed: transformedUsers.length,
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
      
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: 'Processing failed',
        message: (error as any)?.message || String(error)
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
} satisfies ExportedHandler<Env>;

function transformUser(source: SourceUser, caseNumber: number): TransformedUser {
  const now = new Date();
  const creationDate = new Date(source.CreationDate);
  const lastAccessDate = new Date(source.LastAccessDate);
  const batchId = `edge_${caseNumber}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  return {
    user_id: source.Id,
    username: sanitizeUsername(source.DisplayName),
    reputation_score: source.Reputation,
    reputation_tier: calculateReputationTier(source.Reputation),
    reputation_percentile: estimatePercentile(source.Reputation),
    registered_at: creationDate.toISOString(),
    last_login: lastAccessDate.toISOString(),
    account_age_days: daysBetween(creationDate, now),
    activity_status: determineActivityStatus(lastAccessDate),
    is_active: daysBetween(lastAccessDate, now) < 365,
    is_veteran: daysBetween(creationDate, now) > 1825,
    location_original: source.Location,
    location_country: extractCountry(source.Location),
    location_city: extractCity(source.Location),
    location_normalized: normalizeLocation(source.Location),
    bio_original: source.AboutMe,
    bio_summary: extractBioSummary(source.AboutMe),
    bio_wordcount: countWords(source.AboutMe),
    bio_has_content: !!source.AboutMe && source.AboutMe.length > 10,
    website_url: source.WebsiteUrl,
    website_domain: extractDomain(source.WebsiteUrl),
    website_valid: validateUrl(source.WebsiteUrl),
    profile_views: source.Views || 0,
    positive_votes: source.UpVotes || 0,
    negative_votes: source.DownVotes || 0,
    vote_ratio: calculateVoteRatio(source.UpVotes, source.DownVotes),
    engagement_score: calculateEngagementScore(source),
    metadata: {
      original_id: source.Id,
      import_timestamp: now.toISOString(),
      etl_version: '1.0.0',
      processing_case: caseNumber,
      edge_worker: true
    },
    etl_timestamp: now.toISOString(),
    etl_case_number: caseNumber,
    etl_batch_id: batchId
  };
}

// Helper functions (same as in transformer.ts but simplified for edge)
function sanitizeUsername(name: string): string {
  if (!name) return 'anonymous';
  return name.substring(0, 255).replace(/[^a-zA-Z0-9_.-]/g, '_');
}

function calculateReputationTier(reputation: number): string {
  if (reputation <= 100) return 'bronze';
  if (reputation <= 1000) return 'silver';
  if (reputation <= 10000) return 'gold';
  if (reputation <= 100000) return 'platinum';
  return 'legendary';
}

function estimatePercentile(reputation: number): number {
  if (reputation <= 1) return 0;
  if (reputation <= 10) return 20;
  if (reputation <= 100) return 50;
  if (reputation <= 1000) return 75;
  if (reputation <= 10000) return 90;
  if (reputation <= 100000) return 99;
  return 99.9;
}

function daysBetween(date1: Date, date2: Date): number {
  const diffTime = Math.abs(date2.getTime() - date1.getTime());
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

function determineActivityStatus(lastAccess: Date): string {
  const daysSinceAccess = daysBetween(lastAccess, new Date());
  if (daysSinceAccess < 30) return 'active';
  if (daysSinceAccess < 90) return 'regular';
  if (daysSinceAccess < 365) return 'occasional';
  return 'inactive';
}

function extractCountry(location: string | null): string | null {
  if (!location) return null;
  const parts = location.split(',');
  return parts.length > 1 ? parts[parts.length - 1].trim() : null;
}

function extractCity(location: string | null): string | null {
  if (!location) return null;
  const parts = location.split(',');
  return parts.length > 0 ? parts[0].trim() : null;
}

function normalizeLocation(location: string | null): string | null {
  if (!location) return null;
  return location.replace(/[^a-zA-Z0-9, ]/g, '').substring(0, 500);
}

function extractBioSummary(bio: string | null): string | null {
  if (!bio) return null;
  const cleaned = bio.replace(/<[^>]*>/g, '');
  return cleaned.substring(0, 200) + (cleaned.length > 200 ? '...' : '');
}

function countWords(text: string | null): number {
  if (!text) return 0;
  const cleaned = text.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ');
  return cleaned.split(' ').filter(word => word.length > 0).length;
}

function extractDomain(url: string | null): string | null {
  if (!url) return null;
  try {
    const urlObj = new URL(url.startsWith('http') ? url : `http://${url}`);
    return urlObj.hostname;
  } catch {
    return null;
  }
}

function validateUrl(url: string | null): boolean {
  if (!url) return false;
  try {
    new URL(url.startsWith('http') ? url : `http://${url}`);
    return true;
  } catch {
    return false;
  }
}

function calculateVoteRatio(upVotes: number, downVotes: number): number {
  if (downVotes === 0) return upVotes;
  return Number((upVotes / downVotes).toFixed(2));
}

function calculateEngagementScore(user: SourceUser): number {
  const totalVotes = user.UpVotes + user.DownVotes;
  const views = Math.max(user.Views, 1);
  return Number((totalVotes / views).toFixed(6));
}

// export default {
// 	async fetch(request, env, ctx): Promise<Response> {
// 		return new Response('Hello World!');
// 	},
// } satisfies ExportedHandler<Env>;
