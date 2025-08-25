import { GPU } from 'gpu.js';
import Cursor from 'pg-cursor';
import { from as copyFrom } from 'pg-copy-streams';
import * as crypto from 'crypto';
import {
  sourcePool,
  targetPool,
  initializeTargetSchema,
  INPUT_TABLE_NAME,
} from '../config/database';
import { ResearchLogger } from '../core/logger';
import {
  SourceUser,
  TransformedUser,
  PerformanceMetrics,
} from '../core/types';
import { UserTransformer } from '../core/transformer';

/**
 * Case 5: GPU accelerated processing using gpu.js. This case focuses on
 * parallelising numeric transformations on the GPU while keeping string heavy
 * work on the CPU. Extraction and insertion borrow optimisations from Case 4.
 */
export class Case5GPU {
  private gpu: GPU;
  private transformKernel: any;
  private logger: ResearchLogger;
  private transformer: UserTransformer;
  private batchId: string;

  // Tuned parameters (inspired by Case 4 improvements)
  private readonly BATCH_SIZE = 10000; // optimal for GPU memory
  private readonly GPU_THREADS = 256;
  private readonly EXTRACTION_CONCURRENCY = 4;
  private readonly INSERTION_BATCH_SIZE = 5000;

  constructor() {
    this.logger = new ResearchLogger(5, 'GPU Accelerated Processing');
    this.transformer = new UserTransformer(5);
    this.batchId = this.generateBatchId();

    // Detect GPU and fallback to CPU if not available
    try {
      this.gpu = new GPU({ mode: 'gpu' });
      if (this.gpu.mode !== 'gpu') {
        this.gpu = new GPU({ mode: 'cpu' });
      }
    } catch {
      // In environments without GPU/WebGL support
      this.gpu = new GPU({ mode: 'cpu' });
    }

    this.initializeKernel();
  }

  /** Initialise the GPU kernel used for numeric transformations. */
  private initializeKernel(batchSize: number = this.BATCH_SIZE): void {
    const kernelFn = function (
      this: any,
      ids: Float32Array,
      reputations: Float32Array,
      views: Float32Array,
      upvotes: Float32Array,
      downvotes: Float32Array,
      creationDates: Float32Array,
      lastAccessDates: Float32Array,
      currentTime: number
    ) {
      const index = this.thread.x;

      let tier = 1;
      if (reputations[index] > 100000) tier = 5;
      else if (reputations[index] > 10000) tier = 4;
      else if (reputations[index] > 1000) tier = 3;
      else if (reputations[index] > 100) tier = 2;

      const totalVotes = upvotes[index] + downvotes[index];
      const engagement = totalVotes / Math.max(views[index], 1);
      const daysSinceAccess = (currentTime - lastAccessDates[index]) / 86400;

      // return [tier, engagement, daysSinceAccess]
      return [tier, engagement, daysSinceAccess];
    } as any;

    this.transformKernel = this.gpu
      .createKernel(kernelFn)
      .setOutput([batchSize])
      .setPipeline(false);
  }

  /** Execute the full ETL pipeline. */
  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      await initializeTargetSchema();
      await targetPool.query(
        'DELETE FROM transformed_users WHERE etl_case_number = 5'
      );

      // Get total row count
      const countRes = await sourcePool.query(
        `SELECT COUNT(*) FROM ${INPUT_TABLE_NAME}`
      );
      const totalCount = Math.min(
        parseInt(countRes.rows[0].count),
        limit || parseInt(countRes.rows[0].count)
      );

      this.logger.logPhaseStart('GPU Initialization');
      this.logger.logPhaseEnd('GPU Initialization');

      let offset = 0;
      const startTime = Date.now();
      let kernelTime = 0;
      let transferTime = 0;

      while (offset < totalCount) {
        const take = Math.min(this.BATCH_SIZE, totalCount - offset);

        // 1. Extract
        this.logger.logPhaseStart('extract batch');
        const batch = await this.extractBatchOptimized(offset, take);
        this.logger.logPhaseEnd('extract batch', batch.length);

        // 2. Prepare GPU data
        const prepStart = Date.now();
        const { numericData, stringData } = this.prepareGPUData(batch, take);
        transferTime += Date.now() - prepStart;

        // 3. Execute GPU kernel
        const kernelStart = Date.now();
        try {
          this.transformKernel.setOutput([batch.length]);
          const gpuResults = this.transformKernel(
            numericData.ids,
            numericData.reputations,
            numericData.views,
            numericData.upvotes,
            numericData.downvotes,
            numericData.creationDates,
            numericData.lastAccessDates,
            Date.now() / 1000
          ) as number[][];

          kernelTime += Date.now() - kernelStart;

          // 4. Merge GPU + CPU results
          const transformedUsers = this.mergeResults(
            gpuResults,
            numericData,
            stringData
          );

          // 5. Insert batch
          await this.optimizedBatchInsert(transformedUsers);

          processedCount += transformedUsers.length;

          if (processedCount % 10000 === 0) {
            this.logger.logProgress(processedCount, totalCount, 'GPU Pipeline');
          }

          if (processedCount % 50000 === 0) {
            this.cleanupGPUMemory();
            global.gc?.();
          }
        } catch (err: any) {
          // GPU specific errors fallback to CPU mode
          if (
            typeof err?.message === 'string' &&
            (err.message.includes('GPU') || err.message.includes('WebGL'))
          ) {
            this.logger.logError(err as Error, { fallback: 'CPU' });
            this.gpu.destroy();
            this.gpu = new GPU({ mode: 'cpu' });
            this.initializeKernel();
            offset -= take; // retry this batch with CPU mode
          } else {
            throw err;
          }
        }

        offset += take;
      }

      this.logger.logPhaseEnd('GPU Pipeline', processedCount);

      const totalTime = Date.now() - startTime;

      const metrics = await this.logger.finalizeMetrics(
        processedCount,
        errorCount
      );
      (metrics as any).gpu_device = this.gpu.mode;
      (metrics as any).gpu_threads = this.GPU_THREADS;
      (metrics as any).kernel_execution_time_ms = kernelTime;
      (metrics as any).memory_transfer_time_ms = transferTime;
      (metrics as any).gpu_memory_used_mb = 0; // gpu.js does not expose
      (metrics as any).parallel_efficiency = kernelTime
        ? Number(((totalTime / kernelTime) / this.GPU_THREADS).toFixed(4))
        : 0;

      metrics.batch_count = Math.ceil(processedCount / this.BATCH_SIZE);

      return metrics;
    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    }
  }

  /** Optimised extraction using cursor based streaming. */
  private async extractBatchOptimized(
    offset: number,
    limit: number
  ): Promise<SourceUser[]> {
    const client = await sourcePool.connect();
    try {
      await client.query(
        'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED'
      );
      await client.query('SET fetch_size = 10000');

      const cursor = client.query(
        new Cursor(
          `SELECT "Id","Reputation","Views","UpVotes","DownVotes",
                  "CreationDate","LastAccessDate","DisplayName",
                  "Location","AboutMe","WebsiteUrl"
           FROM ${INPUT_TABLE_NAME}
           OFFSET $1 LIMIT $2`,
          [offset, limit]
        )
      );

      const rows: any[] = await new Promise((resolve, reject) => {
        cursor.read(limit, (err: Error | null, rows: any[]) => {
          if (err) reject(err);
          else resolve(rows);
        });
      });
      return rows as SourceUser[];
    } finally {
      client.release();
    }
  }

  /** Prepare numeric data as typed arrays for the GPU and collect string data. */
  private prepareGPUData(batch: SourceUser[], actual: number) {
    const ids = new Float32Array(actual);
    const reputations = new Float32Array(actual);
    const views = new Float32Array(actual);
    const upvotes = new Float32Array(actual);
    const downvotes = new Float32Array(actual);
    const creationDates = new Float32Array(actual);
    const lastAccessDates = new Float32Array(actual);

    const stringData = {
      displayNames: new Array<string>(actual),
      locations: new Array<string | null>(actual),
      abouts: new Array<string | null>(actual),
      websites: new Array<string | null>(actual),
    };

    for (let i = 0; i < actual; i++) {
      const u = batch[i];
      ids[i] = u.Id;
      reputations[i] = u.Reputation;
      views[i] = u.Views;
      upvotes[i] = u.UpVotes;
      downvotes[i] = u.DownVotes;
      creationDates[i] = Date.parse(u.CreationDate) / 1000;
      lastAccessDates[i] = Date.parse(u.LastAccessDate) / 1000;

      stringData.displayNames[i] = u.DisplayName;
      stringData.locations[i] = u.Location;
      stringData.abouts[i] = u.AboutMe;
      stringData.websites[i] = u.WebsiteUrl;
    }

    return {
      numericData: {
        ids,
        reputations,
        views,
        upvotes,
        downvotes,
        creationDates,
        lastAccessDates,
      },
      stringData,
    };
  }

  /** Merge GPU results with CPU string processing to form TransformedUser objects. */
  private mergeResults(
    gpuResults: number[][],
    numericData: any,
    stringData: any
  ): TransformedUser[] {
    const now = new Date();
    const users: TransformedUser[] = [];

    for (let i = 0; i < gpuResults.length; i++) {
      if (!gpuResults[i]) continue;
      const tierNum = gpuResults[i][0];
      const engagement = gpuResults[i][1];
      const daysSinceAccess = gpuResults[i][2];

      const creationDate = new Date(numericData.creationDates[i] * 1000);
      const lastLogin = new Date(numericData.lastAccessDates[i] * 1000);
      const accountAgeDays = Math.floor(
        (now.getTime() - creationDate.getTime()) / 86400000
      );

      const reputationScore = numericData.reputations[i];
      const user: TransformedUser = {
        user_id: numericData.ids[i],
        username: this.sanitizeUsername(stringData.displayNames[i]),
        reputation_score: reputationScore,
        reputation_tier: this.tierToString(tierNum),
        reputation_percentile: this.estimatePercentile(reputationScore),
        registered_at: creationDate,
        last_login: lastLogin,
        account_age_days: accountAgeDays,
        activity_status: this.determineActivityStatus(daysSinceAccess),
        is_active: daysSinceAccess < 365,
        is_veteran: accountAgeDays > 1825,
        location_original: stringData.locations[i],
        location_country: this.extractCountry(stringData.locations[i]),
        location_city: this.extractCity(stringData.locations[i]),
        location_normalized: this.normalizeLocation(stringData.locations[i]),
        bio_original: stringData.abouts[i],
        bio_summary: this.extractBioSummary(stringData.abouts[i]),
        bio_wordcount: this.countWords(stringData.abouts[i]),
        bio_has_content:
          !!stringData.abouts[i] && stringData.abouts[i].length > 10,
        website_url: stringData.websites[i],
        website_domain: this.extractDomain(stringData.websites[i]),
        website_valid: this.validateUrl(stringData.websites[i]),
        profile_views: numericData.views[i],
        positive_votes: numericData.upvotes[i],
        negative_votes: numericData.downvotes[i],
        vote_ratio: this.calculateVoteRatio(
          numericData.upvotes[i],
          numericData.downvotes[i]
        ),
        engagement_score: engagement,
        metadata: {
          original_id: numericData.ids[i],
          import_timestamp: now.toISOString(),
          etl_version: '1.0.0',
          processing_case: 5,
          has_email: false,
          has_avatar: false,
        },
        etl_timestamp: now,
        etl_case_number: 5,
        etl_batch_id: this.batchId,
      };

      users.push(user);
    }

    return users;
  }

  /** Optimised batch insertion using COPY and CSV streaming. */
  private async optimizedBatchInsert(users: TransformedUser[]): Promise<void> {
    if (!users.length) return;

    const client = await targetPool.connect();
    const columns = [
      'user_id',
      'username',
      'reputation_score',
      'reputation_tier',
      'reputation_percentile',
      'registered_at',
      'last_login',
      'account_age_days',
      'activity_status',
      'is_active',
      'is_veteran',
      'location_original',
      'location_country',
      'location_city',
      'location_normalized',
      'bio_original',
      'bio_summary',
      'bio_wordcount',
      'bio_has_content',
      'website_url',
      'website_domain',
      'website_valid',
      'profile_views',
      'positive_votes',
      'negative_votes',
      'vote_ratio',
      'engagement_score',
      'metadata',
      'etl_timestamp',
      'etl_case_number',
      'etl_batch_id',
    ];

    try {
      await client.query('SET session_replication_role = replica');

      const stream = client.query(
        copyFrom(
          `COPY transformed_users (${columns.join(',')}) FROM STDIN WITH (FORMAT csv, HEADER false)`
        )
      );

      for (const user of users) {
        stream.write(this.userToCSV(user));
      }
      stream.end();

      await new Promise((resolve, reject) => {
        stream.on('finish', resolve);
        stream.on('error', reject);
      });

      await client.query('SET session_replication_role = DEFAULT');
    } finally {
      client.release();
    }
  }

  /** Convert a user object to a CSV row for COPY. */
  private userToCSV(u: TransformedUser): string {
    const esc = (v: any) => {
      if (v === null || v === undefined) return '';
      const str = typeof v === 'string' ? v : JSON.stringify(v);
      return '"' + str.replace(/"/g, '""') + '"';
    };

    return [
      u.user_id,
      esc(u.username),
      u.reputation_score,
      esc(u.reputation_tier),
      u.reputation_percentile,
      u.registered_at.toISOString(),
      u.last_login.toISOString(),
      u.account_age_days,
      esc(u.activity_status),
      u.is_active,
      u.is_veteran,
      esc(u.location_original),
      esc(u.location_country),
      esc(u.location_city),
      esc(u.location_normalized),
      esc(u.bio_original),
      esc(u.bio_summary),
      u.bio_wordcount,
      u.bio_has_content,
      esc(u.website_url),
      esc(u.website_domain),
      u.website_valid,
      u.profile_views,
      u.positive_votes,
      u.negative_votes,
      u.vote_ratio,
      u.engagement_score,
      esc(JSON.stringify(u.metadata)),
      u.etl_timestamp.toISOString(),
      u.etl_case_number,
      esc(u.etl_batch_id),
    ].join(',') + '\n';
  }

  /** Cleanup GPU memory between large batches. */
  private cleanupGPUMemory(): void {
    if (this.transformKernel) {
      this.transformKernel.destroy();
    }
    this.initializeKernel();
  }

  // ----- Helper methods borrowed from UserTransformer -----
  private generateBatchId(): string {
    return `case5_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
  }

  private sanitizeUsername(name: string): string {
    if (!name) return 'anonymous';
    return name.substring(0, 255).replace(/[^a-zA-Z0-9_.-]/g, '_');
    }

  private tierToString(tier: number): string {
    switch (Math.round(tier)) {
      case 5:
        return 'legendary';
      case 4:
        return 'platinum';
      case 3:
        return 'gold';
      case 2:
        return 'silver';
      default:
        return 'bronze';
    }
  }

  private estimatePercentile(reputation: number): number {
    if (reputation <= 1) return 0;
    if (reputation <= 10) return 20;
    if (reputation <= 100) return 50;
    if (reputation <= 1000) return 75;
    if (reputation <= 10000) return 90;
    if (reputation <= 100000) return 99;
    return 99.9;
  }

  private determineActivityStatus(daysSinceAccess: number): string {
    if (daysSinceAccess < 30) return 'active';
    if (daysSinceAccess < 90) return 'regular';
    if (daysSinceAccess < 365) return 'occasional';
    return 'inactive';
  }

  private extractCountry(location: string | null): string | null {
    if (!location) return null;
    const countries = [
      'USA',
      'United States',
      'UK',
      'United Kingdom',
      'Canada',
      'Germany',
      'France',
      'India',
      'China',
      'Japan',
      'Brazil',
      'Australia',
    ];
    for (const country of countries) {
      if (location.toLowerCase().includes(country.toLowerCase())) {
        return country;
      }
    }
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
    const cleaned = bio.replace(/<[^>]*>/g, '');
    return cleaned.substring(0, 200) + (cleaned.length > 200 ? '...' : '');
  }

  private countWords(text: string | null): number {
    if (!text) return 0;
    const cleaned = text
      .replace(/<[^>]*>/g, '')
      .replace(/\s+/g, ' ');
    return cleaned.split(' ').filter((w) => w.length > 0).length;
  }

  private extractDomain(url: string | null): string | null {
    if (!url) return null;
    try {
      const obj = new URL(url.startsWith('http') ? url : `http://${url}`);
      return obj.hostname;
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

  private calculateVoteRatio(up: number, down: number): number {
    if (down === 0) return up;
    return Number((up / down).toFixed(2));
  }
}

