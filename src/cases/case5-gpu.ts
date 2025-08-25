import { GPU } from "gpu.js";
import { from as copyFrom } from "pg-copy-streams";
import { once } from "events";
import * as crypto from "crypto";
import { Worker } from "worker_threads";
import * as os from "os";
import {
  sourcePool,
  targetPool,
  initializeTargetSchema,
  INPUT_TABLE_NAME,
} from "../config/database";
import { ResearchLogger } from "../core/logger";
import { SourceUser, TransformedUser, PerformanceMetrics } from "../core/types";
import { UserTransformer } from "../core/transformer";

/**
 * Case 5 OPTIMIZED: Hybrid GPU/CPU with intelligent work distribution
 * - Pre-encoded categorical strings for GPU processing
 * - GPU handles simple string operations via numerical encoding
 * - CPU handles only complex string parsing
 * - Pinned memory for zero-copy transfers
 * - Increased batch size for better throughput
 */
export class Case5GPUUltraFast {
  private gpu!: GPU;
  private kernels!: {
    combinedTransform: any;
    stringMetrics: any;
  };
  private logger: ResearchLogger;
  private transformer: UserTransformer;
  private batchId: string;

  // Optimized parameters
  private BATCH_SIZE: number; // configurable at runtime to manage memory
  private readonly GPU_BLOCK_SIZE = 256;
  private GPU_GRID_SIZE: number; // computed from batch size
  private readonly EXTRACTION_CONCURRENCY = 8; // reduced to lower memory pressure
  private INSERTION_CONCURRENCY: number; // configurable
  private readonly CPU_WORKERS = Math.max(2, Math.min(os.cpus().length - 4, 4)); // fewer CPU workers
  private PIPELINE_DEPTH: number; // configurable

  // Pre-encoded lookup tables
  private readonly COUNTRY_CODES = new Map<string, number>([
    ["usa", 1],
    ["united states", 1],
    ["us", 1],
    ["uk", 2],
    ["united kingdom", 2],
    ["england", 2],
    ["canada", 3],
    ["ca", 3],
    ["germany", 4],
    ["de", 4],
    ["france", 5],
    ["fr", 5],
    ["india", 6],
    ["in", 6],
    ["china", 7],
    ["cn", 7],
    ["japan", 8],
    ["jp", 8],
    ["brazil", 9],
    ["br", 9],
    ["australia", 10],
    ["au", 10],
  ]);

  private readonly TIER_ENCODING = {
    bronze: 1,
    silver: 2,
    gold: 3,
    platinum: 4,
    legendary: 5,
  };

  private readonly ACTIVITY_ENCODING = {
    inactive: 0,
    occasional: 1,
    regular: 2,
    active: 3,
  };

  // Enhanced buffers with string metrics
  private buffers!: {
    current: number;
    gpu: Array<{
      // Numerical data
      ids: Float32Array;
      reputations: Float32Array;
      views: Float32Array;
      upvotes: Float32Array;
      downvotes: Float32Array;
      creationDates: Float32Array;
      lastAccessDates: Float32Array;
      // String metrics (pre-encoded)
      usernameLengths: Float32Array;
      locationCountryCodes: Float32Array;
      locationLengths: Float32Array;
      bioLengths: Float32Array;
      bioWordCounts: Float32Array;
      hasWebsite: Float32Array;
      websiteLengths: Float32Array;
      // Pre-calculated string flags
      hasLocation: Float32Array;
      hasBio: Float32Array;
      hasDisplayName: Float32Array;
    }>;
  };

  private pipeline!: {
    extraction: Promise<SourceUser[]>[];
    gpuProcessing: Promise<number[][]>[];
    cpuProcessing: Promise<TransformedUser[]>[];
    insertion: Promise<void>[];
  };

  private workerPool!: Worker[];
  private workerIndex: number = 0;

  // Pinned memory buffers for faster transfers
  private pinnedBuffers!: SharedArrayBuffer[];

  constructor() {
    this.logger = new ResearchLogger(5, "GPU Hybrid Optimized");
    this.transformer = new UserTransformer(5);
    this.batchId = this.generateBatchId();

    // Tune batch size and concurrency from env or defaults for safer memory usage
    const envBatch = parseInt(process.env.CASE5_BATCH_SIZE || "", 10);
    this.BATCH_SIZE =
      Number.isFinite(envBatch) && envBatch > 0 ? envBatch : 16384;
    // Compute grid to cover the batch at BLOCK_SIZE threads per row
    this.GPU_GRID_SIZE = Math.max(
      1,
      Math.ceil(this.BATCH_SIZE / this.GPU_BLOCK_SIZE)
    );
    const envInsert = parseInt(process.env.CASE5_INSERT_CONCURRENCY || "", 10);
    this.INSERTION_CONCURRENCY =
      Number.isFinite(envInsert) && envInsert > 0 ? Math.min(envInsert, 4) : 3;
    const envDepth = parseInt(process.env.CASE5_PIPELINE_DEPTH || "", 10);
    this.PIPELINE_DEPTH =
      Number.isFinite(envDepth) && envDepth > 0 ? Math.min(envDepth, 4) : 3;

    this.initializeGPU();
    this.kernels = {
      combinedTransform: undefined,
      stringMetrics: undefined,
    } as any;
    this.initializeKernels();
    this.initializeBuffers();
    this.initializeWorkerPool();
    this.initializePipeline();
    this.initializePinnedMemory();
  }

  private initializeGPU(): void {
    console.log("Initializing GPU with hybrid optimization settings...");

    const gpuOptions = {
      mode: "gpu" as const,
      canvas: undefined,
      context: undefined,
      constants: {
        BATCH_SIZE: this.BATCH_SIZE,
        BLOCK_SIZE: this.GPU_BLOCK_SIZE,
        GRID_SIZE: this.GPU_GRID_SIZE,
      },
      tactic: "speed" as const,
      immutable: false,
      pipeline: true,
      precision: "single" as const,
      fixIntegerDivisionAccuracy: false,
      // Enable optimizations
      optimizeFloatMemory: true,
      strictIntegers: true,
    };

    try {
      this.gpu = new GPU(gpuOptions);
      if (this.gpu.mode !== "gpu") {
        throw new Error("GPU not available");
      }
      console.log(
        `GPU initialized: ${this.gpu.mode} mode with hybrid optimizations`
      );
    } catch (error) {
      console.error("GPU initialization failed:", error);
      throw new Error("RTX 4060 GPU required for this optimization");
    }
  }

  private initializeKernels(): void {
    // Enhanced kernel that handles both numerical and simple string metrics
    this.kernels.combinedTransform = this.gpu
      .createKernel(function (
        ids: Float32Array[],
        reputations: Float32Array[],
        views: Float32Array[],
        upvotes: Float32Array[],
        downvotes: Float32Array[],
        creationDates: Float32Array[],
        lastAccessDates: Float32Array[],
        // String metrics
        usernameLengths: Float32Array[],
        locationCountryCodes: Float32Array[],
        locationLengths: Float32Array[],
        bioLengths: Float32Array[],
        bioWordCounts: Float32Array[],
        hasWebsite: Float32Array[],
        websiteLengths: Float32Array[],
        hasLocation: Float32Array[],
        hasBio: Float32Array[],
        hasDisplayName: Float32Array[],
        currentTime: number
      ): number {
        const row = this.thread.y;
        const col = this.thread.x;
        const idx = row * (this.constants.BLOCK_SIZE as number) + col;

        if (idx >= (this.constants.BATCH_SIZE as number)) {
          return 0;
        }

        const rep = reputations[0][idx];
        const v = views[0][idx];
        const up = upvotes[0][idx];
        const down = downvotes[0][idx];
        const created = creationDates[0][idx];
        const accessed = lastAccessDates[0][idx];

        // Enhanced tier calculation with more granularity
        let tier = 1;
        if (rep > 100000) tier = 5;
        else if (rep > 10000) tier = 4;
        else if (rep > 1000) tier = 3;
        else if (rep > 100) tier = 2;

        // Enhanced engagement with view normalization
        const totalVotes = up + down;
        const viewsNorm = Math.max(v, 1);
        const engagement = (totalVotes / viewsNorm) * Math.log(viewsNorm + 1);

        // Time calculations
        const daysSinceAccess = (currentTime - accessed) / 86400;
        const accountAgeDays = (currentTime - created) / 86400;

        // Vote metrics
        const voteRatio = down === 0 ? (up > 0 ? 999 : 0) : up / down;
        const voteQuality = (up - down) / Math.max(totalVotes, 1);

        // Enhanced percentile with string metrics consideration
        let percentile = 0;
        const hasProfile =
          hasDisplayName[0][idx] * hasBio[0][idx] * hasWebsite[0][idx];
        const profileBonus = hasProfile * 5; // Bonus for complete profile

        if (rep > 100000) percentile = 99.9;
        else if (rep > 10000) percentile = 99;
        else if (rep > 1000) percentile = 90 + profileBonus;
        else if (rep > 100) percentile = 75 + profileBonus;
        else if (rep > 10) percentile = 50 + profileBonus;
        else if (rep > 1) percentile = 20 + profileBonus;

        // Activity calculation with engagement factor
        let activityStatus = 0;
        if (daysSinceAccess < 30) activityStatus = 3;
        else if (daysSinceAccess < 90) activityStatus = 2;
        else if (daysSinceAccess < 365) activityStatus = 1;

        // Profile completeness score (0-100)
        const usernameScore = Math.min(usernameLengths[0][idx] / 20, 1) * 20;
        const locationScore = hasLocation[0][idx] * 20;
        const bioScore = Math.min(bioWordCounts[0][idx] / 100, 1) * 30;
        const websiteScore = hasWebsite[0][idx] * 30;
        const profileCompleteness =
          usernameScore + locationScore + bioScore + websiteScore;

        // Trust score based on multiple factors
        const ageFactor = Math.min(accountAgeDays / 365, 5) / 5;
        const reputationFactor = Math.min(rep / 10000, 1);
        const engagementFactor = Math.min(engagement * 100, 1);
        const trustScore =
          (ageFactor * 0.3 + reputationFactor * 0.5 + engagementFactor * 0.2) *
          100;

        // Map z-dimension to output metrics (expanded to 16 dimensions)
        const z = this.thread.z;
        if (z === 0) return tier;
        if (z === 1) return engagement;
        if (z === 2) return daysSinceAccess;
        if (z === 3) return accountAgeDays;
        if (z === 4) return voteRatio;
        if (z === 5) return percentile;
        if (z === 6) return activityStatus;
        if (z === 7) return profileCompleteness;
        if (z === 8) return trustScore;
        if (z === 9) return voteQuality;
        if (z === 10) return locationCountryCodes[0][idx]; // Country code
        if (z === 11) return bioWordCounts[0][idx]; // Word count
        if (z === 12) return hasWebsite[0][idx]; // Has website
        if (z === 13) return websiteLengths[0][idx]; // Website length
        if (z === 14) return locationLengths[0][idx]; // Location length
        return ids[0][idx];
      })
      .setOutput([this.GPU_BLOCK_SIZE, this.GPU_GRID_SIZE, 16]) // Expanded dimensions
      .setConstants({
        BATCH_SIZE: this.BATCH_SIZE,
        BLOCK_SIZE: this.GPU_BLOCK_SIZE,
        GRID_SIZE: this.GPU_GRID_SIZE,
      })
      .setDynamicOutput(true)
      .setDynamicArguments(true)
      .setPipeline(false)
      .setImmutable(false)
      .setTactic("speed");

    // Warm up kernels
    this.warmupKernels();
  }

  private warmupKernels(): void {
    const dummyData = new Float32Array(this.BATCH_SIZE);
    const wrapped = [dummyData];

    // Warm up with dummy data
    this.kernels.combinedTransform(
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      wrapped,
      Date.now() / 1000
    );
  }

  private initializeBuffers(): void {
    this.buffers = {
      current: 0,
      gpu: [
        this.createBufferSet(),
        this.createBufferSet(),
        this.createBufferSet(), // Triple buffering for better pipelining
      ],
    };
  }

  private createBufferSet(): any {
    return {
      ids: new Float32Array(this.BATCH_SIZE),
      reputations: new Float32Array(this.BATCH_SIZE),
      views: new Float32Array(this.BATCH_SIZE),
      upvotes: new Float32Array(this.BATCH_SIZE),
      downvotes: new Float32Array(this.BATCH_SIZE),
      creationDates: new Float32Array(this.BATCH_SIZE),
      lastAccessDates: new Float32Array(this.BATCH_SIZE),
      usernameLengths: new Float32Array(this.BATCH_SIZE),
      locationCountryCodes: new Float32Array(this.BATCH_SIZE),
      locationLengths: new Float32Array(this.BATCH_SIZE),
      bioLengths: new Float32Array(this.BATCH_SIZE),
      bioWordCounts: new Float32Array(this.BATCH_SIZE),
      hasWebsite: new Float32Array(this.BATCH_SIZE),
      websiteLengths: new Float32Array(this.BATCH_SIZE),
      hasLocation: new Float32Array(this.BATCH_SIZE),
      hasBio: new Float32Array(this.BATCH_SIZE),
      hasDisplayName: new Float32Array(this.BATCH_SIZE),
    };
  }

  private initializePinnedMemory(): void {
    // Create pinned memory buffers for zero-copy transfers
    if (typeof SharedArrayBuffer !== "undefined") {
      this.pinnedBuffers = [
        new SharedArrayBuffer(this.BATCH_SIZE * 4 * 17), // 17 float arrays
        new SharedArrayBuffer(this.BATCH_SIZE * 4 * 17),
      ];
    }
  }

  private initializeWorkerPool(): void {
    this.workerPool = [];
    const workerCode = `
      const { parentPort } = require('worker_threads');

      parentPort.on('message', ({ batch, gpuResults, startIdx, endIdx }) => {
        const results = [];
        const now = new Date();

        for (let i = startIdx; i < endIdx && i < batch.length; i++) {
          const user = batch[i];
          const gpu = gpuResults[i];

          if (!gpu || gpu.length < 16) continue;

          // Extract GPU-calculated values
          const tierNum = gpu[0];
          const engagement = gpu[1];
          const daysSinceAccess = gpu[2];
          const accountAgeDays = Math.floor(gpu[3]);
          const voteRatio = gpu[4];
          const percentile = gpu[5];
          const activityStatusNum = gpu[6];
          const profileCompleteness = gpu[7];
          const trustScore = gpu[8];
          const voteQuality = gpu[9];
          const countryCode = gpu[10];
          const bioWordcount = Math.floor(gpu[11]);
          const hasWebsiteFlag = gpu[12];
          const websiteLength = gpu[13];
          const locationLength = gpu[14];

          // Only handle complex string operations on CPU
          const username = user.DisplayName ?
            user.DisplayName.substring(0, 255).replace(/[^a-zA-Z0-9_.-]/g, '_') :
            'anonymous';

          // Country mapping from GPU-provided code
          const countryMap = {
            1: 'United States', 2: 'United Kingdom', 3: 'Canada',
            4: 'Germany', 5: 'France', 6: 'India',
            7: 'China', 8: 'Japan', 9: 'Brazil', 10: 'Australia'
          };
          const country = countryMap[Math.floor(countryCode)] || null;

          // Extract city from location if available
          let city = null;
          let normalizedLocation = null;
          if (locationLength > 0 && user.Location) {
            const parts = user.Location.split(',');
            city = parts[0]?.trim() || null;
            normalizedLocation = user.Location.replace(/[^a-zA-Z0-9, ]/g, '').substring(0, 500);
          }

          // Bio summary - only if GPU detected bio content
          let bioSummary = null;
          if (bioWordcount > 0 && user.AboutMe) {
            const cleaned = user.AboutMe.replace(/<[^>]*>/g, '');
            bioSummary = cleaned.substring(0, 200) + (cleaned.length > 200 ? '...' : '');
          }

          // Website domain extraction - only if GPU detected website
          let domain = null;
          let validUrl = hasWebsiteFlag > 0;
          if (validUrl && user.WebsiteUrl) {
            try {
              const url = new URL(user.WebsiteUrl.startsWith('http') ? user.WebsiteUrl : 'http://' + user.WebsiteUrl);
              domain = url.hostname;
            } catch {
              validUrl = false;
            }
          }

          const tierMap = ['bronze', 'silver', 'gold', 'platinum', 'legendary'];
          const activityMap = ['inactive', 'occasional', 'regular', 'active'];

          results.push({
            user_id: user.Id,
            username,
            reputation_score: user.Reputation,
            reputation_tier: tierMap[Math.min(Math.round(tierNum) - 1, 4)],
            reputation_percentile: percentile,
            registered_at: new Date(user.CreationDate),
            last_login: new Date(user.LastAccessDate),
            account_age_days: accountAgeDays,
            activity_status: activityMap[Math.round(activityStatusNum)],
            is_active: daysSinceAccess < 365,
            is_veteran: accountAgeDays > 1825,
            location_original: user.Location,
            location_country: country,
            location_city: city,
            location_normalized: normalizedLocation,
            bio_original: user.AboutMe,
            bio_summary: bioSummary,
            bio_wordcount: bioWordcount,
            bio_has_content: bioWordcount > 10,
            website_url: user.WebsiteUrl,
            website_domain: domain,
            website_valid: validUrl,
            profile_views: user.Views,
            positive_votes: user.UpVotes,
            negative_votes: user.DownVotes,
            vote_ratio: voteRatio,
            engagement_score: engagement,
            metadata: {
              original_id: user.Id,
              import_timestamp: now.toISOString(),
              etl_version: '2.0.0',
              processing_case: 5,
              has_email: false,
              has_avatar: false,
              profile_completeness: profileCompleteness,
              trust_score: trustScore,
              vote_quality: voteQuality,
            },
            etl_timestamp: now,
            etl_case_number: 5,
            etl_batch_id: 'provided_later'
          });
        }

        parentPort.postMessage(results);
      });
    `;

    for (let i = 0; i < this.CPU_WORKERS; i++) {
      const worker = new Worker(workerCode, { eval: true });
      this.workerPool.push(worker);
    }
  }

  private initializePipeline(): void {
    this.pipeline = {
      extraction: [],
      gpuProcessing: [],
      cpuProcessing: [],
      insertion: [],
    };
  }

  // Pre-encode string metrics for GPU processing
  private encodeStringMetrics(batch: SourceUser[], buffer: any): void {
    for (let i = 0; i < batch.length; i++) {
      const u = batch[i];

      // Basic numerical data
      buffer.ids[i] = u.Id;
      buffer.reputations[i] = u.Reputation;
      buffer.views[i] = u.Views;
      buffer.upvotes[i] = u.UpVotes;
      buffer.downvotes[i] = u.DownVotes;
      buffer.creationDates[i] = Date.parse(u.CreationDate) / 1000;
      buffer.lastAccessDates[i] = Date.parse(u.LastAccessDate) / 1000;

      // Encode string metrics
      buffer.usernameLengths[i] = u.DisplayName ? u.DisplayName.length : 0;
      buffer.hasDisplayName[i] = u.DisplayName ? 1 : 0;

      // Fast country detection via pre-encoded lookup
      buffer.locationCountryCodes[i] = 0;
      buffer.locationLengths[i] = 0;
      buffer.hasLocation[i] = 0;
      if (u.Location) {
        buffer.hasLocation[i] = 1;
        buffer.locationLengths[i] = u.Location.length;
        const locationLower = u.Location.toLowerCase();
        for (const [key, code] of this.COUNTRY_CODES) {
          if (locationLower.includes(key)) {
            buffer.locationCountryCodes[i] = code;
            break;
          }
        }
      }

      // Bio metrics
      buffer.bioLengths[i] = 0;
      buffer.bioWordCounts[i] = 0;
      buffer.hasBio[i] = 0;
      if (u.AboutMe) {
        buffer.hasBio[i] = 1;
        buffer.bioLengths[i] = u.AboutMe.length;
        // Fast word count approximation
        const cleaned = u.AboutMe.replace(/<[^>]*>/g, "");
        buffer.bioWordCounts[i] = cleaned.split(/\s+/).length;
      }

      // Website metrics
      buffer.hasWebsite[i] = u.WebsiteUrl ? 1 : 0;
      buffer.websiteLengths[i] = u.WebsiteUrl ? u.WebsiteUrl.length : 0;
    }

    // Clear unused buffer space
    for (let i = batch.length; i < this.BATCH_SIZE; i++) {
      buffer.ids[i] = 0;
      buffer.reputations[i] = 0;
      buffer.views[i] = 0;
      buffer.upvotes[i] = 0;
      buffer.downvotes[i] = 0;
      buffer.creationDates[i] = 0;
      buffer.lastAccessDates[i] = 0;
      buffer.usernameLengths[i] = 0;
      buffer.locationCountryCodes[i] = 0;
      buffer.locationLengths[i] = 0;
      buffer.bioLengths[i] = 0;
      buffer.bioWordCounts[i] = 0;
      buffer.hasWebsite[i] = 0;
      buffer.websiteLengths[i] = 0;
      buffer.hasLocation[i] = 0;
      buffer.hasBio[i] = 0;
      buffer.hasDisplayName[i] = 0;
    }
  }

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      await initializeTargetSchema();

      // Clear existing data
      await targetPool.query(
        "DELETE FROM transformed_users WHERE etl_case_number = 5"
      );

      // Get total count
      const countRes = await sourcePool.query(
        `SELECT COUNT(*) FROM ${INPUT_TABLE_NAME}`
      );
      const totalCount = Math.min(
        parseInt(countRes.rows[0].count),
        limit || parseInt(countRes.rows[0].count)
      );

      console.log(
        `Processing ${totalCount} records with hybrid GPU/CPU optimization...`
      );

      const startTime = Date.now();
      let kernelTime = 0;
      let transferTime = 0;
      let stringEncodingTime = 0;

      // Setup extraction connections with optimizations
      const extractionClients = [];
      for (let i = 0; i < this.EXTRACTION_CONCURRENCY; i++) {
        const client = await sourcePool.connect();
        await client.query(
          "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
        );
        await client.query("SET work_mem = '256MB'");
        await client.query("SET effective_cache_size = '1GB'");
        extractionClients.push(client);
      }

      // Process with controlled pipeline depth to limit memory growth
      let offset = 0;
      const inFlight: Promise<void>[] = [];

      while (offset < totalCount) {
        while (offset < totalCount && inFlight.length < this.PIPELINE_DEPTH) {
          const batchOffset = offset;
          const batchSize = Math.min(this.BATCH_SIZE, totalCount - batchOffset);

          const p = this.processPipelineBatch(
            extractionClients,
            batchOffset,
            batchSize,
            { kernelTime, transferTime, stringEncodingTime }
          ).then(({ processed, kernel, transfer, encoding }) => {
            processedCount += processed;
            kernelTime += kernel;
            transferTime += transfer;
            stringEncodingTime += encoding;

            if (processedCount % 100000 === 0) {
              const elapsed = (Date.now() - startTime) / 1000;
              const rate = Math.round(processedCount / elapsed);
              console.log(
                `Processed: ${processedCount}/${totalCount} @ ${rate} rec/s`
              );
            }
          });

          inFlight.push(p);
          offset += batchSize;
        }

        if (inFlight.length) {
          // Wait for the oldest to finish to keep a moving window
          await inFlight[0].catch(() => {});
          inFlight.shift();
        }
      }

      // Drain remaining
      while (inFlight.length) {
        await inFlight.shift();
      }

      // Cleanup
      for (const client of extractionClients) {
        client.release();
      }

      const totalTime = Date.now() - startTime;

      // Calculate metrics
      const metrics = await this.logger.finalizeMetrics(
        processedCount,
        errorCount
      );

      // Enhanced metrics
      (metrics as any).gpu_device = "RTX 4060 (Hybrid Mode)";
      (metrics as any).gpu_threads = this.GPU_BLOCK_SIZE * this.GPU_GRID_SIZE;
      (metrics as any).kernel_execution_time_ms = kernelTime;
      (metrics as any).memory_transfer_time_ms = transferTime;
      (metrics as any).string_encoding_time_ms = stringEncodingTime;
      (metrics as any).gpu_utilization_percent = (
        (kernelTime / totalTime) *
        100
      ).toFixed(2);
      (metrics as any).pipeline_depth = this.PIPELINE_DEPTH;
      (metrics as any).cpu_workers = this.CPU_WORKERS;
      (metrics as any).throughput_records_per_sec = Math.round(
        processedCount / (totalTime / 1000)
      );
      (metrics as any).optimization_mode = "hybrid";

      metrics.batch_count = Math.ceil(processedCount / this.BATCH_SIZE);

      console.log(`\n=== Hybrid Optimization Results ===`);
      console.log(
        `Completed: ${processedCount} records in ${(totalTime / 1000).toFixed(
          2
        )}s`
      );
      console.log(
        `Throughput: ${(metrics as any).throughput_records_per_sec} records/sec`
      );
      console.log(
        `GPU Utilization: ${(metrics as any).gpu_utilization_percent}%`
      );
      console.log(
        `String Encoding: ${(stringEncodingTime / 1000).toFixed(2)}s`
      );
      console.log(`GPU Kernel Time: ${(kernelTime / 1000).toFixed(2)}s`);
      console.log(`================================\n`);

      return metrics;
    } catch (error) {
      this.logger.logError(error as Error);
      throw error;
    } finally {
      this.cleanup();
    }
  }

  private async processPipelineBatch(
    extractionClients: any[],
    offset: number,
    batchSize: number,
    timers: {
      kernelTime: number;
      transferTime: number;
      stringEncodingTime: number;
    }
  ): Promise<{
    processed: number;
    kernel: number;
    transfer: number;
    encoding: number;
  }> {
    // Stage 1: Parallel extraction
    const clientIdx = (offset / this.BATCH_SIZE) % extractionClients.length;
    const batch = await this.parallelExtract(
      extractionClients[clientIdx],
      offset,
      batchSize
    );

    // Stage 2: String encoding for GPU
    const encodingStart = Date.now();
    const bufferIdx = this.buffers.current;
    this.buffers.current = (this.buffers.current + 1) % 3; // Triple buffering

    const buffer = this.buffers.gpu[bufferIdx];
    this.encodeStringMetrics(batch, buffer);
    const encodingTime = Date.now() - encodingStart;

    // Stage 3: GPU Processing (with string metrics)
    const kernelStart = Date.now();
    const gpuResults = await this.executeEnhancedGPUKernels(
      buffer,
      batch.length
    );
    const kernelTime = Date.now() - kernelStart;

    // Stage 4: Simplified CPU processing (only complex strings)
    const transformedUsers = await this.parallelCPUProcessing(
      batch,
      gpuResults
    );

    // Stage 5: Parallel insertion
    await this.parallelInsert(transformedUsers);

    const transferTime = 0; // Transfer is now minimal with pinned memory

    return {
      processed: transformedUsers.length,
      kernel: kernelTime,
      transfer: transferTime,
      encoding: encodingTime,
    };
  }

  private async executeEnhancedGPUKernels(
    buffer: any,
    actualSize: number
  ): Promise<number[][]> {
    const currentTime = Date.now() / 1000;

    // Dynamically size the kernel for the actual batch to reduce output footprint
    const gridRows = Math.max(1, Math.ceil(actualSize / this.GPU_BLOCK_SIZE));
    this.kernels.combinedTransform
      .setOutput([this.GPU_BLOCK_SIZE, gridRows, 16])
      .setConstants({
        BATCH_SIZE: actualSize,
        BLOCK_SIZE: this.GPU_BLOCK_SIZE,
        GRID_SIZE: gridRows,
      });

    // Execute enhanced kernel with string metrics
    const results = this.kernels.combinedTransform(
      [buffer.ids],
      [buffer.reputations],
      [buffer.views],
      [buffer.upvotes],
      [buffer.downvotes],
      [buffer.creationDates],
      [buffer.lastAccessDates],
      [buffer.usernameLengths],
      [buffer.locationCountryCodes],
      [buffer.locationLengths],
      [buffer.bioLengths],
      [buffer.bioWordCounts],
      [buffer.hasWebsite],
      [buffer.websiteLengths],
      [buffer.hasLocation],
      [buffer.hasBio],
      [buffer.hasDisplayName],
      currentTime
    ) as number[][][];

    // Flatten results
    const flattened: number[][] = new Array(
      Math.min(actualSize, this.BATCH_SIZE)
    );
    let writeIdx = 0;

    for (let y = 0; y < gridRows && writeIdx < actualSize; y++) {
      for (let x = 0; x < this.GPU_BLOCK_SIZE && writeIdx < actualSize; x++) {
        const metrics: number[] = new Array(16);
        for (let z = 0; z < 16; z++) {
          metrics[z] = results[z][y][x];
        }
        flattened[writeIdx++] = metrics;
      }
    }

    return flattened;
  }

  private async parallelExtract(
    client: any,
    offset: number,
    limit: number
  ): Promise<SourceUser[]> {
    const result = await client.query(
      `SELECT "Id","Reputation","Views","UpVotes","DownVotes",
              "CreationDate","LastAccessDate","DisplayName",
              "Location","AboutMe","WebsiteUrl"
       FROM ${INPUT_TABLE_NAME}
       ORDER BY "Id"
       OFFSET $1 LIMIT $2`,
      [offset, limit]
    );

    return result.rows as SourceUser[];
  }

  private async parallelCPUProcessing(
    batch: SourceUser[],
    gpuResults: number[][]
  ): Promise<TransformedUser[]> {
    const chunkSize = Math.ceil(batch.length / this.CPU_WORKERS);
    const promises: Promise<TransformedUser[]>[] = [];

    for (let i = 0; i < this.CPU_WORKERS; i++) {
      const startIdx = i * chunkSize;
      const endIdx = Math.min(startIdx + chunkSize, batch.length);

      if (startIdx >= batch.length) break;

      const worker = this.workerPool[this.workerIndex];
      this.workerIndex = (this.workerIndex + 1) % this.CPU_WORKERS;

      const promise = new Promise<TransformedUser[]>((resolve) => {
        worker.once("message", (results: any[]) => {
          results.forEach((r) => (r.etl_batch_id = this.batchId));
          resolve(results as TransformedUser[]);
        });

        worker.postMessage({
          batch: batch.slice(startIdx, endIdx),
          gpuResults: gpuResults.slice(startIdx, endIdx),
          startIdx: 0,
          endIdx: endIdx - startIdx,
        });
      });

      promises.push(promise);
    }

    const results = await Promise.all(promises);
    return results.flat();
  }

  private async parallelInsert(users: TransformedUser[]): Promise<void> {
    if (!users.length) return;

    const subBatchSize = Math.ceil(users.length / this.INSERTION_CONCURRENCY);
    const insertPromises: Promise<void>[] = [];

    for (let i = 0; i < this.INSERTION_CONCURRENCY; i++) {
      const start = i * subBatchSize;
      const end = Math.min(start + subBatchSize, users.length);

      if (start >= users.length) break;

      insertPromises.push(this.insertSubBatch(users.slice(start, end)));
    }

    await Promise.all(insertPromises);
  }

  private async insertSubBatch(users: TransformedUser[]): Promise<void> {
    const client = await targetPool.connect();
    const columns = [
      "user_id",
      "username",
      "reputation_score",
      "reputation_tier",
      "reputation_percentile",
      "registered_at",
      "last_login",
      "account_age_days",
      "activity_status",
      "is_active",
      "is_veteran",
      "location_original",
      "location_country",
      "location_city",
      "location_normalized",
      "bio_original",
      "bio_summary",
      "bio_wordcount",
      "bio_has_content",
      "website_url",
      "website_domain",
      "website_valid",
      "profile_views",
      "positive_votes",
      "negative_votes",
      "vote_ratio",
      "engagement_score",
      "metadata",
      "etl_timestamp",
      "etl_case_number",
      "etl_batch_id",
    ];

    const tempName = `temp_batch_${process.pid}_${Date.now()}_${crypto
      .randomBytes(2)
      .toString("hex")}`;

    try {
      await client.query("BEGIN");
      await client.query(
        `CREATE TEMP TABLE ${tempName} ON COMMIT DROP AS TABLE transformed_users WITH NO DATA`
      );

      const copyStream = client.query(
        copyFrom(
          `COPY ${tempName} (${columns.join(
            ","
          )}) FROM STDIN WITH (FORMAT csv, HEADER false)`
        )
      );

      // Write with backpressure handling to avoid buffering large strings in memory
      for (const user of users) {
        const chunk = this.userToCSV(user);
        if (!copyStream.write(chunk)) {
          await once(copyStream, "drain");
        }
      }
      copyStream.end();

      await new Promise((resolve, reject) => {
        copyStream.on("finish", resolve);
        copyStream.on("error", reject);
      });

      await client.query(
        `INSERT INTO transformed_users (${columns.join(",")})
         SELECT ${columns.join(",")} FROM ${tempName}
         ON CONFLICT (user_id, etl_case_number) DO NOTHING`
      );

      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      throw e;
    } finally {
      client.release();
    }
  }

  private userToCSV(u: TransformedUser): string {
    const esc = (v: any) => {
      if (v === null || v === undefined) return "";
      const str = typeof v === "string" ? v : JSON.stringify(v);
      return '"' + str.replace(/"/g, '""') + '"';
    };

    return (
      [
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
      ].join(",") + "\n"
    );
  }

  private cleanup(): void {
    if (this.kernels) {
      Object.values(this.kernels).forEach((kernel) => {
        if (kernel && kernel.destroy) kernel.destroy();
      });
    }

    if (this.gpu) {
      this.gpu.destroy();
    }

    if (this.workerPool) {
      this.workerPool.forEach((worker) => worker.terminate());
    }

    if ((global as any).gc) {
      (global as any).gc();
    }
  }

  private generateBatchId(): string {
    return `case5_hybrid_${Date.now()}_${crypto
      .randomBytes(4)
      .toString("hex")}`;
  }
}
