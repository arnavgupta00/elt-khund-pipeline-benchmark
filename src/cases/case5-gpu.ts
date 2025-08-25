import { GPU } from "gpu.js";
import { from as copyFrom } from "pg-copy-streams";
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
 * Case 5: Ultra-optimized GPU accelerated ETL with maximum parallelism
 * - 2D GPU kernels for massive parallel processing
 * - Multi-stage pipeline with double buffering
 * - Worker threads for CPU-bound string operations
 * - Streaming data transfer with shared buffers
 * - Multiple specialized GPU kernels
 */
export class Case5GPUUltraFast {
  private gpu!: GPU;
  private kernels!: {
    tierCalculation: any;
    engagementMetrics: any;
    dateProcessing: any;
    percentileEstimation: any;
    combinedTransform: any;
  };
  private logger: ResearchLogger;
  private transformer: UserTransformer;
  private batchId: string;

  // Optimized parameters for RTX 4060 laptop (6GB VRAM)
  private readonly BATCH_SIZE = 32768; // Power of 2 for optimal GPU memory alignment
  private readonly GPU_BLOCK_SIZE = 256; // Threads per block (x dimension)
  private readonly GPU_GRID_SIZE = 128; // Number of blocks (y dimension)
  private readonly EXTRACTION_CONCURRENCY = 8; // Parallel extraction
  private readonly INSERTION_CONCURRENCY = 4; // Parallel insertion
  private readonly CPU_WORKERS = Math.max(3, Math.min(os.cpus().length - 2, 8)); // Leave cores for main thread
  private readonly PIPELINE_DEPTH = 3; // Pipeline stages in flight

  // Double buffering for zero-copy transfers
  private buffers!: {
    current: number;
    gpu: Array<{
      ids: Float32Array;
      reputations: Float32Array;
      views: Float32Array;
      upvotes: Float32Array;
      downvotes: Float32Array;
      creationDates: Float32Array;
      lastAccessDates: Float32Array;
    }>;
  };

  // Pipeline stages
  private pipeline!: {
    extraction: Promise<SourceUser[]>[];
    gpuProcessing: Promise<number[][]>[];
    cpuProcessing: Promise<TransformedUser[]>[];
    insertion: Promise<void>[];
  };

  // Worker pool for string processing
  private workerPool!: Worker[];
  private workerIndex: number = 0;

  constructor() {
    this.logger = new ResearchLogger(5, "GPU Ultra-Fast Processing");
    this.transformer = new UserTransformer(5);
    this.batchId = this.generateBatchId();

    // Initialize GPU with optimal settings
    this.initializeGPU();
    // Prepare kernels container before kernel creation
    this.kernels = {
      tierCalculation: undefined,
      engagementMetrics: undefined,
      dateProcessing: undefined,
      percentileEstimation: undefined,
      combinedTransform: undefined,
    } as any;
    this.initializeKernels();
    this.initializeBuffers();
    this.initializeWorkerPool();
    this.initializePipeline();
  }

  private initializeGPU(): void {
    console.log("Initializing GPU with optimized settings...");

    const gpuOptions = {
      mode: "gpu" as const,
      canvas: undefined, // Headless mode
      context: undefined,
      // Optimize for compute, not graphics
      constants: {
        BATCH_SIZE: this.BATCH_SIZE,
        BLOCK_SIZE: this.GPU_BLOCK_SIZE,
      },
      tactic: "speed" as const, // Prioritize speed over precision
      immutable: false, // Allow kernel recreation
      pipeline: true, // Enable pipelining
      precision: "single" as const, // Use 32-bit floats
      fixIntegerDivisionAccuracy: false, // Speed over accuracy for ints
    };

    try {
      this.gpu = new GPU(gpuOptions);
      if (this.gpu.mode !== "gpu") {
        throw new Error("GPU not available");
      }
      console.log(`GPU initialized: ${this.gpu.mode} mode`);
    } catch (error) {
      console.error("GPU initialization failed:", error);
      throw new Error("RTX 4060 GPU required for this optimization");
    }
  }

  private initializeKernels(): void {
    // Ensure kernels container exists
    if (!this.kernels) {
      this.kernels = {
        tierCalculation: undefined,
        engagementMetrics: undefined,
        dateProcessing: undefined,
        percentileEstimation: undefined,
        combinedTransform: undefined,
      } as any;
    }

    // Combined transformation kernel - does all numeric transforms in one pass
    this.kernels.combinedTransform = this.gpu
      .createKernel(function (
        ids: Float32Array[],
        reputations: Float32Array[],
        views: Float32Array[],
        upvotes: Float32Array[],
        downvotes: Float32Array[],
        creationDates: Float32Array[],
        lastAccessDates: Float32Array[],
        currentTime: number
      ): number {
        const row = this.thread.y; // 0..GRID_SIZE-1
        const col = this.thread.x; // 0..BLOCK_SIZE-1
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

        // Tier calculation
        let tier = 1;
        if (rep > 100000) tier = 5;
        else if (rep > 10000) tier = 4;
        else if (rep > 1000) tier = 3;
        else if (rep > 100) tier = 2;

        // Engagement
        const totalVotes = up + down;
        const engagement = totalVotes / Math.max(v, 1);

        // Days calculations
        const daysSinceAccess = (currentTime - accessed) / 86400;
        const accountAgeDays = (currentTime - created) / 86400;

        // Vote ratio
        const voteRatio = down === 0 ? up : up / down;

        // Percentile estimation
        let percentile = 0;
        if (rep > 100000) percentile = 99.9;
        else if (rep > 10000) percentile = 99;
        else if (rep > 1000) percentile = 90;
        else if (rep > 100) percentile = 75;
        else if (rep > 10) percentile = 50;
        else if (rep > 1) percentile = 20;

        // Activity status (encoded as number)
        let activityStatus = 0; // inactive
        if (daysSinceAccess < 30) activityStatus = 3; // active
        else if (daysSinceAccess < 90) activityStatus = 2; // regular
        else if (daysSinceAccess < 365) activityStatus = 1; // occasional

        // Map z-dimension to metric
        const z = this.thread.z;
        if (z === 0) return tier;
        if (z === 1) return engagement;
        if (z === 2) return daysSinceAccess;
        if (z === 3) return accountAgeDays;
        if (z === 4) return voteRatio;
        if (z === 5) return percentile;
        if (z === 6) return activityStatus;
        return ids[0][idx];
      })
      .setOutput([this.GPU_BLOCK_SIZE, this.GPU_GRID_SIZE, 8]) // [x, y, z]
      .setConstants({
        BATCH_SIZE: this.BATCH_SIZE,
        BLOCK_SIZE: this.GPU_BLOCK_SIZE,
      })
      .setPipeline(false) // Get results immediately
      .setImmutable(false)
      .setTactic("speed");

    // Pre-warm kernels
    this.warmupKernels();
  }

  private warmupKernels(): void {
    const dummyData = new Float32Array(this.BATCH_SIZE);
    const wrapped = [dummyData];

    // Run each kernel once to compile shaders
    this.kernels.combinedTransform(
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
    // Create double buffers for zero-copy transfers
    this.buffers = {
      current: 0,
      gpu: [
        {
          ids: new Float32Array(this.BATCH_SIZE),
          reputations: new Float32Array(this.BATCH_SIZE),
          views: new Float32Array(this.BATCH_SIZE),
          upvotes: new Float32Array(this.BATCH_SIZE),
          downvotes: new Float32Array(this.BATCH_SIZE),
          creationDates: new Float32Array(this.BATCH_SIZE),
          lastAccessDates: new Float32Array(this.BATCH_SIZE),
        },
        {
          ids: new Float32Array(this.BATCH_SIZE),
          reputations: new Float32Array(this.BATCH_SIZE),
          views: new Float32Array(this.BATCH_SIZE),
          upvotes: new Float32Array(this.BATCH_SIZE),
          downvotes: new Float32Array(this.BATCH_SIZE),
          creationDates: new Float32Array(this.BATCH_SIZE),
          lastAccessDates: new Float32Array(this.BATCH_SIZE),
        },
      ],
    };
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

          if (!gpu || gpu.length < 8) continue;

          const tierNum = gpu[0];
          const engagement = gpu[1];
          const daysSinceAccess = gpu[2];
          const accountAgeDays = Math.floor(gpu[3]);
          const voteRatio = gpu[4];
          const percentile = gpu[5];
          const activityStatusNum = gpu[6];

          // String processing
          const username = user.DisplayName ?
            user.DisplayName.substring(0, 255).replace(/[^a-zA-Z0-9_.-]/g, '_') :
            'anonymous';

          const location = user.Location;
          let country = null, city = null, normalizedLocation = null;

          if (location) {
            const countries = ['USA', 'United States', 'UK', 'United Kingdom',
                             'Canada', 'Germany', 'France', 'India', 'China',
                             'Japan', 'Brazil', 'Australia'];
            for (const c of countries) {
              if (location.toLowerCase().includes(c.toLowerCase())) {
                country = c;
                break;
              }
            }
            const parts = location.split(',');
            city = parts[0]?.trim() || null;
            if (!country && parts.length > 1) {
              country = parts[parts.length - 1].trim();
            }
            normalizedLocation = location.replace(/[^a-zA-Z0-9, ]/g, '').substring(0, 500);
          }

          const bio = user.AboutMe;
          let bioSummary = null, bioWordcount = 0;
          if (bio) {
            const cleaned = bio.replace(/<[^>]*>/g, '');
            bioSummary = cleaned.substring(0, 200) + (cleaned.length > 200 ? '...' : '');
            bioWordcount = cleaned.replace(/\\s+/g, ' ').split(' ').filter(w => w.length > 0).length;
          }

          const website = user.WebsiteUrl;
          let domain = null, validUrl = false;
          if (website) {
            try {
              const url = new URL(website.startsWith('http') ? website : 'http://' + website);
              domain = url.hostname;
              validUrl = true;
            } catch {}
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
            location_original: location,
            location_country: country,
            location_city: city,
            location_normalized: normalizedLocation,
            bio_original: bio,
            bio_summary: bioSummary,
            bio_wordcount: bioWordcount,
            bio_has_content: !!bio && bio.length > 10,
            website_url: website,
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
              etl_version: '1.0.0',
              processing_case: 5,
              has_email: false,
              has_avatar: false,
            },
            etl_timestamp: now,
            etl_case_number: 5,
            etl_batch_id: 'provided_later'
          });
        }

        parentPort.postMessage(results);
      });
    `;

    // Create worker threads
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

  public async execute(limit?: number): Promise<PerformanceMetrics> {
    let processedCount = 0;
    let errorCount = 0;

    try {
      await initializeTargetSchema();

      // Clear existing data for case 5
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
        `Processing ${totalCount} records with ultra-fast GPU pipeline...`
      );

      const startTime = Date.now();
      let kernelTime = 0;
      let transferTime = 0;

      // Setup parallel extraction connections
      const extractionClients = [];
      for (let i = 0; i < this.EXTRACTION_CONCURRENCY; i++) {
        const client = await sourcePool.connect();
        await client.query(
          "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
        );
        extractionClients.push(client);
      }

      // Process in pipeline stages
      let offset = 0;
      const inFlight: Promise<void>[] = [];
      const waitOne = async () => {
        const idx = await Promise.race(
          inFlight.map((p, i) =>
            p.then(() => i).catch((err) => {
              errorCount++;
              this.logger.logError(err);
              return i;
            })
          )
        );
        inFlight.splice(idx, 1);
      };
      while (offset < totalCount) {
        while (offset < totalCount && inFlight.length < this.PIPELINE_DEPTH) {
          const batchOffset = offset;
          const batchSize = Math.min(this.BATCH_SIZE, totalCount - batchOffset);
          const p = this.processPipelineBatch(
            extractionClients,
            batchOffset,
            batchSize,
            { kernelTime, transferTime }
          ).then(({ processed, kernel, transfer }) => {
            processedCount += processed;
            kernelTime += kernel;
            transferTime += transfer;
            if (processedCount % 50000 === 0) {
              this.logger.logProgress(processedCount, totalCount, "GPU Pipeline");
            }
          });
          inFlight.push(p);
          offset += batchSize;
        }
        if (inFlight.length > 0) await waitOne();
      }
      while (inFlight.length > 0) await waitOne();

      // Cleanup extraction clients
      for (const client of extractionClients) {
        client.release();
      }

      const totalTime = Date.now() - startTime;

      // Calculate metrics
      const metrics = await this.logger.finalizeMetrics(
        processedCount,
        errorCount
      );

      // Add GPU-specific metrics
      (metrics as any).gpu_device = "RTX 4060";
      (metrics as any).gpu_threads =
        this.GPU_BLOCK_SIZE * this.GPU_GRID_SIZE;
      (metrics as any).kernel_execution_time_ms = kernelTime;
      (metrics as any).memory_transfer_time_ms = transferTime;
      (metrics as any).gpu_utilization_percent = (
        (kernelTime / totalTime) *
        100
      ).toFixed(2);
      (metrics as any).pipeline_depth = this.PIPELINE_DEPTH;
      (metrics as any).cpu_workers = this.CPU_WORKERS;
      (metrics as any).throughput_records_per_sec = Math.round(
        processedCount / (totalTime / 1000)
      );

      metrics.batch_count = Math.ceil(processedCount / this.BATCH_SIZE);

      console.log(`Completed: ${processedCount} records in ${totalTime}ms`);
      console.log(
        `Throughput: ${(metrics as any).throughput_records_per_sec} records/sec`
      );
      console.log(
        `GPU Utilization: ${(metrics as any).gpu_utilization_percent}%`
      );

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
    _timers: { kernelTime: number; transferTime: number }
  ): Promise<{ processed: number; kernel: number; transfer: number }> {
    // Stage 1: Parallel extraction
    const clientIdx = (offset / this.BATCH_SIZE) % extractionClients.length;
    const batch = await this.parallelExtract(
      extractionClients[clientIdx],
      offset,
      batchSize
    );

    // Stage 2: Prepare GPU data (use double buffering)
    const transferStart = Date.now();
    const bufferIdx = this.buffers.current;
    this.buffers.current = (this.buffers.current + 1) % 2;

    const buffer = this.buffers.gpu[bufferIdx];
    this.prepareGPUBuffers(batch, buffer);
    const transferTime = Date.now() - transferStart;

    // Stage 3: GPU Processing
    const kernelStart = Date.now();
    const gpuResults = await this.executeGPUKernels(buffer, batch.length);
    const kernelTime = Date.now() - kernelStart;

    // Stage 4: Parallel CPU string processing
    const transformedUsers = await this.parallelCPUProcessing(
      batch,
      gpuResults,
      null
    );

    // Stage 5: Parallel insertion
    await this.parallelInsert(transformedUsers);

    return {
      processed: transformedUsers.length,
      kernel: kernelTime,
      transfer: transferTime,
    };
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

  private prepareGPUBuffers(
    batch: SourceUser[],
    buffer: any
  ): void {
    // Vectorized copy to GPU buffers
    for (let i = 0; i < batch.length; i++) {
      const u = batch[i];
      buffer.ids[i] = u.Id;
      buffer.reputations[i] = u.Reputation;
      buffer.views[i] = u.Views;
      buffer.upvotes[i] = u.UpVotes;
      buffer.downvotes[i] = u.DownVotes;
      buffer.creationDates[i] = Date.parse(u.CreationDate) / 1000;
      buffer.lastAccessDates[i] = Date.parse(u.LastAccessDate) / 1000;
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
    }
  }

  private async executeGPUKernels(
    buffer: any,
    actualSize: number
  ): Promise<number[][]> {
    const currentTime = Date.now() / 1000;

    // Execute combined kernel for all transformations
    const results = this.kernels.combinedTransform(
      [buffer.ids],
      [buffer.reputations],
      [buffer.views],
      [buffer.upvotes],
      [buffer.downvotes],
      [buffer.creationDates],
      [buffer.lastAccessDates],
      currentTime
    ) as number[][][]; // GPU.js returns [z][y][x]

    // Reconstruct per-record arrays from 3D output [x, y, z] => nested [z][y][x]
    const flattened: number[][] = new Array(
      Math.min(actualSize, this.BATCH_SIZE)
    );
    let writeIdx = 0;
    for (let y = 0; y < this.GPU_GRID_SIZE; y++) {
      for (let x = 0; x < this.GPU_BLOCK_SIZE; x++) {
        if (writeIdx >= actualSize) break;
        const metrics: number[] = new Array(8);
        for (let z = 0; z < 8; z++) {
          metrics[z] = results[z][y][x]; // <-- correct indexing
        }
        flattened[writeIdx++] = metrics;
      }
    }

    return flattened;
  }

  private async parallelCPUProcessing(
    batch: SourceUser[],
    gpuResults: number[][],
    _stringData: any
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
          // Add batch ID to results
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

    // Split into sub-batches for parallel insertion
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

    // Generate temp table name ONCE and reuse
    const tempName = `temp_batch_${process.pid}_${Date.now()}_${crypto.randomBytes(2).toString("hex")}`;

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

      for (const user of users) {
        copyStream.write(this.userToCSV(user));
      }
      copyStream.end();

      await new Promise((resolve, reject) => {
        copyStream.on("finish", resolve);
        copyStream.on("error", reject);
      });

      // Use ON CONFLICT for upsert
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
    ].join(",") + "\n";
  }

  private cleanup(): void {
    // Destroy GPU kernels
    if (this.kernels) {
      Object.values(this.kernels).forEach((kernel) => {
        if (kernel && kernel.destroy) {
          kernel.destroy();
        }
      });
    }

    // Destroy GPU context
    if (this.gpu) {
      this.gpu.destroy();
    }

    // Terminate worker threads
    if (this.workerPool) {
      this.workerPool.forEach((worker) => worker.terminate());
    }

    // Force garbage collection
    if ((global as any).gc) {
      (global as any).gc();
    }
  }

  private generateBatchId(): string {
    return `case5_ultra_${Date.now()}_${crypto
      .randomBytes(4)
      .toString("hex")}`;
  }
}
