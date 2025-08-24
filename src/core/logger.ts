import * as winston from 'winston';
import * as fs from 'fs';
import * as path from 'path';
import { PerformanceMetrics } from './types';

export class ResearchLogger {
  logBatchStart(offset: number, limit: number) {
    this.logger.info(`Batch started`, {
      offset,
      limit,
      timestamp: Date.now() - this.startTime
    });
  }
  private logger: winston.Logger;
  private metricsFile: string;
  private startTime: number;
  private memoryBaseline: NodeJS.MemoryUsage;
  private cpuBaseline: NodeJS.CpuUsage;
  private peakMemory: number = 0;
  private metrics: Partial<PerformanceMetrics>;

  constructor(caseNumber: number, caseName: string) {
    // Create logs directory if not exists
    const logsDir = path.join(process.cwd(), 'research_logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/:/g, '-');
    const logFile = path.join(logsDir, `case${caseNumber}_${timestamp}.log`);
    this.metricsFile = path.join(logsDir, `metrics_case${caseNumber}_${timestamp}.json`);

    this.logger = winston.createLogger({
      level: 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
      ),
      transports: [
        new winston.transports.File({ filename: logFile }),
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
          )
        })
      ]
    });

    this.startTime = Date.now();
    this.memoryBaseline = process.memoryUsage();
    this.cpuBaseline = process.cpuUsage();
    
    this.metrics = {
      case_number: caseNumber,
      case_name: caseName,
      start_time: new Date(),
      memory_start_mb: this.memoryBaseline.heapUsed / 1024 / 1024,
      detailed_timings: []
    };

    // Monitor memory periodically
    this.startMemoryMonitoring();
  }

  private startMemoryMonitoring(): void {
    const interval = setInterval(() => {
      const currentMemory = process.memoryUsage().heapUsed / 1024 / 1024;
      if (currentMemory > this.peakMemory) {
        this.peakMemory = currentMemory;
      }
    }, 100);

    process.on('beforeExit', () => clearInterval(interval));
  }

  public logPhaseStart(phase: string): void {
    this.logger.info(`Phase started: ${phase}`, {
      phase,
      timestamp: Date.now() - this.startTime,
      memory_mb: process.memoryUsage().heapUsed / 1024 / 1024
    });
  }

  public logPhaseEnd(phase: string, recordCount?: number): void {
    const duration = Date.now() - this.startTime;
    this.logger.info(`Phase completed: ${phase}`, {
      phase,
      duration_ms: duration,
      records: recordCount,
      memory_mb: process.memoryUsage().heapUsed / 1024 / 1024
    });

    this.metrics.detailed_timings?.push({
      phase,
      duration_ms: duration,
      records: recordCount,
      timestamp: new Date()
    });
  }

  public logProgress(current: number, total: number, phase: string): void {
    const percentage = ((current / total) * 100).toFixed(2);
    const elapsed = Date.now() - this.startTime;
    const rate = current / (elapsed / 1000);
    const eta = (total - current) / rate;

    this.logger.info(`Progress: ${phase}`, {
      current,
      total,
      percentage: `${percentage}%`,
      rate_per_second: rate.toFixed(2),
      eta_seconds: eta.toFixed(0),
      memory_mb: process.memoryUsage().heapUsed / 1024 / 1024
    });
  }

  public logError(error: Error, context?: any): void {
    this.logger.error('Error occurred', {
      message: error.message,
      stack: error.stack,
      context,
      timestamp: Date.now() - this.startTime
    });
  }

  public async finalizeMetrics(totalRecords: number, errorCount: number = 0): Promise<PerformanceMetrics> {
    const endTime = new Date();
    const totalDuration = Date.now() - this.startTime;
    const cpuUsage = process.cpuUsage(this.cpuBaseline);
    const memoryEnd = process.memoryUsage().heapUsed / 1024 / 1024;

    const finalMetrics: PerformanceMetrics = {
      ...this.metrics as PerformanceMetrics,
      end_time: endTime,
      total_duration_ms: totalDuration,
      total_records: totalRecords,
      records_per_second: totalRecords / (totalDuration / 1000),
      memory_peak_mb: this.peakMemory,
      memory_end_mb: memoryEnd,
      cpu_user_ms: cpuUsage.user / 1000,
      cpu_system_ms: cpuUsage.system / 1000,
      error_count: errorCount,
      extract_duration_ms: 0,
      transform_duration_ms: 0,
      load_duration_ms: 0
    };

    // Calculate phase durations from detailed timings
    const timings = this.metrics.detailed_timings || [];
    for (const timing of timings) {
      if (timing.phase.includes('extract')) {
        finalMetrics.extract_duration_ms += timing.duration_ms;
      } else if (timing.phase.includes('transform')) {
        finalMetrics.transform_duration_ms += timing.duration_ms;
      } else if (timing.phase.includes('load')) {
        finalMetrics.load_duration_ms += timing.duration_ms;
      }
    }

    // Save metrics to file
    fs.writeFileSync(this.metricsFile, JSON.stringify(finalMetrics, null, 2));

    // Log summary
    this.logger.info('ETL Process Completed', {
      total_duration_seconds: (totalDuration / 1000).toFixed(2),
      total_records: totalRecords,
      records_per_second: finalMetrics.records_per_second.toFixed(2),
      memory_peak_mb: this.peakMemory.toFixed(2),
      cpu_seconds: ((cpuUsage.user + cpuUsage.system) / 1000000).toFixed(2),
      error_count: errorCount
    });

    return finalMetrics;
  }
}