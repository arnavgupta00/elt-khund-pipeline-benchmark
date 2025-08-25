# ETL Performance Benchmark

A comprehensive research project comparing different ETL (Extract, Transform, Load) architectural patterns for large-scale data processing. This study evaluates sequential, bulk, multi-threaded, and edge computing approaches using real-world Stack Overflow data.

## 📊 About

This project benchmarks four distinct ETL implementation strategies to analyze their performance characteristics, resource utilization, and scalability patterns when processing millions of records. The research provides empirical evidence for choosing optimal ETL architectures based on data volume, infrastructure constraints, and performance requirements.

### Research Objectives

* Compare execution time and throughput across different ETL patterns
* Analyze memory consumption and CPU utilization for each approach
* Evaluate the benefits of edge computing in distributed data processing
* Provide actionable insights for ETL pipeline optimization

### Key Features

* **Real-world Dataset**: Uses Stack Overflow's user dataset (~650K records for testing)
* **Multiple Architectures**: Sequential, Bulk File, Multi-threaded Pipeline, and Edge Computing
* **Comprehensive Metrics**: Detailed performance logging and analysis
* **Production-Ready**: TypeScript implementation with proper error handling and logging
* **Cloud-Native**: Includes Cloudflare Workers for edge computing demonstration

## 🏗️ Architecture Patterns

| Case   | Pattern                 | Description                                   | Use Case                        |
| ------ | ----------------------- | --------------------------------------------- | ------------------------------- |
| **1**  | Sequential              | Row-by-row processing                         | Simple, small datasets          |
| **2**  | Bulk Export/Import      | File-based batch processing                   | Large batch operations          |
| **3**  | Multi-threaded Pipeline | Concurrent processing with worker threads     | CPU-intensive transformations   |
| **4a** | Edge Computing          | Distributed processing via Cloudflare Workers | Geographically distributed data |

## 📈 Performance Results

With **test dataset of ~650,000 records**:

* **Sequential**: ~230s (≈2,822 recs/sec)
* **Bulk File Import**: ~27s (≈23,945 recs/sec)
* **Multi-threaded Pipeline**: ~134s (≈4,861 recs/sec)
* **Edge Computing (Cloudflare Workers)**: ~30s (≈21,895 recs/sec)

**Key Insights**: Bulk file processing offers the best throughput for large datasets, while edge computing provides excellent performance with geographical distribution benefits.

## 🛠️ Technologies Used

* **Language**: TypeScript/Node.js
* **Database**: PostgreSQL
* **Edge Computing**: Cloudflare Workers
* **Threading**: Node.js Worker Threads
* **Package Manager**: pnpm
* **Build Tool**: tsx
* **Monitoring**: Custom performance metrics logger

## 🎯 Research Applications

This benchmark is valuable for:

* Data engineers designing ETL pipelines
* Researchers studying distributed systems
* Organizations optimizing data processing workflows
* Academic courses on database systems and data engineering

## 📚 Citation

If you use this research in your work, please cite:

```
@software{etl_performance_benchmark_2025,
  author = {Arnav},
  title = {ETL Performance Benchmark: A Comparative Study of Data Processing Architectures},
  year = {2025},
  url = {https://github.com/arnavgupta00/elt-khund-pipeline-benchmark}
}
```

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/arnavgupta00/elt-khund-pipeline-benchmark.git
cd elt-khund-pipeline-benchmark

# Install dependencies
pnpm install

# Set up environment variables
cp .env.template .env
# Edit .env with your database credentials

# Run specific test cases
pnpm case1    # Sequential processing
pnpm case2    # Bulk file processing
pnpm case3    # Multi-threaded pipeline
pnpm case4    # Edge computing

# Run with custom limit
pnpm start -- --case=1 --limit=10000

# Deploy edge transformer (requires Cloudflare account)
cd edge-transformer
pnpm install
pnpm deploy
```

## 📋 Prerequisites

* Node.js (v18+)
* PostgreSQL database
* pnpm package manager
* Cloudflare account (for edge computing case)

## 📊 Sample Results

Performance comparison results are automatically saved as JSON files with detailed metrics including execution time, memory usage, and throughput statistics.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is open source. Feel free to use and modify as needed.

## 🔬 Research Details

The benchmark tests were conducted using a subset of Stack Overflow's user dataset, focusing on realistic data transformation scenarios including data validation, type conversion, and aggregation operations.

---

**Keywords**: ETL, Data Pipeline, Performance Benchmark, Edge Computing, Distributed Systems, Big Data, PostgreSQL, Cloudflare Workers, TypeScript

---

## 🏷️ **GitHub Topics to Add:**

```
etl
data-engineering
performance-testing
benchmark
postgresql
cloudflare-workers
typescript
distributed-systems
big-data
research
edge-computing
data-pipeline
multithreading
database
data-processing
```

## 📁 **Repository Structure:**

```
elt-khund-pipeline-benchmark/
├── README.md
├── .env.template
├── .gitignore
├── package.json
├── pnpm-lock.yaml
├── tsconfig.json
├── src/
│   ├── index.ts
│   ├── cases/
│   │   ├── case1-sequential.ts
│   │   ├── case2-bulk.ts
│   │   ├── case3-pipeline.ts
│   │   └── case4a-edge.ts
│   ├── config/
│   └── core/
├── edge-transformer/        # Cloudflare Workers implementation
│   ├── src/
│   ├── wrangler.jsonc
│   └── package.json
├── research_logs/          # Performance test results (gitignored)
└── temp/                   # Temporary processing files
```
