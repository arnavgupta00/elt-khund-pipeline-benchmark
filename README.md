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

* **Real-world Dataset**: Uses Stack Overflow's 5.6GB user dataset (\~3M records)
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

With \~3 million user records:

* **Sequential**: \~45-60 minutes (baseline)
* **Bulk**: \~8-12 minutes (5x faster)
* **Multi-threaded**: \~5-8 minutes (8x faster)
* **Edge Computing**: \~3-6 minutes (12x faster)

With **test dataset of \~650,000 records**:

* **Sequential**: \~230s (≈2822 recs/sec)
* **Bulk File Import**: \~27s (≈23944 recs/sec)
* **Multi-threaded Pipeline**: \~134s (≈4861 recs/sec)
* **Edge Computing (Cloudflare Workers)**: \~30s (≈21895 recs/sec)

## 🛠️ Technologies Used

* **Language**: TypeScript/Node.js
* **Database**: PostgreSQL
* **Edge Computing**: Cloudflare Workers
* **Threading**: Node.js Worker Threads
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
  author = Arnav,
  title = {ETL Performance Benchmark: A Comparative Study of Data Processing Architectures},
  year = 2025,
  url = https://github.com/arnavgupta00/elt-khund-pipeline-benchmark/tree/main
}
```

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/etl-performance-benchmark.git

# Install dependencies
npm install

# Run benchmarks
npm run benchmark -- --limit=10000
```

See [Installation Guide](./docs/INSTALLATION.md) for detailed setup instructions.

## 📊 Sample Results

![Performance Comparison](./docs/images/performance-chart.png)

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## 📄 License

MIT License - see [LICENSE](./LICENSE) for details.

## 🔬 Research Paper

The full research paper is available at: \[Link to Paper]

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
etl-performance-benchmark/
├── README.md
├── LICENSE (MIT)
├── .gitignore
├── package.json
├── tsconfig.json
├── src/
│   ├── cases/
│   ├── core/
│   └── config/
├── cloudflare-worker/
├── docs/
│   ├── INSTALLATION.md
│   ├── ARCHITECTURE.md
│   ├── RESULTS.md
│   └── images/
├── research_logs/  (gitignored)
├── scripts/
│   ├── setup-db.sh
│   └── load-data.sh
└── tests/
```
