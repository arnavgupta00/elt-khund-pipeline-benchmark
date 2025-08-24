# ETL Research Project

## Setup Instructions

### 1. Install Dependencies
```bash
npm install
```

### 2. Setup PostgreSQL Databases

Create two PostgreSQL instances (or databases on different ports):
- Source database (port 5432): Contains Stack Overflow users data
- Target database (port 5433): Will contain transformed data

### 3. Load Stack Overflow Data

```bash
# Import your users CSV into PostgreSQL
psql -h localhost -p 5432 -U postgres -d stackoverflow -c "
CREATE TABLE users (
    Id INTEGER PRIMARY KEY,
    Reputation INTEGER,
    CreationDate TIMESTAMP,
    DisplayName VARCHAR(255),
    LastAccessDate TIMESTAMP,
    WebsiteUrl TEXT,
    Location VARCHAR(500),
    AboutMe TEXT,
    Views INTEGER,
    UpVotes INTEGER,
    DownVotes INTEGER,
    ProfileImageUrl TEXT,
    EmailHash VARCHAR(255),
    AccountId INTEGER
);"

# Copy CSV data
psql -h localhost -p 5432 -U postgres -d stackoverflow -c "\COPY users FROM 'path/to/users.csv' CSV HEADER"
```

### 4. Configure Environment

Copy `.env.template` to `.env` and update with your database credentials.

### 5. Deploy Cloudflare Worker (for Case 4a)

```bash
cd cloudflare-worker
npm install -g wrangler
wrangler login
wrangler deploy
```

Update the CLOUDFLARE_WORKER_URL in your .env file with the deployed URL.

## Running the Cases

### Run Individual Cases
```bash
# Case 1: Sequential
npm run case1

# Case 2: Bulk
npm run case2

# Case 3: Multi-threaded Pipeline
npm run case3

# Case 4a: Edge Computing
npm run case4

# Run with limit (for testing)
npx tsx src/index.ts --case=1 --limit=10000
```

### Run All Cases
```bash
npm start
# or
npx tsx src/index.ts --case=all --limit=100000
```

## Research Data Collection

All metrics are automatically saved to `research_logs/` directory:
- Individual case logs: `case{N}_{timestamp}.log`
- Metrics JSON: `metrics_case{N}_{timestamp}.json`
- Comparison report: `comparison_{timestamp}.json`

## Performance Metrics Collected

- Total execution time
- Records per second
- Memory usage (start, peak, end)
- CPU utilization
- Phase-wise timing (Extract, Transform, Load)
- Error counts
- Thread/Worker counts (Cases 3 & 4a)

## Expected Results

With ~3 million users (5.6GB):
- Case 1: ~45-60 minutes
- Case 2: ~8-12 minutes
- Case 3: ~5-8 minutes
- Case 4a: ~3-6 minutes

## Customizing Transformations

Edit `src/core/transformer.ts` to add more complex transformations if needed to better demonstrate performance differences.