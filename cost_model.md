# Cost Model — Source-Checked

Claude's baseline answer asserts a $35,000/mo AWS infrastructure budget for this workload. This file source-checks every line item against published pricing and re-prices the proposed counter-stack the same way.

**Workload [Estimated]:** 50M events/day × ~1KB = 50 GB/day raw ingest. 50M ÷ 86,400 = **580 events/sec sustained**, **~5,800 events/sec at 10x peak**. Monthly volume ~1.5 TB ingested. Compressed in ClickHouse at typical 3-5x ratio → ~300-500 GB/mo retained warm.

## Table 1 — Claude's Stack, Re-Priced

us-east-1, 730 hr/mo, on-demand unless noted.

| Line | Claude's number | Honest re-calc | Source | Verdict |
|---|---|---|---|---|
| **MSK 3× kafka.m5.large + 1TB** | $3,000 | 3 × $0.21/hr × 730 = **$460** brokers + 1024 GB × $0.10 = **$102** storage + cross-AZ ≈ **$30**. **~$590/mo** | [MSK pricing](https://aws.amazon.com/msk/pricing/) | **Over by ~5x** |
| **Kinesis Data Analytics (Flink) — 4 KPU** | $5,000 | 4 KPU × $0.11/hr × 730 = **$321** + 1 orchestration KPU ($80) + ~50 GB state × $0.10 = $5. **~$406/mo**. Note: AWS **renamed this product** to Amazon Managed Service for Apache Flink in 2023; Claude's name is stale. | [MSAF pricing](https://aws.amazon.com/managed-service-apache-flink/pricing/) | **Over by ~10x** |
| **Timestream (24h hot)** | $4,000 | Writes 50M/day × 30 × $0.50/M = **$750**. Memory store ~50 GB × 24h × 30 × $0.036 = **$1,296**. Magnetic ~$3. Queries 4 TCU × $0.518/hr × 730 = **$1,512**. **~$3,500-4,500/mo** | [Timestream pricing](https://aws.amazon.com/timestream/pricing/) | **Wrong product entirely.** Timestream for LiveAnalytics [closed to new customers 2025-06-20](https://docs.aws.amazon.com/timestream/latest/developerguide/AmazonTimestreamForLiveAnalytics-availability-change.html). Number lands by accident. |
| **ClickHouse 3× r5.xlarge (self-managed EC2)** | $6,000 | 3 × $0.252/hr × 730 = **$552** compute + 450 GB gp3 × $0.08 = $36 = **~$590/mo on-demand**, **~$390/mo on 1-yr Savings Plan** | [r5.xlarge pricing](https://aws.amazon.com/ec2/pricing/on-demand/) | **Over by ~10x.** Claude appears to confuse self-managed EC2 with ClickHouse Cloud pricing. |
| **Redis 3× r5.large (ElastiCache)** | $2,000 | 3 × $0.216/hr × 730 = **$473/mo**. Reserved: ~$307. | [ElastiCache pricing](https://aws.amazon.com/elasticache/pricing/) | **Over by ~4x** |
| **EC2 c5.xlarge ingestion (auto-scale 4-10)** | $3,000 | Avg 4 instances: 4 × $0.17/hr × 730 = **$496/mo**. Peak avg 8: **$993/mo**. **~$500-1,000/mo** | [c5.xlarge pricing](https://aws.amazon.com/ec2/pricing/on-demand/) | **Over by 3-6x** |
| **S3 + data transfer** | $5,000 | Storage 18 TB cumulative × $0.023 = **$414**. Egress 50 GB/day × 30 × $0.09 = **$135**. Batched PUTs (1/min) ≈ **$0.22**. **~$200-1,500/mo honest range** — or **$7,500/mo** if naïvely writing one S3 object per event. | [S3 pricing](https://aws.amazon.com/s3/pricing/) | **Over unless naïve PUT pattern**, in which case catastrophically under |
| **ALB + CloudWatch monitoring** | $2,000 | ALB ~$25 + LCU at this RPS ~$50. CloudWatch logs+metrics ~$300-800. **~$400-900/mo** | [ALB pricing](https://aws.amazon.com/elasticloadbalancing/pricing/), [CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/) | **Over by ~3x** |
| **Buffer** | $5,000 | Padding | — | 60%+ of true-cost padding |
| **Honest total** | **$35,000** | **~$6,500-9,500/mo on-demand, ~$5,500 with Savings Plans** | | **Claude is over by 3.5-5x** |

## Table 2 — Counter-Stack, Source-Linked

| Line | Cost | Math | Source |
|---|---|---|---|
| **Kinesis Data Streams (on-demand)** | **~$170/mo** | Ingest 1.5 TB × $0.08 = $120 + retrieval 1.5 TB × $0.04 = $60 + per-stream $0.04 × 730 = $29 | [KDS pricing](https://aws.amazon.com/kinesis/data-streams/pricing/) |
| **Fargate ingester (4 vCPU / 8 GB avg, 16/32 peak)** | **~$200/mo** | 4 vCPU × $0.04048/hr + 8 GB × $0.004445/hr = $0.197/hr × 730 = **$144** + headroom. ARM saves ~20%. | [Fargate pricing](https://aws.amazon.com/fargate/pricing/) |
| **ClickHouse Cloud (Scale tier, 2 replicas, 32 GiB total)** | **~$1,200-1,800/mo** | Scale tier minimum $499 (2×8 GiB). Realistic with MVs handling aggregations: ~16 GiB sustained × 2 replicas ≈ 4× minimum. Compressed 30-day storage ~50-150 GB ≈ $5/mo. | [ClickHouse Cloud pricing](https://clickhouse.com/pricing) |
| **ElastiCache Redis 3× r5.large** | **~$475/mo** | (kept from Claude's stack, fixed price) | [ElastiCache pricing](https://aws.amazon.com/elasticache/pricing/) |
| **S3 cold storage + Iceberg** | **~$50-200/mo** | 6 TB avg × $0.023 = $138 + batched PUTs ~$5. Glacier transition optional. | [S3 pricing](https://aws.amazon.com/s3/pricing/) |
| **Inter-AZ + dashboard egress** | **~$300-600/mo** | KDS cross-AZ at $0.01/GB × 1.5 TB × 2 ≈ $30. Dashboard egress (100 GB/day API) = 3 TB × $0.09 = **$270**. Long-pole. | [AWS data transfer](https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer) |
| **DynamoDB (DEK store) + KMS** | **~$50/mo** | DynamoDB on-demand at 100K user keys × low RPS = $5-20. KMS CMK $1/mo per key × 500 tenants = $500 ceiling, but pooled = $50. | [DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/), [KMS pricing](https://aws.amazon.com/kms/pricing/) |
| **Aurora Postgres (PII store)** | **~$200-400/mo** | db.r5.large × 730 + 50 GB storage. Burstable t-class is cheaper. | [Aurora pricing](https://aws.amazon.com/rds/aurora/pricing/) |
| **ALB + CloudWatch + buffer** | **~$500/mo** | Modest | — |
| **Counter-stack total** | **~$3,150-4,400/mo** | | |

If swapping ClickHouse MVs for **Materialize streaming SQL**: add ~$800-1,500/mo. Total **~$4,000-6,000/mo**. Still half of Claude's honest re-price, one-sixth of Claude's stated number.

## Verdict

**Claude's $35K is not arithmetic, it's a vibe.** Every line item is over by 3-10x except Timestream, which lands by accident because Timestream's write+TCU pricing is genuinely punitive — but the product is closed to new customers as of June 2025, so the number is right for a product that no longer exists.

**The honest envelope for the proposed counter-stack is $3-5K/mo.** That gives ~10x headroom under the $50K/mo brief ceiling, room for Snowflake/BigQuery export, room to grow to 500M events/day (10x) without re-architecture.

**What this means for the submission:** every cost claim in the 4-page answer needs a `[Benchmarked: AWS calc URL]` label, not Claude's gut-estimates. This is the lowest-effort, highest-return source-labeling work — Claude's whole table is unlabeled, so we're competing on labels alone, never mind the actual numbers.
