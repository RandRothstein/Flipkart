# 🚀 Flipkart Finance ETL Pipelines & Automation Scripts

This repository contains production-grade ETL pipelines, batch ingestion tools, APIs, and cron jobs used on Flipkart's internal servers for automating finance data processing and reporting.

---

## 📦 Contents

- [Overview](#overview)
- [Production ETL Pipelines](#production-etl-pipelines)
- [5MB Batch Ingestor to GCP](#5mb-batch-ingestor-to-gcp)
- [Scripts](#scripts)
- [Cronjobs](#cronjobs)
- [APIs](#apis)
- [Usage](#usage)
---

## 📄 Overview

This project includes:

- Daily finance-related ETL pipelines (Invoices, Accruals, I2P, etc.)
- GCP ingestion tooling (`fdp-batch-ingestor`)
- Automated cron jobs for scheduled processing
- Lightweight REST APIs for triggering and monitoring workflows
- Scripts running on Flipkart’s production servers

---

## ⚙️ Production ETL Pipelines

These pipelines are designed for ingesting millions of records reliably and are run daily on production environments.

---

## ☁️ 5MB Batch Ingestor to GCP

Used to ingest finance reports to Flipkart's GCP data lake in controlled 5MB chunks. This ensures upload size constraints are met and reduces the risk of timeouts.


