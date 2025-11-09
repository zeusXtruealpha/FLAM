# QueueCTL - CLI Background Job Queue System

`queuectl` is a CLI-based background job queue system built in Python. It is designed to be a minimal, production-grade service that manages background jobs, supports concurrent workers, handles automatic retries with exponential backoff, and includes a Dead Letter Queue (DLQ) for failed jobs.

You can go through the Demo video in the below link
[https://drive.google.com/file/d/1ZqBCZzYuAP73mRjqW_ILx72pshVIiALg/view?usp=sharing](https://drive.google.com/file/d/1ZqBCZzYuAP73mRjqW_ILx72pshVIiALg/view?usp=sharing)

---

## Features

- **Persistent Storage**: Job data persists across restarts using a **SQLite** database
- **Concurrent Workers**: Run multiple workers in parallel to process jobs concurrently
- **Automatic Retries**: Failed jobs are automatically retried with **exponential backoff** delay
- **Dead Letter Queue (DLQ)**: Jobs that fail all retry attempts are moved to the DLQ for manual inspection
- **Configuration Management**: CLI-based commands to view and set configuration like retry counts
- **Web Dashboard**: A built-in web dashboard provides real-time monitoring of job states
- **Clean CLI Interface**: A full-featured CLI for enqueuing, listing, and managing jobs

---

## How It Works: "What Happens"

This system uses a central database (SQLite) as the single source of truth, allowing multiple processes to coordinate.

### 1. Enqueueing (The "To-Do" List)
When you run `queuectl enqueue ...`, you are creating a new row in the `jobs.db` database. This new job is given the state **`pending`**. It's now in the "to-do" list, waiting for a worker.

### 2. Working (Doing the Job)
- You start one or more workers using `queuectl worker start`
- Each worker is a separate process that constantly polls the database, looking for jobs
- When a worker finds a `pending` job, it "locks" it by immediately changing its state to **`processing`**. This prevents any other worker from grabbing the same job
- The worker then executes the job's `command` (e.g., `timeout /t 10`) using a subprocess

### 3. Finishing (The Outcome)
- **If the command succeeds** (exits with code 0): The worker updates the job's state to **`completed`**. The job is now finished
- **If the command fails** (exits with a non-zero code): The worker updates the state to **`failed`** and calculates the next retry time using exponential backoff (e.g., 2^attempts seconds)
- The job will remain in the `failed` state until its `next_retry_at` time is in the past. Then, a worker is allowed to pick it up, lock it (set to `processing`), and try again

### 4. Permanent Failure (The "Too-Hard" Pile)
If a job fails more than its `max_retries` (e.g., 3 times), the worker will give up. Instead of retrying, it sets the job's state to **`dead`**. This moves the job into the Dead Letter Queue (DLQ), removing it from the main workflow so it can be manually reviewed.

---

## Setup and Installation

### 1. Clone Repository
```bash
git clone https://github.com/zeusXtruealpha/FLAM_Niranjan
cd FLAM_Niranjan
```

### 2. Install Dependencies
(A virtual environment is recommended)
```bash
pip install -r requirements.txt
```

### 3. Run
All commands are run from your project's root directory using the `python -m queuectl.cli` module.

---

## CLI Command Reference

All examples are for the **Windows Command Prompt (cmd.exe)**.

### 1. Enqueue a Job

Adds a new job to the queue. The JSON must be wrapped in `"` and all internal quotes must be escaped with `\`.

**Syntax:**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"<job-id>\", ...}"
```

**Examples:**

**Enqueue a simple job:**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-1\",\"command\":\"echo Hello World\"}"
```

**Enqueue a long-running job (sleeps for 10 sec):**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"long-job-1\",\"command\":\"timeout /t 10 /nobreak\"}"
```

**Enqueue a job that will fail:**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"fail-job-1\",\"command\":\"nonexistent-command\"}"
```

**Enqueue a job with custom max retries:**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"custom-retry-job\",\"command\":\"echo Test\",\"max_retries\":5}"
```

### 2. Manage Workers

Workers are the processes that run the jobs. You must run this in its own terminal.

**Start 3 workers:**
```cmd
python -m queuectl.cli worker start --count 3
```

**Start 1 worker:**
```cmd
python -m queuectl.cli worker start --count 1
```

**Stop workers gracefully:**
(Run this from a *different* terminal)
```cmd
python -m queuectl.cli worker stop
```

### 3. Check Status

Get a quick summary of all job states.

```cmd
python -m queuectl.cli status
```

---

## Test Scenarios

### Scenario 1: Basic Job Completes Successfully

**Terminal 1: Enqueue the job**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-success-1\",\"command\":\"timeout /t 5 /nobreak\"}"
```

**Terminal 2: Start the worker**
```cmd
python -m queuectl.cli worker start --count 1
```

**Terminal 1: Check for completion after ~5 seconds**
```cmd
python -m queuectl.cli list --state completed
```

### Scenario 2: Failed Job Retries (Backoff) & Moves to DLQ

**Terminal 1: Enqueue the failing job**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-fail-1\",\"command\":\"nonexistent-command-12345\"}"
```

**Terminal 2: Ensure worker is running**
```cmd
python -m queuectl.cli worker start --count 1
```

**Terminal 1: Check the DLQ after ~15 seconds**
```cmd
python -m queuectl.cli dlq list
```

### Scenario 3: Multiple Workers Process Jobs Without Overlap

**Terminal 1: Enqueue two long jobs**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"multi-job-A\",\"command\":\"timeout /t 10 /nobreak\"}"
python -m queuectl.cli enqueue "{\"id\":\"multi-job-B\",\"command\":\"timeout /t 10 /nobreak\"}"
```

**Terminal 2: Stop any old workers (Ctrl+C), then start 2 new ones**
```cmd
python -m queuectl.cli worker start --count 2
```

### Scenario 4: Job Data Survives Restart

**Terminal 2: Stop any running workers (Ctrl+C)**

**Terminal 1: Enqueue the persistence test job**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-persist-1\",\"command\":\"echo I survived!\"}"
```

**Terminal 1: Verify it's pending**
```cmd
python -m queuectl.cli list --state pending
```

**(Close all terminals, then re-open a new one and cd to your project)**

**Terminal 1 (New): Verify it's still pending**
```cmd
python -m queuectl.cli list --state pending
```

### Scenario 5: Enqueue Duplicate Job ID

**Terminal 1: Enqueue the first job (this will work)**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-duplicate-1\",\"command\":\"echo first job\"}"
```

**Terminal 1: Enqueue the second job with the same ID (this will fail)**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-duplicate-1\",\"command\":\"echo second job\"}"
```

### Scenario 6: Change Backoff Configuration

**Terminal 1: Set a new backoff base**
```cmd
python -m queuectl.cli config set backoff-base 5
```

**Terminal 1: Verify the change**
```cmd
python -m queuectl.cli config get backoff-base
```

**Terminal 2: Stop any running workers (Ctrl+C)**

**Terminal 1: Enqueue a failing job**
```cmd
python -m queuectl.cli enqueue "{\"id\":\"job-backoff-test\",\"command\":\"nonexistent-command-999\"}"
```

**Terminal 2: Start a new worker**
```cmd
python -m queuectl.cli worker start --count 1
```

**(Observe the worker log for a "5.0s" retry delay)**

**Terminal 1 (Cleanup): Reset the config to default**
```cmd
python -m queuectl.cli config set backoff-base 2
```

---

## Architecture Overview

![WhatsApp Image 2025-11-09 at 22 06 27_c2512d5f](https://github.com/user-attachments/assets/6d9a873f-bb53-463a-86ca-23c3f96032b7)


### Components

- **`cli.py`** (Controller): The main CLI entry point using `click`. It parses user input and calls the appropriate manager

- **`storage.py`** (Model/Persistence): The persistence layer. It handles all SQLite database connections, queries, and transactions. This is the only part of the app that "knows" SQL

- **`worker.py`** (Logic): Contains the `WorkerManager` and `Worker` classes. The `WorkerManager` starts and stops worker processes. The `Worker` class contains the core logic for fetching, locking, executing (`subprocess.Popen`), and updating jobs

- **`models.py`** (Data Structure): Defines the `Job` dataclass and `JobState` constants, ensuring clean data handling

- **`config.py`** (Configuration): Manages reading/writing the `config.json` file

- **`dashboard.py`** (View): A simple `Flask` app that reads from `storage.py` to display data

### Job Lifecycle

Jobs move between states based on worker actions:

1. **`pending`**: A job is created (from `enqueue`)
2. **`processing`**: A worker locks the job and begins execution
3. **`completed`**: The job's command exits with code 0
4. **`failed`**: The command exits with a non-zero code
   - If `attempts < max_retries`, the job is scheduled for a future retry (with exponential backoff) and will return to `pending` when ready
   - If `attempts >= max_retries`, the job is moved to...
5. **`dead`**: The job is moved to the Dead Letter Queue (DLQ) for manual review

---

## Assumptions & Trade-offs

### Storage (SQLite)
- **Pro:** Zero-configuration, serverless, and file-based. Perfect for a self-contained CLI tool and fulfills the persistence requirement simply
- **Con:** Not ideal for extremely high-concurrency, multi-server distributed systems (where RabbitMQ, Redis, or PostgreSQL would be better)

### Concurrency (Threading)
- **Pro:** The `WorkerManager` uses Python's `threading` to run multiple workers. This is simple to manage and works well for I/O-bound tasks (like waiting for a subprocess to finish)
- **Con:** Due to the Global Interpreter Lock (GIL), this model does not provide true parallelism for CPU-bound tasks

### Platform (Windows)
- **Pro:** The solution is fully functional on Windows
- **Con:** The `enqueue` command syntax is complex due to `cmd.exe`'s quote handling. The test commands also use Windows-specific commands (`timeout`) instead of cross-platform commands (`sleep`)

### Job Execution (`shell=True`)
- **Pro:** Allows for complex commands to be run easily (e.g., `echo "hi" > file.txt`)
- **Con:** This is a potential security risk if a user can enqueue a malicious command. It assumes all commands are from a trusted source

---

## Testing Instructions

You can test all core flows using the automated script or by running the manual scenarios.

### Test 1: Automated Validation Script 

This script automatically cleans the environment and runs all 6 core scenarios.

1. Save the code below as `validate.py` in your project's root folder
2. From your Command Prompt, run the script:

```cmd
python validate.py
```

**Validation Script:**
```python
#!/usr/bin/env python3
"""
Comprehensive validation script for QueueCTL core functionality.

This script tests:
1. Basic Job Success (Simple & Long-Running)
2. Failure -> Retry -> DLQ
3. Concurrency (Multiple workers)
4. Job Persistence (Worker restart)
5. DLQ Retry Functionality
6. Configuration (Edge Case)

It is designed to be run from the command line (cmd.exe) and will
clean up the environment, run all tests, and report a summary.
"""
```

---
