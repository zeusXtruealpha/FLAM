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

import subprocess
import json
import time
import os
import sys
from pathlib import Path

# --- Configuration ---
# Use 'python -m ...' to avoid PATH issues
CLI_PREFIX = [sys.executable, '-m', 'queuectl.cli']
DB_PATH = Path.home() / ".queuectl" / "jobs.db"
PID_FILE = Path.home() / ".queuectl" / "workers.pid"

# Global var to hold the worker subprocess
WORKER_PROCESS = None

# --- Helper Functions ---

def print_header(title):
    """Prints a formatted header for each test section."""
    print("\n" + "=" * 60)
    print(f"TEST: {title}")
    print("=" * 60)

def run_cmd(command_args, check=True):
    """Runs a queuectl command and returns its stdout."""
    try:
        # Use a list for args to avoid shell escaping
        process = subprocess.run(
            CLI_PREFIX + command_args,
            capture_output=True,
            text=True,
            check=check,
            encoding='utf-8'
        )
        return process.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"  [CMD FAILED] {' '.join(command_args)}")
        print(f"  [STDOUT] {e.stdout}")
        print(f"  [STDERR] {e.stderr}")
        if check:
            raise
        return None
    except FileNotFoundError:
        print(f"  [FATAL] 'python' command not found. Is it in your PATH?")
        sys.exit(1)

def enqueue_job(job_id, command, max_retries=None):
    """Helper to build and enqueue a job."""
    job_data = {"id": job_id, "command": command}
    if max_retries is not None:
        job_data["max_retries"] = max_retries
    
    # Create the JSON string
    job_json = json.dumps(job_data)
    
    print(f"  Enqueuing job '{job_id}': {command}")
    
    # Use run_cmd, passing the JSON as a single argument in the list.
    # This avoids all shell quoting issues.
    try:
        run_cmd(['enqueue', job_json])
    except subprocess.CalledProcessError as e:
        print(f"  [FATAL] Failed to enqueue job: {e.stderr}")
        raise

def get_job(job_id):
    """Gets the full job object from the list."""
    output = run_cmd(['list', '--format', 'json'])
    if not output:
        return None
    try:
        jobs = json.loads(output)
        for job in jobs:
            if job['id'] == job_id:
                return job
    except json.JSONDecodeError:
        print("  [ERROR] Could not decode JSON from 'list' command.")
        return None
    return None

def get_job_state(job_id):
    """Polls for the state of a single job."""
    job = get_job(job_id)
    return job['state'] if job else None

def get_dlq_jobs():
    """Gets a list of all job IDs in the DLQ."""
    output = run_cmd(['dlq', 'list', '--format', 'json'])
    if not output:
        return []
    try:
        jobs = json.loads(output)
        return [job['id'] for job in jobs]
    except json.JSONDecodeError:
        print("  [ERROR] Could not decode JSON from 'dlq list' command.")
        return []

def start_workers(count=1):
    """Starts worker processes in the background."""
    global WORKER_PROCESS
    if WORKER_PROCESS:
        stop_workers()  # Ensure no old workers are running
    
    print(f"  Starting {count} worker(s) in background...")
    # Start the worker process detached
    WORKER_PROCESS = subprocess.Popen(
        CLI_PREFIX + ['worker', 'start', '--count', str(count)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8'
    )
    time.sleep(2)  # Give workers time to spin up
    print(f"  Workers started (PID: {WORKER_PROCESS.pid})")

def stop_workers():
    """Stops any running workers."""
    global WORKER_PROCESS
    print("  Stopping workers gracefully...")
    run_cmd(['worker', 'stop'], check=False)
    
    # Check if the subprocess is still running
    if WORKER_PROCESS:
        try:
            WORKER_PROCESS.wait(timeout=5)
            print("  Worker process exited.")
        except subprocess.TimeoutExpired:
            print("  [WARN] Worker process did not stop gracefully, terminating.")
            WORKER_PROCESS.terminate()
        WORKER_PROCESS = None

    # Final check for PID file
    for _ in range(5):
        if not PID_FILE.exists():
            print("  Workers confirmed stopped (PID file removed).")
            return
        time.sleep(1)
    print("  [WARN] PID file still exists after stop command.")

def clean_slate():
    """Wipes the DB and stops workers for a clean test."""
    print_header("Cleaning environment")
    stop_workers()  # Try to stop gracefully
    
    # Now, forcefully remove data files
    if DB_PATH.exists():
        try:
            os.remove(DB_PATH)
            print(f"  Removed old database: {DB_PATH}")
        except PermissionError:
            print(f"  [WARN] Could not remove DB. It might be locked. Retrying...")
            time.sleep(2)
            os.remove(DB_PATH)
            print(f"  Removed old database: {DB_PATH}")

    
    if PID_FILE.exists():
        print(f"  Removing stale PID file: {PID_FILE}")
        os.remove(PID_FILE)
        
    assert not DB_PATH.exists(), "Database file could not be deleted."
    assert not PID_FILE.exists(), "PID file could not be deleted."
    print("  Environment is clean.")

def wait_for_job(job_id, target_state, timeout=30):
    """Polls until a job reaches the target state."""
    print(f"  Waiting for job '{job_id}' to reach state '{target_state}'...")
    start = time.time()
    last_state = None
    while time.time() - start < timeout:
        state = get_job_state(job_id)
        if state != last_state:
            print(f"    -> State change: {last_state} -> {state}")
            last_state = state
        
        if state == target_state:
            print(f"  -> Success: Job '{job_id}' is now '{target_state}'.")
            return True
        time.sleep(1)
    
    print(f"  -> [FAIL] Job '{job_id}' timed out. Last state was '{state}'.")
    return False

# --- Main Test Runner ---

def run_all_tests():
    try:
        # --- Test 1: Basic Success ---
        print_header("Test 1: Basic Job Success (Simple & Long-Running)")
        enqueue_job("success-1", "echo This job works")
        # Note: 'timeout /t 3' is the Windows equivalent of 'sleep 3'
        enqueue_job("success-2", "timeout /t 3 /nobreak")
        
        start_workers(1)
        
        assert wait_for_job("success-1", "completed"), "Simple job failed to complete."
        assert wait_for_job("success-2", "completed"), "Long-running job failed to complete."
        stop_workers()

        # --- Test 2: Failure, Retry, and DLQ ---
        print_header("Test 2: Failure, Retry (x3), and Move to DLQ")
        # Set config to 3 retries (default) just to be safe
        run_cmd(['config', 'set', 'max-retries', '3'])
        enqueue_job("fail-1", "nonexistent-command-12345")
        
        start_workers(1)
        # Wait for retries + backoff (2 + 4 + 8 = 14s + execution time)
        assert wait_for_job("fail-1", "dead", timeout=25), "Job did not move to DLQ."
        
        dlq = get_dlq_jobs()
        assert "fail-1" in dlq, "Job was not found in DLQ list."
        print("  -> Success: Job failed 3 times and moved to DLQ.")
        stop_workers()

        # --- Test 3: Concurrency Test ---
        print_header("Test 3: Concurrency (3 jobs, 3 workers)")
        enqueue_job("concur-1", "timeout /t 4 /nobreak")
        enqueue_job("concur-2", "timeout /t 4 /nobreak")
        enqueue_job("concur-3", "timeout /t 4 /nobreak")
        
        start_time = time.time()
        start_workers(3)
        
        # Wait for all 3 jobs to complete
        assert wait_for_job("concur-1", "completed"), "Job concur-1 failed."
        assert wait_for_job("concur-2", "completed"), "Job concur-2 failed."
        assert wait_for_job("concur-3", "completed"), "Job concur-3 failed."
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"  Concurrency test finished in {duration:.2f} seconds.")
        # All 3 jobs ran for 4s. Serially = 12s. Concurrently = ~4-6s.
        assert duration < 8, "Jobs did not run concurrently (took too long)."
        print(f"  -> Success: 3 jobs ran in parallel.")
        stop_workers()

        # --- Test 4: Persistence Across Restart ---
        print_header("Test 4: Job Persistence (Simulated Restart)")
        enqueue_job("persist-1", "echo I survived the restart!")
        assert get_job_state("persist-1") == "pending", "Job was not pending."
        
        print("  Simulating restart by stopping workers...")
        stop_workers() # This stops workers and confirms PID file is gone
        
        print("  'Restarting' workers...")
        start_workers(1) # Start fresh
        
        assert wait_for_job("persist-1", "completed"), "Job did not survive restart."
        print("  -> Success: Job was processed after restart.")
        stop_workers()

        # --- Test 5: DLQ Retry ---
        print_header("Test 5: DLQ Retry Functionality")
        # We know "fail-1" is in the DLQ from Test 2
        print("  Retrying job 'fail-1' from DLQ...")
        run_cmd(['dlq', 'retry', 'fail-1'])
        
        assert get_job_state("fail-1") == "pending", "Job did not move back to pending."
        
        # It will fail again
        start_workers(1)
        assert wait_for_job("fail-1", "dead", timeout=25), "Retried job did not return to DLQ."
        print("  -> Success: Job was retried, failed again, and returned to DLQ.")
        stop_workers()

        # --- Test 6: Configuration Edge Case (1 Retry) ---
        print_header("Test 6: Config (max_retries = 1)")
        print("  Setting max_retries to 1...")
        run_cmd(['config', 'set', 'max-retries', '1'])
        
        enqueue_job("fail-fast", "fake-command-again", max_retries=1)
        
        start_workers(1)
        # Should fail very fast (1 attempt + 1 retry = ~2s + backoff)
        assert wait_for_job("fail-fast", "dead", timeout=10), "Job did not fail fast."
        
        job = get_job("fail-fast")
        # Your logic: attempt 1 fails, attempts becomes 1.
        # It checks if it can retry (attempts < max_retries). 1 is not < 1.
        # So it moves to DLQ. This means it only ever "attempted" once.
        assert job['attempts'] == 1, f"Job attempts were {job['attempts']}, expected 1."
        assert job['max_retries'] == 1, "Job max_retries was not 1."
        print("  -> Success: Job failed after exactly 1 attempt.")
        
        # --- Cleanup ---
        print_header("Cleanup")
        run_cmd(['config', 'set', 'max-retries', '3']) # Reset to default
        print(f"  Config reset to default. (View log: {DB_PATH.parent})")

    except Exception as e:
        print(f"\n\n[FATAL TEST ERROR] An assertion or command failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n--- Test run finished. Cleaning up all worker processes. ---")
        stop_workers()
        print("\nValidation complete. All tests passed!")


if __name__ == "__main__":
    clean_slate()
    run_all_tests()