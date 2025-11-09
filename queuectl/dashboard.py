"""Enhanced Web Dashboard for QueueCTL with Auto-Update"""

from flask import Flask, jsonify, render_template_string
from queuectl.storage import Storage
from queuectl.models import JobState
from queuectl.config import Config
from queuectl.worker import WorkerManager
import json
from datetime import datetime
import os

app = Flask(__name__)

# Enhanced HTML template with modern design and auto-update
ENHANCED_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QueueCTL Dashboard</title>
    <style>
        :root {
            --primary: #2563eb;
            --primary-dark: #1d4ed8;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --info: #06b6d4;
            --gray-50: #f9fafb;
            --gray-100: #f3f4f6;
            --gray-200: #e5e7eb;
            --gray-300: #d1d5db;
            --gray-400: #9ca3af;
            --gray-500: #6b7280;
            --gray-600: #4b5563;
            --gray-700: #374151;
            --gray-800: #1f2937;
            --gray-900: #111827;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: var(--gray-800);
        }
        
        .dashboard {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 8px;
        }
        
        .header p {
            color: var(--gray-600);
            font-size: 1.1rem;
        }
        
        .controls {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
        }
        
        .control-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .btn-primary {
            background: var(--primary);
            color: white;
        }
        
        .btn-primary:hover {
            background: var(--primary-dark);
            transform: translateY(-1px);
        }
        
        .btn-secondary {
            background: var(--gray-200);
            color: var(--gray-700);
        }
        
        .btn-secondary:hover {
            background: var(--gray-300);
        }
        
        .auto-update-toggle {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .toggle-switch {
            position: relative;
            width: 50px;
            height: 24px;
            background: var(--gray-300);
            border-radius: 12px;
            cursor: pointer;
            transition: background 0.3s ease;
        }
        
        .toggle-switch.active {
            background: var(--success);
        }
        
        .toggle-switch::after {
            content: '';
            position: absolute;
            top: 2px;
            left: 2px;
            width: 20px;
            height: 20px;
            background: white;
            border-radius: 50%;
            transition: transform 0.3s ease;
        }
        
        .toggle-switch.active::after {
            transform: translateX(26px);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 25px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            text-align: center;
            transition: transform 0.2s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-2px);
        }
        
        .stat-card.pending {
            border-top: 4px solid var(--warning);
        }
        
        .stat-card.processing {
            border-top: 4px solid var(--info);
        }
        
        .stat-card.completed {
            border-top: 4px solid var(--success);
        }
        
        .stat-card.failed {
            border-top: 4px solid var(--danger);
        }
        
        .stat-card.dead {
            border-top: 4px solid var(--gray-600);
        }
        
        .stat-number {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 5px;
        }
        
        .stat-label {
            color: var(--gray-600);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.875rem;
            letter-spacing: 0.05em;
        }
        
        .jobs-section {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 25px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }
        
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        
        .section-title {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--gray-800);
        }
        
        .job-filters {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .filter-btn {
            padding: 8px 16px;
            border: 1px solid var(--gray-300);
            background: white;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
            font-size: 0.875rem;
        }
        
        .filter-btn.active {
            background: var(--primary);
            color: white;
            border-color: var(--primary);
        }
        
        .jobs-container {
            max-height: 400px;
            overflow-y: auto;
        }
        
        .job-item {
            background: var(--gray-50);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 10px;
            border-left: 4px solid var(--gray-400);
            transition: all 0.2s ease;
        }
        
        .job-item:hover {
            background: var(--gray-100);
            transform: translateX(2px);
        }
        
        .job-item.pending {
            border-left-color: var(--warning);
        }
        
        .job-item.processing {
            border-left-color: var(--info);
        }
        
        .job-item.completed {
            border-left-color: var(--success);
        }
        
        .job-item.failed {
            border-left-color: var(--danger);
        }
        
        .job-item.dead {
            border-left-color: var(--gray-600);
        }
        
        .job-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 8px;
        }
        
        .job-id {
            font-family: 'Courier New', monospace;
            font-weight: 600;
            color: var(--gray-800);
            font-size: 0.875rem;
        }
        
        .job-state {
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .state-pending {
            background: rgba(245, 158, 11, 0.1);
            color: var(--warning);
        }
        
        .state-processing {
            background: rgba(6, 182, 212, 0.1);
            color: var(--info);
        }
        
        .state-completed {
            background: rgba(16, 185, 129, 0.1);
            color: var(--success);
        }
        
        .state-failed {
            background: rgba(239, 68, 68, 0.1);
            color: var(--danger);
        }
        
        .state-dead {
            background: rgba(75, 85, 99, 0.1);
            color: var(--gray-600);
        }
        
        .job-command {
            font-family: 'Courier New', monospace;
            font-size: 0.875rem;
            color: var(--gray-600);
            margin-bottom: 8px;
            word-break: break-all;
        }
        
        .job-footer {
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.75rem;
            color: var(--gray-500);
        }
        
        .job-attempts {
            font-weight: 600;
        }
        
        .job-error {
            background: rgba(239, 68, 68, 0.05);
            border: 1px solid rgba(239, 68, 68, 0.2);
            border-radius: 4px;
            padding: 8px;
            margin-top: 8px;
            font-size: 0.75rem;
            color: var(--danger);
            font-family: 'Courier New', monospace;
        }
        
        .no-jobs {
            text-align: center;
            padding: 40px;
            color: var(--gray-500);
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: var(--gray-500);
        }
        
        .status-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        
        .status-indicator.connected {
            background: var(--success);
        }
        
        .status-indicator.disconnected {
            background: var(--danger);
        }
        
        @keyframes pulse {
            0% {
                opacity: 1;
            }
            50% {
                opacity: 0.5;
            }
            100% {
                opacity: 1;
            }
        }
        
        .last-updated {
            text-align: center;
            color: var(--gray-500);
            font-size: 0.875rem;
            margin-top: 20px;
        }
        
        .update-indicator {
            display: inline-block;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--success);
            margin-left: 8px;
            animation: pulse 1s infinite;
        }
        
        @media (max-width: 768px) {
            .dashboard {
                padding: 10px;
            }
            
            .header h1 {
                font-size: 2rem;
            }
            
            .controls {
                flex-direction: column;
                align-items: stretch;
            }
            
            .control-group {
                justify-content: center;
            }
            
            .stats-grid {
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            }
            .section-header {
                flex-direction: column; /* Stack title and buttons */
                align-items: flex-start; /* Align title to the left */
                gap: 15px; /* Add space between title and buttons */
            }
            
            .job-filters {
                flex-wrap: wrap; /* Let buttons wrap to the next line */
                margin-bottom: 0; /* Remove extra margin */
            }
        }
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>QueueCTL Dashboard</h1>
            <p>Real-time Job Queue Management</p>
        </div>
        
        <div class="controls">
            <div class="control-group">
                <button class="btn btn-primary" onclick="refreshData()">
                    <span></span> Refresh Now
                </button>
                <button class="btn btn-secondary" onclick="clearFilters()">
                    <span></span> Clear Filters
                </button>
            </div>
            
            <div class="auto-update-toggle">
                <span>Auto Update:</span>
                <div class="toggle-switch active" id="autoUpdateToggle" onclick="toggleAutoUpdate()"></div>
                <span id="autoUpdateStatus">ON</span>
            </div>
            
            <div class="control-group">
                <span class="status-indicator connected" id="connectionStatus"></span>
                <span id="connectionText">Connected</span>
            </div>
        </div>
        
        <div class="stats-grid" id="statsContainer">
            <div class="loading">Loading statistics...</div>
        </div>
        
        <div class="jobs-section">
            <div class="section-header">
                <h2 class="section-title">Job Queue</h2>
                <div class="job-filters" id="jobFilters">
                    <button class="filter-btn active" data-state="all">All</button>
                    <button class="filter-btn" data-state="pending">Pending</button>
                    <button class="filter-btn" data-state="processing">Processing</button>
                    <button class="filter-btn" data-state="completed">Completed</button>
                    <button class="filter-btn" data-state="failed">Failed</button>
                    <button class="filter-btn" data-state="dead">Dead</button>
                </div>
            </div>
            
            <div class="jobs-container" id="jobsContainer">
                <div class="loading">Loading jobs...</div>
            </div>
            
            <div class="last-updated" id="lastUpdated"></div>
        </div>
    </div>

    <script>
        let autoUpdateInterval = null;
        let currentFilter = 'all';
        let isAutoUpdateEnabled = true;
        let lastUpdateTime = null;
        
        function init() {
            setupEventListeners();
            refreshData();
            startAutoUpdate();
        }
        
        function setupEventListeners() {
            // Job filter buttons
            document.querySelectorAll('.filter-btn').forEach(btn => {
                btn.addEventListener('click', function() {
                    const state = this.dataset.state;
                    setFilter(state);
                });
            });
            
            // Refresh on visibility change
            document.addEventListener('visibilitychange', function() {
                if (!document.hidden && isAutoUpdateEnabled) {
                    refreshData();
                }
            });
        }
        
        function setFilter(state) {
            currentFilter = state;
            
            // Update button states
            document.querySelectorAll('.filter-btn').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.state === state);
            });
            
            // Reload jobs with filter
            loadJobs();
        }
        
        function clearFilters() {
            setFilter('all');
        }
        
        function toggleAutoUpdate() {
            isAutoUpdateEnabled = !isAutoUpdateEnabled;
            const toggle = document.getElementById('autoUpdateToggle');
            const status = document.getElementById('autoUpdateStatus');
            
            toggle.classList.toggle('active', isAutoUpdateEnabled);
            status.textContent = isAutoUpdateEnabled ? 'ON' : 'OFF';
            
            if (isAutoUpdateEnabled) {
                startAutoUpdate();
                refreshData();
            } else {
                stopAutoUpdate();
            }
        }
        
        function startAutoUpdate() {
            stopAutoUpdate(); // Clear existing interval
            if (isAutoUpdateEnabled) {
                autoUpdateInterval = setInterval(refreshData, 5000); // Update every 5 seconds
            }
        }
        
        function stopAutoUpdate() {
            if (autoUpdateInterval) {
                clearInterval(autoUpdateInterval);
                autoUpdateInterval = null;
            }
        }
        
        function refreshData() {
            loadStats();
            loadJobs();
            updateConnectionStatus(true);
        }
        
        function updateConnectionStatus(connected) {
            const indicator = document.getElementById('connectionStatus');
            const text = document.getElementById('connectionText');
            
            if (connected) {
                indicator.className = 'status-indicator connected';
                text.textContent = 'Connected';
            } else {
                indicator.className = 'status-indicator disconnected';
                text.textContent = 'Disconnected';
            }
        }
        
        function loadStats() {
            fetch('/api/status')
                .then(response => {
                    if (!response.ok) throw new Error('Network response was not ok');
                    return response.json();
                })
                .then(data => {
                    updateStats(data.stats);
                    updateConnectionStatus(true);
                })
                .catch(error => {
                    console.error('Error loading stats:', error);
                    updateConnectionStatus(false);
                    document.getElementById('statsContainer').innerHTML = 
                        '<div class="stat-card"><div class="stat-number">⚠️</div><div class="stat-label">Connection Error</div></div>';
                });
        }
        
        function updateStats(stats) {
            const container = document.getElementById('statsContainer');
            const states = ['pending', 'processing', 'completed', 'failed', 'dead'];
            
            container.innerHTML = states.map(state => `
                <div class="stat-card ${state}">
                    <div class="stat-number">${stats[state] || 0}</div>
                    <div class="stat-label">${state.charAt(0).toUpperCase() + state.slice(1)}</div>
                </div>
            `).join('');
        }
        
        function loadJobs() {
            fetch('/api/jobs')
                .then(response => {
                    if (!response.ok) throw new Error('Network response was not ok');
                    return response.json();
                })
                .then(data => {
                    displayJobs(data.jobs || []);
                    updateConnectionStatus(true);
                })
                .catch(error => {
                    console.error('Error loading jobs:', error);
                    updateConnectionStatus(false);
                    document.getElementById('jobsContainer').innerHTML = 
                        '<div class="no-jobs">❌ Error loading jobs. Check connection.</div>';
                });
        }
        
        function displayJobs(jobs) {
            const container = document.getElementById('jobsContainer');
            
            // Filter jobs based on current filter
            let filteredJobs = jobs;
            if (currentFilter !== 'all') {
                filteredJobs = jobs.filter(job => job.state === currentFilter);
            }
            
            // Sort by created_at (newest first)
            filteredJobs.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
            
            // Show only the 20 most recent jobs
            const recentJobs = filteredJobs.slice(0, 20);
            
            if (recentJobs.length === 0) {
                container.innerHTML = `<div class="no-jobs">No ${currentFilter === 'all' ? '' : currentFilter} jobs found</div>`;
                return;
            }
            
            container.innerHTML = recentJobs.map(job => `
                <div class="job-item ${job.state}">
                    <div class="job-header">
                        <div class="job-id">${escapeHtml(job.id)}</div>
                        <div class="job-state state-${job.state}">${job.state}</div>
                    </div>
                    <div class="job-command">${escapeHtml(job.command)}</div>
                    <div class="job-footer">
                        <div class="job-attempts">Attempts: ${job.attempts}/${job.max_retries}</div>
                        <div>Created: ${formatDate(job.created_at)}</div>
                    </div>
                    ${job.error ? `<div class="job-error">${escapeHtml(job.error)}</div>` : ''}
                </div>
            `).join('');
            
            lastUpdateTime = new Date();
            updateLastUpdated();
        }
        
        function formatDate(dateString) {
            const date = new Date(dateString);
            const now = new Date();
            const diffMs = now - date;
            const diffMins = Math.floor(diffMs / 60000);
            const diffHours = Math.floor(diffMs / 3600000);
            const diffDays = Math.floor(diffMs / 86400000);
            
            if (diffMins < 1) return 'Just now';
            if (diffMins < 60) return `${diffMins}m ago`;
            if (diffHours < 24) return `${diffHours}h ago`;
            return `${diffDays}d ago`;
        }
        
        function updateLastUpdated() {
            const element = document.getElementById('lastUpdated');
            if (lastUpdateTime) {
                element.innerHTML = `Last updated: ${formatDate(lastUpdateTime.toISOString())} <span class="update-indicator"></span>`;
            }
        }
        
        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // Initialize dashboard
        init();
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            stopAutoUpdate();
        });
    </script>
</body>
</html>
'''


@app.route('/')
def index():
    """Main dashboard page - returns enhanced HTML"""
    return render_template_string(ENHANCED_TEMPLATE)


@app.route('/api/status')
def api_status():
    """Get system status with enhanced statistics"""
    storage = Storage()
    
    stats = storage.get_stats()
    
    # Enhanced worker detection - check if worker processes are actually running
    workers_running = False
    try:
        worker_manager = WorkerManager()
        workers_running = worker_manager.is_running()
    except:
        # Fallback to processing jobs check
        workers_running = stats.get('processing', 0) > 0
    
    # Add system information
    system_info = {
        'timestamp': datetime.now().isoformat(),
        'uptime': 'Running',
        'version': '1.0.0'
    }
    
    return jsonify({
        'stats': stats,
        'workers_running': workers_running,
        'system': system_info
    })


@app.route('/api/jobs')
def api_jobs():
    """Get all jobs with enhanced data"""
    storage = Storage()
    jobs = storage.get_all_jobs()
    
    jobs_data = []
    for job in jobs:
        job_dict = job.to_dict()
        # Add computed fields for better display
        job_dict['should_move_to_dlq'] = job.should_move_to_dlq()
        job_dict['next_retry_in'] = job.get_next_retry_delay() if job.state == 'FAILED' else None
        jobs_data.append(job_dict)
    
    return jsonify({'jobs': jobs_data})


def run_dashboard(host='127.0.0.1', port=5000, debug=False):
    """Run the minimal dashboard server"""
    print(f"🚀 Starting QueueCTL Minimal Dashboard...")
    print(f"📊 Dashboard available at: http://{host}:{port}")
    print(f"⏹️  Press Ctrl+C to stop")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_dashboard()

