#!/bin/bash

# =============================================================================
# ZFS Local Backup Script
# Script Name: local_zfs_backup.sh
# Version: 1.0
# Date: YYYY-MM-DD
#
# This script automates the process of backing up ZFS datasets to another
# local ZFS dataset. It supports both full and incremental backups, manages
# retention and pruning, generates manifests, updates status files, and can
# send notifications via Mailgun.
#
# Key Features:
#   - Full and incremental ZFS snapshot streaming to local ZFS dataset
#   - Target dataset management and pruning based on set count and size
#   - Manifest file generation for backup sets
#   - MOTD status file updates for system status visibility
#   - Email notifications for backup events and errors
#   - Robust error handling and logging
#
# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================
#
# BASIC USAGE:
#   sudo /usr/local/bin/local_zfs_backup.sh
#
# CONFIGURATION:
#   Edit the configuration section of this script to set:
#   - SOURCE_DATASETS: ZFS datasets to back up
#   - BACKUP_POOL: Target ZFS dataset for backups
#   - Retention settings, notification preferences, etc.
#
# BACKUP PROCESS:
#   This script creates snapshots with prefix "local_backup_" followed by timestamp
#   Snapshots are sent to the backup location defined in BACKUP_POOL
#   Only one snapshot is kept per dataset by default (configurable)
#
# SYSTEMD SCHEDULING:
#   This script is designed to be run via systemd timer:
#   Timer unit: /etc/systemd/system/local-zfs-backup.timer
#   Service unit: /etc/systemd/system/local-zfs-backup.service
#
# RESTORATION INSTRUCTIONS:
#   To restore a dataset from backup, follow these steps:
#
#   1. List available backups:
#      sudo zfs list -t snapshot | grep "backup/pool"
#
#   2. Restore to original location (CAUTION - this will overwrite data):
#      sudo zfs send backup/pool/pool/dataset1@local_backup_TIMESTAMP | \
#      sudo zfs receive -F pool/dataset1
#
#   3. Restore to alternate location (safer):
#      sudo zfs send backup/pool/pool/dataset1@local_backup_TIMESTAMP | \
#      sudo zfs receive -F pool/restored_dataset1
#
#   4. Mount the restored dataset:
#      sudo zfs set mountpoint=/mnt/restore pool/restored_dataset1
#      sudo zfs mount pool/restored_dataset1
#
# TROUBLESHOOTING:
#   - Check logs: tail -f /var/log/local_zfs_backup.log
#   - Verify snapshots: sudo zfs list -t snapshot | grep local_backup
#   - Manual run with bash -x: sudo bash -x /usr/local/bin/local_zfs_backup.sh
#
# =============================================================================

# --- Configuration Section ---
# Source ZFS datasets to back up
SOURCE_DATASETS=("pool/dataset1" "pool/dataset2")

# Target ZFS dataset where backups will be stored
BACKUP_POOL="backup/pool"

# Prefix for local ZFS snapshots created by this script
SNAPSHOT_PREFIX_LOCAL="local_backup"

# Log file for all script output and errors (from simple script)
LOG_FILE="/var/log/local_zfs_backup.log"

# Number of successful runs before rotating the log file (from simple script)
MAX_SUCCESSFUL_RUNS_FOR_LOG_ROTATE=30

# Directory for storing state files and manifests
STATE_DIR="/var/lib/zfs-local-backup"
MANIFEST_DIR="${STATE_DIR}/manifests"

# Retention and pruning settings:
# Maximum number of backup sets to keep per dataset (keep only one like simple script)
MAX_BACKUP_SETS_PER_DATASET=1

# Number of days between forced full backups
FULL_BACKUP_INTERVAL_DAYS=0

# Enable pruning based on total backup size
ENABLE_TOTAL_SIZE_LIMIT_PRUNING=false

# Maximum allowed total backup size in bytes (approximately 1.8TB)
MAX_TOTAL_SIZE_BYTES="1800000000000"

# Start pruning if total size exceeds this percentage of the limit
PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE=90

# Manifest and status file settings:
# Enable generation of manifest JSON files for each backup set
ENABLE_MANIFEST_GENERATION=true

# Enable updating of a MOTD-style status file for system visibility
ENABLE_MOTD_STATUS_FILE=false

# Path to the MOTD status file
MOTD_STATUS_FILE_PATH="/var/lib/zfs-backup-status/local_backup_status.txt"

# Email notification settings (Mailgun):
# Enable or disable email notifications globally
ENABLE_MAILGUN_NOTIFICATIONS=true  

# Control which types of notifications are sent
NOTIFY_ON_SUCCESS=true                 # Send notification when backup completes successfully
NOTIFY_ON_PARTIAL_FAILURE=true         # Send notification when backup completes with partial failures
NOTIFY_ON_CRITICAL_FAILURE=true        # Send notification when backup fails completely
NOTIFY_SUMMARY_DETAIL_LEVEL="standard" # Options: "minimal", "standard", "verbose"

# Mailgun API key (must be set by the user)
MAILGUN_API_KEY="YOUR_MAILGUN_API_KEY"

# Mailgun domain for sending emails
MAILGUN_DOMAIN="YOUR_MAILGUN_DOMAIN"

# Base URL for Mailgun API. Change to "https://api.mailgun.net/v3" if using in US
MAILGUN_API_BASE_URL="https://api.eu.mailgun.net/v3"

# Recipient email address for notifications
MAILGUN_RECIPIENT_EMAIL="YOUR_RECIPIENT_EMAIL"

# Sender email address for notifications
MAILGUN_SENDER_EMAIL="notifications@${MAILGUN_DOMAIN}"

# Prefix for email subject lines
MAILGUN_SUBJECT_PREFIX="[ZFS Local Backup]"
