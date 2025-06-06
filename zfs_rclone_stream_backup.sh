#!/bin/bash

# =============================================================================
# ZFS Stream Backup to Rclone Remote Script
# Script Name: zfs_rclone_stream_backup.sh
# Version: 1.0
# Date: 2025-05-13
#
# This script automates the process of backing up ZFS datasets to a remote
# storage location using rclone. It supports both full and incremental
# backups, manages retention and pruning, generates manifests, updates
# status files, and can send notifications via Mailgun.
#
# Key Features:
#   - Full and incremental ZFS snapshot streaming to remote storage
#   - Remote storage management and pruning based on set count and size
#   - Manifest file generation for backup sets
#   - MOTD status file updates for system status visibility
#   - Email notifications for backup events and errors
#   - Robust error handling and logging
#
#
# Main takeaway: not fully tested yet, but should work.
#   - Tested: Remote streaming of ZFS snapshots to rclone remote.
#   - Tested: Manifest generation and upload.
#   - Tested: Email notifications via Mailgun.
#   - Tested: Restoration of backups from remote.
#   - Not fully tested: Pruning logic and retention policies.
#   - Not fully tested: Incremental backup logic.
#   - Not fully tested: Error handling in all scenarios.
#   - Not fully tested: MOTD status file updates.
#   - Not fully tested: Compatibility with various ZFS and rclone versions.
#
#
# Important Notes:
#   - This script is designed to be run as a cron job or manually.
#   - This script is a work in progress and may require adjustments
#     based on your specific environment and requirements.
#   - Ensure you have the necessary permissions and configurations
#     in place before running this script.
#   - Always test in a safe environment before deploying in production.
#
#
# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================
#
# BASIC USAGE:
#   sudo /usr/local/bin/zfs_rclone_stream_backup.sh [--backup]
#   sudo /usr/local/bin/zfs_rclone_stream_backup.sh --verify-latest-set ALL
#
# COMMAND OPTIONS:
#   --backup                   Run normal backup (default)
#   --verify-latest-set DATASET  Verify latest backup set for dataset (or "ALL")
#   --verify-set DATASET SETID   Verify specific backup set
#   --verify-all-latest-sets   Verify latest backup set for all datasets
#
# CONFIGURATION:
#   Edit the configuration section of this script to set:
#   - SOURCE_DATASETS: ZFS datasets to back up
#   - RCLONE_REMOTE_NAME: Name of rclone remote
#   - RCLONE_BASE_PATH_ON_REMOTE: Path on remote for backups
#   - Retention settings, notification preferences, etc.
#
# BACKUP PROCESS:
#   This script creates snapshots with prefix "zfsstream_snap_" followed by timestamp
#   Snapshots are compressed with PIGZ and streamed to remote storage
#   Backup sets are kept according to MAX_BACKUP_SETS_PER_DATASET setting
#
# SYSTEMD SCHEDULING:
#   This script is designed to be run via systemd timer:
#   Timer unit: /etc/systemd/system/remote-zfs-backup.timer
#   Service unit: /etc/systemd/system/remote-zfs-backup.service
#
# RESTORATION INSTRUCTIONS:
#   To restore a dataset from remote backup, follow these steps:
#
#   1. List available backups (requires jq):
#      DATASET="zroot/ROOT/debian"  # Replace with your dataset
#      DATASET_SAFE=$(echo "$DATASET" | tr '/' '_')
#      RCLONE_CONFIG="/path/to/rclone.conf"  # Usually ~/.config/rclone/rclone.conf
#      REMOTE="google_drive_crypt"  # Your remote name
#      REMOTE_PATH="serwerholythighble/zfs_streams/$DATASET_SAFE"
#      
#      rclone --config "$RCLONE_CONFIG" lsf "$REMOTE:$REMOTE_PATH"
#
#   2. Download the backup file:
#      BACKUP_FILE="full_${DATASET_SAFE}_set-20250514030000_snap-zfsstream_snap_20250514030000.zfs.gz"
#      rclone --config "$RCLONE_CONFIG" copy "$REMOTE:$REMOTE_PATH/$BACKUP_FILE" .
#
#   3. Restore to a new dataset (safest option):
#      cat "$BACKUP_FILE" | gunzip | sudo zfs receive -F zroot/restored_dataset
#
#   4. Restore to original location (CAUTION - this will overwrite data):
#      cat "$BACKUP_FILE" | gunzip | sudo zfs receive -F "$DATASET"
#
#   5. Mount the restored dataset:
#      sudo zfs set mountpoint=/mnt/restore zroot/restored_dataset
#      sudo zfs mount zroot/restored_dataset
#
# TROUBLESHOOTING:
#   - Check logs: sudo tail -f /var/log/zfs_rclone_stream_backup.log
#   - Verify snapshots: sudo zfs list -t snapshot | grep zfsstream_snap
#   - Test rclone connectivity: rclone --config "$RCLONE_CONFIG" lsd "$REMOTE:"
#   - Check remote space: rclone --config "$RCLONE_CONFIG" size "$REMOTE:$REMOTE_PATH"
#
# =============================================================================

# --- Configuration Section ---
# List of ZFS datasets to back up. Each dataset will be processed in order.
SOURCE_DATASETS=("tank/data" "pool/backups") # Replace with your ZFS datasets
# Name of the rclone remote as defined in your rclone config.
RCLONE_REMOTE_NAME="your_rclone_remote_name"
# Base path on the remote where backups will be stored.
RCLONE_BASE_PATH_ON_REMOTE="remote_path/zfs_backups"
# Path to the rclone configuration file.
RCLONE_CONFIG_PATH="/path/to/your/rclone.conf"
# Prefix for local ZFS snapshots created by this script.
SNAPSHOT_PREFIX_LOCAL="zfsstream_snap"

# Log file for all script output and errors.
LOG_FILE="/var/log/zfs_rclone_stream_backup.log"
# Number of successful runs before rotating the log file.
MAX_SUCCESSFUL_RUNS_FOR_LOG_ROTATE=30
# Directory for storing state files (e.g., last snapshot info).
STATE_DIR="/var/lib/zfs-rclone-stream-backup"
MANIFEST_DIR="${STATE_DIR}/manifests"

# Retention and pruning settings:
# Maximum number of backup sets to keep per dataset.
MAX_BACKUP_SETS_PER_DATASET=3
# Number of days between forced full backups.
FULL_BACKUP_INTERVAL_DAYS=7
# Enable pruning based on total remote backup size.
ENABLE_TOTAL_SIZE_LIMIT_PRUNING=true
# Maximum allowed total remote backup size in bytes.
MAX_TOTAL_REMOTE_SIZE_BYTES="1000000000000" # 1TB example
# If true, assumes the rclone base path is used only for these backups.
ASSUME_RCLONE_BASE_PATH_DEDICATED=true
# Start pruning if remote usage exceeds this percentage of the limit.
PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE=90

# Minimum time (in seconds) that must pass between script runs 
COOLDOWN_PERIOD=10800
# File to track when the script last successfully ran
LAST_RUN_FILE="${STATE_DIR}/last_successful_run_timestamp"

# Manifest and status file settings:
# Enable generation of manifest JSON files for each backup set.
ENABLE_MANIFEST_GENERATION=true
# Enable updating of a MOTD-style status file for system visibility.
ENABLE_MOTD_STATUS_FILE=false
# Path to the MOTD status file.
MOTD_STATUS_FILE_PATH="/var/lib/zfs-backup-status/backup_status.txt"

# Email notification settings (Mailgun):
# Enable or disable email notifications globally.
ENABLE_MAILGUN_NOTIFICATIONS=true
# Control which types of notifications are sent
NOTIFY_ON_SUCCESS=true                 # Send notification when backup completes successfully
NOTIFY_ON_PARTIAL_FAILURE=true         # Send notification when backup completes with partial failures
NOTIFY_ON_CRITICAL_FAILURE=true        # Send notification when backup fails completely
NOTIFY_SUMMARY_DETAIL_LEVEL="standard" # Options: "minimal", "standard", "verbose"
# Mailgun API key (must be set by the user).
MAILGUN_API_KEY="your-mailgun-api-key-here"
# Mailgun domain for sending emails.
MAILGUN_DOMAIN="your-domain.com"
# Base URL for Mailgun API. change to "https://api.mailgun.net/v3" if using in US.
MAILGUN_API_BASE_URL="https://api.eu.mailgun.net/v3"
# Recipient email address for notifications.
MAILGUN_RECIPIENT_EMAIL="your-email@example.com"
# Sender email address for notifications.
MAILGUN_SENDER_EMAIL="notifications@${MAILGUN_DOMAIN}"
# Prefix for email subject lines.
MAILGUN_SUBJECT_PREFIX="[ZFS Backup]"

# --- Command Definitions ---
# These variables define the base commands for external tools used by the script.
readonly ZFS_CMD_BASE="zfs"
readonly RCLONE_CMD_BASE="rclone"
readonly PIGZ_CMD_BASE="pigz"
readonly JQ_CMD_BASE="jq"
readonly CURL_CMD_BASE="curl"

# Arrays of options for various rclone and PIGZ commands.
readonly PIGZ_OPTIONS_ARRAY=("-6")  
readonly RCLONE_GLOBAL_OPTIONS_ARRAY=( "--config" "${RCLONE_CONFIG_PATH}" "--tpslimit" "20" "--tpslimit-burst" "10" "--transfers" "8" "--checkers" "16" "--drive-pacer-min-sleep" "5ms" "--drive-pacer-burst" "10" "--retries" "20" "--low-level-retries" "15" )
readonly RCLONE_RCAT_OPTIONS_ARRAY=("-v" "--stats=1m" "--stats-one-line" "--buffer-size=1024M" "--use-mmap" "--drive-chunk-size=256M" "--retries" "5" "--low-level-retries" "10" "--multi-thread-streams" "4")
readonly RCLONE_MKDIR_OPTIONS_ARRAY=( )
readonly RCLONE_LSJSON_OPTIONS_ARRAY=( "--fast-list" ) 
readonly RCLONE_COPY_OPTIONS_ARRAY=( )
readonly RCLONE_CAT_OPTIONS_ARRAY=( )
readonly RCLONE_SIZE_OPTIONS_ARRAY=( )
# --- End of Configuration ---

# Script version for logging and manifest purposes.
SCRIPT_VERSION="1.0"
# Tracks the current action for status and notifications.
CURRENT_SCRIPT_ACTION="NONE"
# Timestamp when the script started, for duration calculation.
OPERATION_START_TIME=$(date +%s)
# Overall status of the job (SUCCESS, PARTIAL_FAILURE, etc.).
OVERALL_JOB_STATUS="PENDING"
# Accumulates a summary log for notifications and status files.
JOB_SUMMARY_LOG=""
# Associative array for per-dataset status lines.
declare -A DATASET_STATS
# Last calculated total remote backup size.
LAST_CALCULATED_TOTAL_REMOTE_SIZE="N/A"

# Global timestamp for the entire script run, used for snapshot and set IDs.
readonly GLOBAL_SCRIPT_RUN_TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# =============================================================================
# Utility Functions
# =============================================================================

# Internal logging function: always logs to file and stderr, for debugging.
_log_internal() { echo "$(date +'%F %T') - (internal) - $1" | sudo tee -a "$LOG_FILE" >&2; }
# Standard logging function: logs to file and stdout.
log() { echo "$(date +'%F %T') - $1" | sudo tee -a "$LOG_FILE"; }
# Adds a message to the summary log (used for notifications and MOTD).
add_to_summary() { JOB_SUMMARY_LOG+="$(date +'%F %T') - $1\n"; }
# Logs a message and adds it to the summary at the same time.
log_and_summarize() { log "$1"; add_to_summary "$1"; }

# Sends an email notification using Mailgun API.
# Arguments:
#   $1 - Subject of the email
#   $2 - Body of the email (plain text, will also be converted to HTML)
#   $3 - Optional notification type: "success", "partial_failure", "critical_failure" (default: "critical_failure")
send_mailgun_notification() {
    local notification_type="${3:-critical_failure}"
    
    # Check if this notification type should be sent
    if [ "${ENABLE_MAILGUN_NOTIFICATIONS}" != true ]; then
        return 0
    elif [ "${notification_type}" = "success" ] && [ "${NOTIFY_ON_SUCCESS}" != true ]; then
        log "Email notification suppressed (success notification disabled)"
        return 0
    elif [ "${notification_type}" = "partial_failure" ] && [ "${NOTIFY_ON_PARTIAL_FAILURE}" != true ]; then
        log "Email notification suppressed (partial failure notification disabled)"
        return 0
    elif [ "${notification_type}" = "critical_failure" ] && [ "${NOTIFY_ON_CRITICAL_FAILURE}" != true ]; then
        log "Email notification suppressed (critical failure notification disabled)"
        return 0
    fi
    
    local subject="$1"; local body="$2"; local full_subject="${MAILGUN_SUBJECT_PREFIX} ${subject}"
    local mailgun_api_send_url="${MAILGUN_API_BASE_URL}/${MAILGUN_DOMAIN}/messages"
    local log_cmd_mg="log"; if ! command -v log &>/dev/null || [[ "$(type -t log)" != "function" ]]; then log_cmd_mg="echo"; fi
    $log_cmd_mg "Attempting Mailgun notification (Subject: ${full_subject})"
    
    # Adjust email body based on detail level
    local email_body="${body}"
    if [ "${NOTIFY_SUMMARY_DETAIL_LEVEL}" = "minimal" ]; then
        # Extract just the first few lines and a summary of issues
        email_body=$(echo -e "${body}" | grep -E "^[0-9]{4}-[0-9]{2}-[0-9]{2}.*(\[|ERROR|WARNING|CRITICAL|SUCCESS)")
    elif [ "${NOTIFY_SUMMARY_DETAIL_LEVEL}" = "verbose" ]; then
        # Keep the full body for verbose mode
        email_body="${body}"
    fi
    # standard level uses the default body
    
    # Create temporary file for email content - this avoids issues with special characters
    local temp_text_file=$(mktemp)
    
    # Write content to temporary file
    echo -e "${email_body}" > "${temp_text_file}"
    
    # Send email using temporary file - plain text only
    local CURL_RESPONSE; CURL_RESPONSE=$(${CURL_CMD_BASE} --silent --show-error -X POST \
        --user "api:${MAILGUN_API_KEY}" "${mailgun_api_send_url}" \
        -F from="${MAILGUN_SENDER_EMAIL}" \
        -F to="${MAILGUN_RECIPIENT_EMAIL}" \
        -F subject="${full_subject}" \
        -F text=@"${temp_text_file}" 2>&1)
        
    local curl_exit_status=$?
    
    # Clean up temporary file
    rm -f "${temp_text_file}"
    
    if [ ${curl_exit_status} -eq 0 ] && ([[ "${CURL_RESPONSE}" == *"Queued. Thank you."* ]] || [[ "${CURL_RESPONSE}" == *"message.id"* ]]); then 
        $log_cmd_mg "Mailgun notification sent."; 
    else 
        $log_cmd_mg "Error/Warn Mailgun. Code: ${curl_exit_status}. API Resp: ${CURL_RESPONSE}"; 
        add_to_summary "WARNING/ERROR: Mailgun notification issue. Curl Exit: ${curl_exit_status}. API Resp: ${CURL_RESPONSE}"; 
    fi
}

# Updates the MOTD status file with the latest backup status and summary.
# This file can be displayed on login or monitored by other tools.
update_motd_status_file() {
    if [ "${ENABLE_MOTD_STATUS_FILE}" = true ] && [ -n "${MOTD_STATUS_FILE_PATH}" ]; then
        _log_internal "Updating MOTD: ${MOTD_STATUS_FILE_PATH}"; local motd_content=""
        local motd_dir; motd_dir=$(dirname "${MOTD_STATUS_FILE_PATH}")
        # Ensure the MOTD directory exists and is owned by the correct user.
        if [ ! -d "$motd_dir" ]; then if sudo mkdir -p "$motd_dir"; then if [ -n "$SUDO_USER" ] && [ -n "$SUDO_GID" ]; then sudo chown "$SUDO_USER:$SUDO_GID" "$motd_dir"; else sudo chown "$(id -u):$(id -g)" "$motd_dir"; fi; fi; fi
        motd_content+="ZFS Rclone Stream Backup Status (Run Ended: $(date +'%Y-%m-%d %H:%M:%S %Z'))\n"; motd_content+="Overall Status: ${OVERALL_JOB_STATUS}\nScript Action: ${CURRENT_SCRIPT_ACTION}\n"
        # Show total remote backup size and limit.
        local total_remote_size_str="N/A"; if [[ "${LAST_CALCULATED_TOTAL_REMOTE_SIZE}" =~ ^[0-9]+$ ]]; then if [ "${ENABLE_TOTAL_SIZE_LIMIT_PRUNING}" = true ]; then total_remote_size_str="$(numfmt --to=iec-i --suffix=B "${LAST_CALCULATED_TOTAL_REMOTE_SIZE}") / $(numfmt --to=iec-i --suffix=B "${MAX_TOTAL_REMOTE_SIZE_BYTES}") limit"; else total_remote_size_str="$(numfmt --to=iec-i --suffix=B "${LAST_CALCULATED_TOTAL_REMOTE_SIZE}")"; fi; fi
        motd_content+="Total Remote Backup Size: ${total_remote_size_str}\n"; for ds_key in "${!DATASET_STATS[@]}"; do motd_content+="\nDataset: ${ds_key//_//}\n  ${DATASET_STATS[$ds_key]}\n"; done
        motd_content+="\nSummary (last 15 lines):\n$(echo -e "${JOB_SUMMARY_LOG}" | tail -n 15)"; echo -e "${motd_content}" | sudo tee "${MOTD_STATUS_FILE_PATH}" > /dev/null
        if [ $? -eq 0 ]; then _log_internal "MOTD updated."; else _log_internal "Error MOTD update."; add_to_summary "ERROR: MOTD update failed."; fi
    fi
}

# Cleanup function called on script exit.
# Handles notifications, MOTD update, lock release, and sets exit code.
script_exit_cleanup() {
    local exit_status_of_last_command=$? ; local final_job_status_for_subject="${OVERALL_JOB_STATUS}"; local log_cmd_cleanup="log"; if ! command -v log &>/dev/null || [[ "$(type -t log)" != "function" ]]; then log_cmd_cleanup="echo"; fi
    # Determine final status for notification and summary.
    if [[ "$OVERALL_JOB_STATUS" == "PENDING" ]] || ([[ "$OVERALL_JOB_STATUS" == "SUCCESS" ]] && [[ $exit_status_of_last_command -ne 0 && $exit_status_of_last_command -ne 130 && $exit_status_of_last_command -ne 143 && $exit_status_of_last_command -ne 129 ]]); then final_job_status_for_subject="UNEXPECTED_ERROR (Exit: $exit_status_of_last_command)"; add_to_summary "CRITICAL: Script exited unexpectedly. Last cmd exit: $exit_status_of_last_command. Action: ${CURRENT_SCRIPT_ACTION}"; OVERALL_JOB_STATUS="UNEXPECTED_ERROR";
    elif [[ $exit_status_of_last_command -eq 130 && ("$OVERALL_JOB_STATUS" == "PENDING" || "$OVERALL_JOB_STATUS" == "SUCCESS") ]]; then final_job_status_for_subject="INTERRUPTED (SIGINT)"; add_to_summary "INFO: Script interrupted by SIGINT. Action: ${CURRENT_SCRIPT_ACTION}"; OVERALL_JOB_STATUS="INTERRUPTED"; fi
    OPERATION_END_TIME=$(date +%s); local duration=$((OPERATION_END_TIME - OPERATION_START_TIME)); add_to_summary "Total script execution time: ${duration} seconds."
    
    # Send notification and update MOTD if not just initializing.
    if [[ "$CURRENT_SCRIPT_ACTION" != "NONE" ]] && [[ "$CURRENT_SCRIPT_ACTION" != "INIT" ]] ; then
        local notification_type="critical_failure"
        if [[ "$OVERALL_JOB_STATUS" == "SUCCESS" ]] || [[ "$OVERALL_JOB_STATUS" == "VERIFY_SUCCESS" ]]; then
            notification_type="success"
        elif [[ "$OVERALL_JOB_STATUS" == "PARTIAL_FAILURE" ]]; then
            notification_type="partial_failure"
        fi
        
        send_mailgun_notification "Run Finished - ${final_job_status_for_subject}" \
            "ZFS Rclone Stream Backup Script (${CURRENT_SCRIPT_ACTION}) status: ${OVERALL_JOB_STATUS} on $(hostname -f). Summary:\n${JOB_SUMMARY_LOG}\nFull log: ${LOG_FILE}" \
            "${notification_type}"
            
        update_motd_status_file;
    elif [[ "$OVERALL_JOB_STATUS" == "INIT_FAILURE" ]] || [[ "$OVERALL_JOB_STATUS" == "UNEXPECTED_ERROR" && "$CURRENT_SCRIPT_ACTION" == "INIT" ]]; then
        send_mailgun_notification "Script Init FAILED - ${final_job_status_for_subject}" \
            "ZFS Rclone Stream Backup Script failed init on $(hostname -f). Status: ${OVERALL_JOB_STATUS}.\nSummary:\n${JOB_SUMMARY_LOG}\nFull log: ${LOG_FILE}" \
            "critical_failure"
            
        update_motd_status_file;
    fi
    
    # Release lock if held (file descriptor 200).
    if [ -e /proc/$$/fd/200 ]; then (_log_internal "Releasing lock on FD 200 for ${LOCK_FILE}."); flock -u 200; exec 200>&-; else _log_internal "FD 200 not open during cleanup, lock likely already released or exec failed."; fi
    # Set exit code based on job status.
    if [[ "$OVERALL_JOB_STATUS" == "SUCCESS" ]] || [[ "$OVERALL_JOB_STATUS" == "VERIFY_SUCCESS" ]]; then exit 0; elif [[ "$OVERALL_JOB_STATUS" == "PENDING" ]]; then exit 2; else exit 1; fi
}
# Trap signals and errors for cleanup and logging.
trap 'script_exit_cleanup' EXIT
trap 'add_to_summary "ERROR: An unhandled error occurred at line $LINENO (command: $BASH_COMMAND)."; OVERALL_JOB_STATUS="UNEXPECTED_ERROR"; exit 1' ERR
trap 'add_to_summary "INFO: Script interrupted by SIGINT (Ctrl+C)."; OVERALL_JOB_STATUS="INTERRUPTED"; exit 130' SIGINT
trap 'add_to_summary "INFO: Script interrupted by SIGTERM."; OVERALL_JOB_STATUS="INTERRUPTED"; exit 143' SIGTERM
trap 'add_to_summary "INFO: Script interrupted by SIGHUP."; OVERALL_JOB_STATUS="INTERRUPTED"; exit 129' SIGHUP

# --- Helper functions for parsing and file listing ---
# Extracts the snapshot suffix from a filename or snapshot name.
# Returns the suffix if found, or an empty string otherwise.
get_snap_suffix_from_filename() { local filename_or_snap_suffix="$1"; local snap_prefix_pattern="${SNAPSHOT_PREFIX_LOCAL}_[0-9]{14}"; if [[ "$filename_or_snap_suffix" =~ _to_(${snap_prefix_pattern})(\.zfs\.gz)?$ ]]; then echo "${BASH_REMATCH[1]}"; elif [[ "$filename_or_snap_suffix" =~ _snap-(${snap_prefix_pattern})(\.zfs\.gz)?$ ]]; then echo "${BASH_REMATCH[1]}"; elif [[ "$filename_or_snap_suffix" =~ ^(${snap_prefix_pattern})$ ]]; then echo "${BASH_REMATCH[1]}"; else echo ""; fi; }
# Extracts the backup set ID from a filename.
get_set_id_from_filename() { local filename="$1"; local name_part; name_part=$(basename "$filename"); if [[ "$name_part" =~ _set-([0-9]{14})_ ]]; then echo "${BASH_REMATCH[1]}"; else echo ""; fi; }

# --- Manifest Functions ---
# Generates or updates a manifest JSON file for a backup set and uploads it to the remote.
# Arguments:
#   $1 - Dataset safe name (underscored)
#   $2 - Set ID (timestamp)
#   $3 - Backup type ("full" or "incremental")
#   $4 - Filename of the backup file
#   $5 - Size of the backup file in bytes
#   $6 - Snapshot name for this backup
#   $7 - Previous snapshot name (for incrementals)
generate_or_update_manifest() {
    if [ "${ENABLE_MANIFEST_GENERATION}" != true ]; then return 0; fi
    
    local dataset_safe="$1"
    local set_id="$2" 
    local backup_type="$3"
    local filename="$4"
    local size_bytes="$5"
    local snapshot_name="$6"
    local prev_snapshot_name="$7"
    local manifest_filename="manifest_set-${set_id}_dataset-${dataset_safe}.json"
    local remote_manifest_path="${RCLONE_BASE_PATH_ON_REMOTE}/${dataset_safe}/${manifest_filename}"
    local full_remote_manifest="${RCLONE_REMOTE_NAME}:${remote_manifest_path}"
    local temp_manifest_file; temp_manifest_file=$(mktemp)
    local manifest_content
    local timestamp_now; timestamp_now=$(date +"%Y-%m-%d %H:%M:%S")
    
    log "Manifest: Generating/updating manifest for SET_ID ${set_id} dataset ${dataset_safe}."
    
    # Check if manifest exists and download it, or create a new one if not found.
    if ${RCLONE_CMD_BASE} --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" cat "${full_remote_manifest}" > "${temp_manifest_file}" 2>/dev/null; then
        log "Manifest: Existing manifest found and downloaded."
    else
        log "Manifest: Creating new manifest for SET_ID ${set_id}."
        echo '{"set_id":"'${set_id}'","dataset":"'${dataset_safe}'","creation_time":"'${timestamp_now}'","last_updated":"'${timestamp_now}'","files":[]}' > "${temp_manifest_file}"
    fi
    
    # Update the manifest with the new file entry.
    local updated_manifest
    updated_manifest=$(${JQ_CMD_BASE} --arg filename "${filename}" \
        --arg backup_type "${backup_type}" \
        --arg size "${size_bytes}" \
        --arg snapshot "${snapshot_name}" \
        --arg prev_snapshot "${prev_snapshot_name}" \
        --arg timestamp "${timestamp_now}" \
        '.last_updated = $timestamp | .files += [{"filename": $filename, "type": $backup_type, "size": $size, "snapshot": $snapshot, "prev_snapshot": $prev_snapshot, "timestamp": $timestamp}]' "${temp_manifest_file}")
    
    echo "${updated_manifest}" > "${temp_manifest_file}"
    
    # Upload the updated manifest to the remote location.
    if ${RCLONE_CMD_BASE} --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" copyto "${temp_manifest_file}" "${full_remote_manifest}"; then
        log "Manifest: Successfully updated manifest ${manifest_filename}."
    else
        log_and_summarize "ERROR: Failed to upload manifest ${manifest_filename}."
    fi
    
    rm -f "${temp_manifest_file}"
    return 0
}

# --- Pruning Helper Functions ---
# Calculates the total size of all remote backup files, either using rclone size
# (if the base path is dedicated) or by summing individual file sizes.
# Updates LAST_CALCULATED_TOTAL_REMOTE_SIZE and echoes the total size in bytes.
get_current_total_remote_size() {
    local total_size=0; local ASSUME_RCLONE_BASE_PATH_DEDICATED_EFFECTIVE="${ASSUME_RCLONE_BASE_PATH_DEDICATED}"
    local rclone_size_stderr; local size_json_stdout;
    
    if [ "${ASSUME_RCLONE_BASE_PATH_DEDICATED}" = true ]; then
        (_log_internal "PRUNING_UTIL(get_total_size): Using '${RCLONE_CMD_BASE} size' for dedicated path: ${RCLONE_REMOTE_NAME}:${RCLONE_BASE_PATH_ON_REMOTE}")
        local rclone_stderr_tempfile; rclone_stderr_tempfile=$(mktemp)
        
        size_json_stdout=$(${RCLONE_CMD_BASE} size --json "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_SIZE_OPTIONS_ARRAY[@]}" "${RCLONE_REMOTE_NAME}:${RCLONE_BASE_PATH_ON_REMOTE}" 2> "${rclone_stderr_tempfile}")
        local rclone_size_status=$?
        rclone_size_stderr=$(cat "${rclone_stderr_tempfile}")
        rm -f "${rclone_stderr_tempfile}"

        if [ ${rclone_size_status} -eq 0 ] && [ -n "${size_json_stdout}" ] && ${JQ_CMD_BASE} -e '.bytes' <<< "${size_json_stdout}" > /dev/null 2>&1; then
            total_size=$(${JQ_CMD_BASE} -r '.bytes' <<< "${size_json_stdout}")
        else 
            if [[ "${rclone_size_stderr}" == *"directory not found"* ]] || [[ "${size_json_stdout}" == *"directory not found"* ]]; then # Check both
                 (_log_internal "PRUNING_UTIL(get_total_size): '${RCLONE_CMD_BASE} size' reports base directory not found. Size is 0.")
                 total_size=0 
            else
                (_log_internal "PRUNING_UTIL(get_total_size): WARNING: '${RCLONE_CMD_BASE} size' failed (status ${rclone_size_status}, stderr: ${rclone_size_stderr}) or bad JSON (stdout: ${size_json_stdout}). Will sum files.")
                ASSUME_RCLONE_BASE_PATH_DEDICATED_EFFECTIVE=false
            fi
        fi
    fi

    # If not using dedicated path, sum file sizes for each dataset directory.
    if [ "${ASSUME_RCLONE_BASE_PATH_DEDICATED_EFFECTIVE}" = false ]; then
        (_log_internal "PRUNING_UTIL(get_total_size): Summing individual '.zfs.gz' files...")
        total_size=0
        for dataset_sum in "${SOURCE_DATASETS[@]}"; do
            local d_safe_sum=$(echo "${dataset_sum}" | tr '/' '_'); local r_ds_dir_sum="${RCLONE_BASE_PATH_ON_REMOTE}/${d_safe_sum}"; local fr_ds_dir_sum="${RCLONE_REMOTE_NAME}:${r_ds_dir_sum}"
            local rclone_lsjson_stderr; local rclone_lsjson_output; 
            rclone_lsjson_output=$(${RCLONE_CMD_BASE} lsjson "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_LSJSON_OPTIONS_ARRAY[@]}" "${fr_ds_dir_sum}" 2>&1); local rclone_ls_status=$?
            if [ $rclone_ls_status -ne 0 ]; then 
                if [[ "$rclone_lsjson_output" != *"directory not found"* ]]; then 
                    (_log_internal "PRUNING_UTIL(get_total_size): WARNING: rclone lsjson failed for ${fr_ds_dir_sum} (status $rclone_ls_status). Output: $rclone_lsjson_output. Sum may be inaccurate."); 
                else (_log_internal "PRUNING_UTIL(get_total_size): Directory ${fr_ds_dir_sum} not found for summing (normal)."); fi
                continue; 
            fi
            local files_output; files_output=$(${JQ_CMD_BASE} -r '.[] | select(.Name | endswith(".zfs.gz")) | .Size' <<< "${rclone_lsjson_output}" 2>/dev/null)
            if [ -n "${files_output}" ]; then while IFS= read -r size_val; do if [[ "$size_val" =~ ^[0-9]+$ ]]; then total_size=$((total_size + size_val)); fi; done <<< "${files_output}"; fi
        done; (_log_internal "PRUNING_UTIL(get_total_size): Calculated total by sum: ${total_size} B.")
    fi
    LAST_CALCULATED_TOTAL_REMOTE_SIZE=${total_size}; echo "${total_size}" # This is the only stdout
}
# (Other pruning helpers: get_all_remote_sets_globally_sorted, delete_backup_set_files - as in v1.7.4 conceptual, ensure robust rclone calls)

# --- Remote Sets Management and Pruning Functions ---
# Gets a sorted list of all backup sets across all datasets,
# returning them in order from oldest to newest.
# Output format: <SET_ID> <DATASET_SAFE_NAME> <APPROX_SIZE_BYTES>
get_all_remote_sets_globally_sorted() {
    local datasets_to_scan=(); local ds_safe
    local temp_output_file; temp_output_file=$(mktemp)
    
    # Use provided datasets or scan for them if ALL
    for dataset_to_scan in "${SOURCE_DATASETS[@]}"; do
        ds_safe=$(echo "${dataset_to_scan}" | tr '/' '_')
        datasets_to_scan+=("${ds_safe}")
    done
    
    _log_internal "PRUNING_UTIL(get_all_sets): Scanning datasets: ${datasets_to_scan[*]}"
    
    # For each dataset, get all unique set IDs
    for ds_safe in "${datasets_to_scan[@]}"; do
        local r_path="${RCLONE_BASE_PATH_ON_REMOTE}/${ds_safe}"
        local full_r_path="${RCLONE_REMOTE_NAME}:${r_path}"
        
        _log_internal "PRUNING_UTIL(get_all_sets): Checking ${full_r_path}"
        
        # List all files in the dataset's remote directory
        local files_list_json; files_list_json=$(${RCLONE_CMD_BASE} lsjson "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_LSJSON_OPTIONS_ARRAY[@]}" "${full_r_path}" 2>/dev/null)
        local lsjson_status=$?
        if [ $lsjson_status -ne 0 ]; then 
            _log_internal "PRUNING_UTIL(get_all_sets): lsjson failed for ${full_r_path} (status: $lsjson_status)"
            continue
        fi
        
        if [ -z "${files_list_json}" ] || [ "${files_list_json}" = "null" ] || [ "${files_list_json}" = "[]" ]; then
            _log_internal "PRUNING_UTIL(get_all_sets): No files found in ${full_r_path}"
            continue
        fi
        
        # Create temporary file for this dataset's set calculations
        local temp_dataset_file; temp_dataset_file=$(mktemp)
        
        # Process each .zfs.gz file to extract set info
        echo "${files_list_json}" | ${JQ_CMD_BASE} -r '.[] | select(.Name | endswith(".zfs.gz")) | "\(.Name) \(.Size)"' | while read -r filename filesize; do
            if [ -n "${filename}" ] && [[ "${filesize}" =~ ^[0-9]+$ ]]; then
                local set_id; set_id=$(get_set_id_from_filename "${filename}")
                if [ -n "${set_id}" ]; then
                    echo "${set_id} ${filesize}" >> "${temp_dataset_file}"
                    _log_internal "PRUNING_UTIL(get_all_sets): Found set ${set_id} file ${filename} size ${filesize}"
                fi
            fi
        done
        
        # Sum up sizes for each set ID and output results
        if [ -s "${temp_dataset_file}" ]; then
            # Group by set_id and sum sizes
            sort "${temp_dataset_file}" | awk '
            {
                if ($1 in set_sizes) {
                    set_sizes[$1] += $2
                } else {
                    set_sizes[$1] = $2
                }
            }
            END {
                for (set_id in set_sizes) {
                    print set_id " '${ds_safe}' " set_sizes[set_id]
                }
            }' >> "${temp_output_file}"
        fi
        
        rm -f "${temp_dataset_file}"
    done
    
    # Sort all results by set ID (timestamp) and output
    if [ -s "${temp_output_file}" ]; then
        sort -k1 "${temp_output_file}"
        _log_internal "PRUNING_UTIL(get_all_sets): Found $(wc -l < "${temp_output_file}") sets total"
    else
        _log_internal "PRUNING_UTIL(get_all_sets): No sets found across all datasets"
    fi
    
    rm -f "${temp_output_file}"
}

# Deletes all files related to a specific backup set from remote storage.
# Arguments:
#   $1 - Dataset safe name (with underscores)
#   $2 - Set ID (timestamp)
delete_backup_set_files() {
    local dataset_safe="$1"; local set_id="$2"
    if [ -z "${dataset_safe}" ] || [ -z "${set_id}" ]; then
        log_and_summarize "ERROR: delete_backup_set_files called with invalid params: '${dataset_safe}', '${set_id}'."
        return 1
    fi
    
    log "PRUNING: Deleting SET_ID ${set_id} for dataset ${dataset_safe}"
    local remote_path="${RCLONE_BASE_PATH_ON_REMOTE}/${dataset_safe}"
    local full_remote_path="${RCLONE_REMOTE_NAME}:${remote_path}"
    
    # Get list of files for this set
    local files_list; files_list=$(${RCLONE_CMD_BASE} lsf "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${full_remote_path}" 2>/dev/null | grep -E "(_set-${set_id}_)")
    
    if [ -z "${files_list}" ]; then
        log "PRUNING: No files found for SET_ID ${set_id} in ${dataset_safe}."
        return 0
    fi
    
    # Delete each file
    local delete_errors=0
    while IFS= read -r file; do
        if [ -n "${file}" ]; then
            log "PRUNING: Deleting ${remote_path}/${file}"
            if ! ${RCLONE_CMD_BASE} deletefile "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${full_remote_path}/${file}" 2>/dev/null; then
                log "PRUNING ERROR: Failed to delete ${remote_path}/${file}"
                delete_errors=$((delete_errors+1))
            fi
        fi
    done <<< "${files_list}"
    
    if [ ${delete_errors} -gt 0 ]; then
        log_and_summarize "PRUNING WARNING: ${delete_errors} errors while deleting SET_ID ${set_id} files for ${dataset_safe}."
        return 1
    fi
    
    log "PRUNING: Successfully deleted all files for SET_ID ${set_id} from ${dataset_safe}."
    return 0
}

# --- Manifest Functions --- (As in v1.7.4 conceptual)
# ...

# --- Verification Mode Functions ---
# Verifies backup sets - either latest or specific set ID
# Arguments:
#   $1 - Dataset name or "ALL" for all datasets
#   $2 - Set ID to verify or "LATEST" for most recent
run_verification_mode() {
    local dataset_to_verify="$1"
    local set_id_to_verify="$2"
    
    CURRENT_SCRIPT_ACTION="VERIFY"
    log_and_summarize "--- Verification Mode Started ---"
    OVERALL_JOB_STATUS="VERIFY_SUCCESS"
    
    local datasets_to_check=()
    
    # Determine which datasets to verify
    if [ "${dataset_to_verify}" = "ALL" ]; then
        for ds in "${SOURCE_DATASETS[@]}"; do
            datasets_to_check+=("${ds}")
        done
        log_and_summarize "Verifying ${set_id_to_verify} backup set(s) for ALL datasets"
    else
        # Check if the specified dataset is in our source list
        local dataset_found=false
        for ds in "${SOURCE_DATASETS[@]}"; do
            if [ "${ds}" = "${dataset_to_verify}" ]; then
                dataset_found=true
                datasets_to_check+=("${ds}")
                break
            fi
        done
        
        if [ "${dataset_found}" = false ]; then
            log_and_summarize "ERROR: Dataset ${dataset_to_verify} not found in SOURCE_DATASETS"
            OVERALL_JOB_STATUS="VERIFY_FAILURE"
            return 1
        fi
        
        log_and_summarize "Verifying ${set_id_to_verify} backup set(s) for dataset: ${dataset_to_verify}"
    fi
    
    # Process each dataset
    for current_dataset in "${datasets_to_check[@]}"; do
        local dataset_safe_name=$(echo "${current_dataset}" | tr '/' '_')
        local remote_dataset_dir="${RCLONE_BASE_PATH_ON_REMOTE}/${dataset_safe_name}"
        local full_remote_dataset_dir="${RCLONE_REMOTE_NAME}:${remote_dataset_dir}"
        
        log "Verifying dataset: ${current_dataset}"
        
        # Get list of backup files
        local files_json
        files_json=$(${RCLONE_CMD_BASE} lsjson "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_LSJSON_OPTIONS_ARRAY[@]}" "${full_remote_dataset_dir}" 2>/dev/null)
        if [ $? -ne 0 ]; then
            log_and_summarize "ERROR: Cannot list files for ${current_dataset}"
            OVERALL_JOB_STATUS="VERIFY_FAILURE"
            continue
        fi
        
        # Find the specific set ID or latest set
        local set_id_actual="${set_id_to_verify}"
        if [ "${set_id_to_verify}" = "LATEST" ]; then
            set_id_actual=$(echo "${files_json}" | ${JQ_CMD_BASE} -r '.[] | select(.Name | endswith(".zfs.gz")) | .Name' | grep -Eo "set-([0-9]{14})" | sort -r | head -1 | cut -d'-' -f2)
            if [ -z "${set_id_actual}" ]; then
                log_and_summarize "ERROR: No backup sets found for ${current_dataset}"
                OVERALL_JOB_STATUS="VERIFY_FAILURE"
                continue
            fi
            log "Found latest set ID for ${current_dataset}: ${set_id_actual}"
        fi
        
        # Find all files belonging to this set
        local set_files
        set_files=$(echo "${files_json}" | ${JQ_CMD_BASE} -r --arg set "set-${set_id_actual}" '.[] | select(.Name | contains($set)) | .Name')
        if [ -z "${set_files}" ]; then
            log_and_summarize "ERROR: No files found for set ID ${set_id_actual} in ${current_dataset}"
            OVERALL_JOB_STATUS="VERIFY_FAILURE"
            continue
        fi
        
        # Verify each file by checking that rclone can at least read its metadata
        local verification_errors=0
        while IFS= read -r file; do
            log "Verifying file: ${file}"
            if ! ${RCLONE_CMD_BASE} size "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${full_remote_dataset_dir}/${file}" &>/dev/null; then
                log_and_summarize "ERROR: File verification failed for ${file}"
                verification_errors=$((verification_errors+1))
            fi
        done <<< "${set_files}"
        
        if [ ${verification_errors} -gt 0 ]; then
            log_and_summarize "VERIFICATION FAILED: ${verification_errors} errors found in set ${set_id_actual} for ${current_dataset}"
            OVERALL_JOB_STATUS="VERIFY_FAILURE"
        else
            log_and_summarize "VERIFICATION PASSED: All files in set ${set_id_actual} for ${current_dataset} verified successfully"
        fi
    done
    
    log_and_summarize "--- Verification Mode Finished. Overall Status: ${OVERALL_JOB_STATUS} ---"
    return 0
}

# --- Main Backup Function ---
# This function performs the main backup logic for all datasets.
# It handles snapshot creation, backup streaming, manifest generation,
# local snapshot cleanup, and remote pruning.
# Arguments:
#   $1 - main_run_timestamp (must be passed in for consistency)
run_backup_mode() {
    local main_run_timestamp="$1" # CRITICAL: Use this passed parameter
    if [ -z "${main_run_timestamp}" ]; then
        log_and_summarize "CRITICAL FATAL: main_run_timestamp parameter received by run_backup_mode is empty! Aborting backup operations."
        OVERALL_JOB_STATUS="INIT_FAILURE"; return 1;
    fi
    log "DEBUG: run_backup_mode started with main_run_timestamp: ${main_run_timestamp}"

    CURRENT_SCRIPT_ACTION="BACKUP"; log_and_summarize "--- Backup Mode Started ---"; OVERALL_JOB_STATUS="SUCCESS" 
    
    # Step 0: Proactive Pruning
    # If enabled, check if remote usage is above the configured threshold and prune oldest sets if needed.
    if [ "${ENABLE_TOTAL_SIZE_LIMIT_PRUNING}" = true ] && [ "${PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE}" -gt 0 ] && [ "${PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE}" -le 100 ]; then
        local original_action_context_proactive="${CURRENT_SCRIPT_ACTION}"; CURRENT_SCRIPT_ACTION="PROACTIVE_PRUNING"
        log_and_summarize "PROACTIVE_PRUNING: Checking remote size."
        local current_total_size_pro; current_total_size_pro=$(get_current_total_remote_size)
        local threshold_size_pro=$((MAX_TOTAL_REMOTE_SIZE_BYTES * PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE / 100))
        if [ -n "${current_total_size_pro}" ] && [[ "${current_total_size_pro}" =~ ^[0-9]+$ ]] && [ "${current_total_size_pro}" -gt "${threshold_size_pro}" ]; then
            log_and_summarize "PROACTIVE_PRUNING: Current size ${current_total_size_pro} B > threshold ${threshold_size_pro} B. Will prune."
            local proactive_prune_attempts=0; local MAX_PROACTIVE_PRUNE_ATTEMPTS=5 
            local proactive_target_size=$((MAX_TOTAL_REMOTE_SIZE_BYTES * (PRUNE_IF_TOTAL_SIZE_ABOVE_PERCENTAGE - 5) / 100 )); if [ $proactive_target_size -lt 0 ]; then proactive_target_size=0; fi
            while [ "${current_total_size_pro}" -gt "${proactive_target_size}" ] && [ ${proactive_prune_attempts} -lt ${MAX_PROACTIVE_PRUNE_ATTEMPTS} ]; do
                mapfile -t globally_sorted_sets_pro < <(get_all_remote_sets_globally_sorted)
                if [ ${#globally_sorted_sets_pro[@]} -eq 0 ]; then log_and_summarize "PROACTIVE_PRUNING: No sets to prune further."; break; fi
                local oldest_set_info_pro; read -r oldest_set_id_pro oldest_ds_safe_pro oldest_set_size_pro_approx <<< "${globally_sorted_sets_pro[0]}"
                if [ -z "${oldest_set_id_pro}" ]; then log_and_summarize "PROACTIVE_PRUNING: Could not determine oldest set."; break; fi
                log_and_summarize "PROACTIVE_PRUNING: Deleting oldest global SET_ID ${oldest_set_id_pro} from ${oldest_ds_safe_pro} (size ~${oldest_set_size_pro_approx} B)."
                delete_backup_set_files "${oldest_ds_safe_pro}" "${oldest_set_id_pro}" || { OVERALL_JOB_STATUS="PARTIAL_FAILURE"; add_to_summary "ERROR: Proactive deletion of set ${oldest_set_id_pro} failed."; }
                current_total_size_pro=$(get_current_total_remote_size); proactive_prune_attempts=$((proactive_prune_attempts + 1))
            done
            if [ "${current_total_size_pro}" -gt "${threshold_size_pro}" ]; then log_and_summarize "PROACTIVE_PRUNING: Warning: Size still near/over threshold after ${proactive_prune_attempts} attempts."; fi
        elif [ -n "${current_total_size_pro}" ] && [[ "${current_total_size_pro}" =~ ^[0-9]+$ ]]; then log "PROACTIVE_PRUNING: Total size (${current_total_size_pro} B) below proactive threshold (${threshold_size_pro} B)."; 
        else log_and_summarize "PROACTIVE_PRUNING: Could not determine current total size accurately for proactive check. Skipping proactive prune.";fi
        CURRENT_SCRIPT_ACTION="BACKUP" 
    fi
    
    log_and_summarize "--- Step 1: Processing Source Datasets for Backup ---"; local any_dataset_backup_failed_in_step1=false
    for dataset in "${SOURCE_DATASETS[@]}"; do
        dataset_processed_successfully_this_run=false
        log "DEBUG: Dataset loop - main_run_timestamp is '${main_run_timestamp}' for dataset ${dataset}" # Added Debug
        if [ -z "${main_run_timestamp}" ]; then log_and_summarize "CRITICAL FATAL: main_run_timestamp became empty for ${dataset}! Aborting dataset."; any_dataset_backup_failed_in_step1=true; continue; fi
        
        log_and_summarize "BACKUP: Starting dataset ${dataset}"; DATASET_SAFE_NAME=$(echo "${dataset}" | tr '/' '_'); LAST_SUCCESSFUL_SNAP_STATE_FILE="${STATE_DIR}/${DATASET_SAFE_NAME}.last_snap_suffix"; CURRENT_SET_ID_STATE_FILE="${STATE_DIR}/${DATASET_SAFE_NAME}.current_set_id"; LAST_FULL_BACKUP_TS_STATE_FILE="${STATE_DIR}/${DATASET_SAFE_NAME}.last_full_backup_timestamp";
        # Check if the dataset exists before proceeding.
        if ! ${ZFS_CMD_BASE} list -H -o name "${dataset}" > /dev/null 2>&1; then log_and_summarize "ERROR: Source dataset ${dataset} does not exist. Skipping."; any_dataset_backup_failed_in_step1=true; continue; fi
        # Create a new ZFS snapshot for this backup run.
        NEW_SNAPSHOT_NAME_SUFFIX="${SNAPSHOT_PREFIX_LOCAL}_${main_run_timestamp}"; NEW_SNAPSHOT_FULL_NAME="${dataset}@${NEW_SNAPSHOT_NAME_SUFFIX}";
        log "BACKUP: Creating source snapshot: ${NEW_SNAPSHOT_FULL_NAME}"; if ! ${ZFS_CMD_BASE} snapshot "${NEW_SNAPSHOT_FULL_NAME}"; then log_and_summarize "ERROR: Failed to create snapshot ${NEW_SNAPSHOT_FULL_NAME} for ${dataset}."; any_dataset_backup_failed_in_step1=true; continue; fi; log "BACKUP: Successfully created snapshot ${NEW_SNAPSHOT_FULL_NAME}."
        current_set_id_for_this_backup=""; last_successful_snap_suffix_for_inc=""; is_incremental_flag=0; force_new_full_this_run_flag=0; zfs_send_args_part_final=""; remote_filename_final=""; backup_type_for_manifest_final=""; current_set_id_from_state=$(cat "${CURRENT_SET_ID_STATE_FILE}" 2>/dev/null); last_full_backup_ts_from_state=$(cat "${LAST_FULL_BACKUP_TS_STATE_FILE}" 2>/dev/null)

        # Determine if a full or incremental backup is needed based on interval and state.
        if [ "${FULL_BACKUP_INTERVAL_DAYS}" -gt 0 ]; then if [ -n "${last_full_backup_ts_from_state}" ]; then last_full_date_seconds=$(date -d "${last_full_backup_ts_from_state:0:4}-${last_full_backup_ts_from_state:4:2}-${last_full_backup_ts_from_state:6:2} ${last_full_backup_ts_from_state:8:2}:${last_full_backup_ts_from_state:10:2}:${last_full_backup_ts_from_state:12:2}" +%s 2>/dev/null || echo 0); current_date_seconds=$(date +%s); if [ "${last_full_date_seconds}" -ne 0 ]; then days_since_last_full=$(((current_date_seconds - last_full_date_seconds) / 86400)); log "BACKUP: Days since last full for ${dataset}: ${days_since_last_full} (Interval: ${FULL_BACKUP_INTERVAL_DAYS} days)"; DATASET_STATS["${DATASET_SAFE_NAME}"]="Days since last full: ${days_since_last_full}"; if [ "${days_since_last_full}" -ge "${FULL_BACKUP_INTERVAL_DAYS}" ]; then log_and_summarize "BACKUP: Forcing new full for ${dataset} (interval)."; force_new_full_this_run_flag=1; fi; else log_and_summarize "WARNING: Could not parse last full ts '${last_full_backup_ts_from_state}'. Forcing full."; force_new_full_this_run_flag=1; fi; else log_and_summarize "BACKUP: No record of last full. Forcing full."; force_new_full_this_run_flag=1; DATASET_STATS["${DATASET_SAFE_NAME}"]="No previous full backup record."; fi; fi
        last_successful_snap_suffix_for_inc=$(cat "${LAST_SUCCESSFUL_SNAP_STATE_FILE}" 2>/dev/null); if ! [[ "${last_successful_snap_suffix_for_inc}" =~ ^${SNAPSHOT_PREFIX_LOCAL}_[0-9]{14}$ ]]; then if [ -n "${last_successful_snap_suffix_for_inc}" ]; then log_and_summarize "WARNING: Invalid last_snap_suffix '${last_successful_snap_suffix_for_inc}' for ${dataset}. Forcing full."; fi; last_successful_snap_suffix_for_inc=""; fi
        if [ ${force_new_full_this_run_flag} -eq 1 ] || [ -z "${current_set_id_from_state}" ] || [ -z "${last_successful_snap_suffix_for_inc}" ]; then is_incremental_flag=0; else if ${ZFS_CMD_BASE} list -H -o name "${dataset}@${last_successful_snap_suffix_for_inc}" > /dev/null 2>&1; then is_incremental_flag=1; current_set_id_for_this_backup="${current_set_id_from_state}"; else log_and_summarize "WARNING: Base snap '${last_successful_snap_suffix_for_inc}' for inc not found locally for ${dataset}. Forcing full."; is_incremental_flag=0; fi; fi

        # Set up the ZFS send command and remote filename based on backup type.
        if [ ${is_incremental_flag} -eq 0 ]; then current_set_id_for_this_backup="${main_run_timestamp}"; log_and_summarize "BACKUP: Performing FULL for ${dataset}. New SET_ID: ${current_set_id_for_this_backup}"; zfs_send_args_part_final="\"${NEW_SNAPSHOT_FULL_NAME}\""; remote_filename_final="full_${DATASET_SAFE_NAME}_set-${current_set_id_for_this_backup}_snap-${NEW_SNAPSHOT_NAME_SUFFIX}.zfs.gz"; backup_type_for_manifest_final="full"; last_successful_snap_suffix_for_inc=""; else log_and_summarize "BACKUP: Performing INCREMENTAL for ${dataset}. SET_ID: ${current_set_id_for_this_backup}. From: ${last_successful_snap_suffix_for_inc}"; zfs_send_args_part_final="-I \"${dataset}@${last_successful_snap_suffix_for_inc}\" \"${NEW_SNAPSHOT_FULL_NAME}\""; remote_filename_final="inc_${DATASET_SAFE_NAME}_set-${current_set_id_for_this_backup}_from-${last_successful_snap_suffix_for_inc}_to-${NEW_SNAPSHOT_NAME_SUFFIX}.zfs.gz"; backup_type_for_manifest_final="incremental"; fi
        FINAL_ZFS_SEND_COMMAND="${ZFS_CMD_BASE} send ${zfs_send_args_part_final}"; REMOTE_DATASET_DIR="${RCLONE_BASE_PATH_ON_REMOTE}/${DATASET_SAFE_NAME}"; REMOTE_TARGET_FILE="${REMOTE_DATASET_DIR}/${remote_filename_final}"; FULL_RCLONE_TARGET_FILE="${RCLONE_REMOTE_NAME}:${REMOTE_TARGET_FILE}"; FULL_RCLONE_DATASET_DIR="${RCLONE_REMOTE_NAME}:${REMOTE_DATASET_DIR}"; log "BACKUP: Target: ${FULL_RCLONE_TARGET_FILE}"; log "BACKUP: ZFS send args: ${zfs_send_args_part_final}"; local rclone_mkdir_status=0; local rclone_mkdir_stderr; rclone_mkdir_stderr=$(${RCLONE_CMD_BASE} mkdir --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_MKDIR_OPTIONS_ARRAY[@]}" "${FULL_RCLONE_DATASET_DIR}" 2>&1); rclone_mkdir_status=$?; if [ $rclone_mkdir_status -ne 0 ]; then log "Warning: rclone mkdir ${FULL_RCLONE_DATASET_DIR} failed (status $rclone_mkdir_status). Stderr: $rclone_mkdir_stderr"; fi
        set -o pipefail; log "BACKUP: Executing pipeline for ${dataset}..."; local rclone_rcat_stderr_capture; rclone_rcat_stderr_capture=$(mktemp); local rclone_rcat_stdout_capture; rclone_rcat_stdout_capture=$(mktemp) # Capture stdout of rcat too for its logs
        eval "${FINAL_ZFS_SEND_COMMAND}" | ${PIGZ_CMD_BASE} "${PIGZ_OPTIONS_ARRAY[@]}" | ${RCLONE_CMD_BASE} rcat --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_RCAT_OPTIONS_ARRAY[@]}" "${FULL_RCLONE_TARGET_FILE}" >"${rclone_rcat_stdout_capture}" 2>"${rclone_rcat_stderr_capture}"; local pipe_statuses_rcat=("${PIPESTATUS[@]}"); local rclone_rcat_final_status=${pipe_statuses_rcat[2]}; local rcat_stdout_content=$(cat "${rclone_rcat_stdout_capture}"); local rcat_stderr_content=$(cat "${rclone_rcat_stderr_capture}"); rm -f "${rclone_rcat_stdout_capture}" "${rclone_rcat_stderr_capture}";
        if [ -n "${rcat_stdout_content}" ]; then log "Rclone rcat STDOUT: ${rcat_stdout_content}"; fi # Log rclone's own stdout stats
        if [ ${rclone_rcat_final_status} -eq 0 ]; then set +o pipefail; log_and_summarize "BACKUP: Uploaded stream for ${NEW_SNAPSHOT_FULL_NAME} to ${FULL_RCLONE_TARGET_FILE}"; dataset_processed_successfully_this_run=true; uploaded_size_bytes=0; local size_json_output_stderr; local size_json_output; size_json_output=$(${RCLONE_CMD_BASE} size --json --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_SIZE_OPTIONS_ARRAY[@]}" "${FULL_RCLONE_TARGET_FILE}" 2>&1); local rclone_size_status=$?; if [ $rclone_size_status -eq 0 ] && [ -n "${size_json_output}" ] && [[ $(${JQ_CMD_BASE} -r '.bytes // "-1"' <<< "${size_json_output}") =~ ^[0-9]+$ ]]; then uploaded_size_bytes=$(${JQ_CMD_BASE} -r '.bytes' <<< "${size_json_output}"); log "BACKUP: Size of ${remote_filename_final} is ${uploaded_size_bytes} bytes."; else log_and_summarize "WARNING: Could not get size of ${remote_filename_final} (rclone size status $rclone_size_status, output: $size_json_output). Manifest size 0."; fi
            if echo "${NEW_SNAPSHOT_NAME_SUFFIX}" > "${LAST_SUCCESSFUL_SNAP_STATE_FILE}"; then log "BACKUP: Updated .last_snap_suffix for ${dataset} to: ${NEW_SNAPSHOT_NAME_SUFFIX}"; else log_and_summarize "ERROR: Failed write .last_snap_suffix for ${dataset}."; any_dataset_backup_failed_in_step1=true; fi
            if [ ${is_incremental_flag} -eq 0 ]; then if echo "${current_set_id_for_this_backup}" > "${CURRENT_SET_ID_STATE_FILE}"; then log "BACKUP: Updated .current_set_id for ${dataset} to: ${current_set_id_for_this_backup}"; else log_and_summarize "ERROR: Failed write .current_set_id for ${dataset}."; any_dataset_backup_failed_in_step1=true; fi; if echo "${current_set_id_for_this_backup}" > "${LAST_FULL_BACKUP_TS_STATE_FILE}"; then log "BACKUP: Updated .last_full_backup_timestamp for ${dataset} to: ${current_set_id_for_this_backup}"; else log_and_summarize "ERROR: Failed write .last_full_backup_timestamp for ${dataset}."; any_dataset_backup_failed_in_step1=true; fi; fi
            generate_or_update_manifest "${DATASET_SAFE_NAME}" "${current_set_id_for_this_backup}" "${backup_type_for_manifest_final}" "${remote_filename_final}" "${uploaded_size_bytes}" "${NEW_SNAPSHOT_NAME_SUFFIX}" "${last_successful_snap_suffix_for_inc}"
        else set +o pipefail; log_and_summarize "CRITICAL ERROR: Pipeline failed for ${NEW_SNAPSHOT_FULL_NAME} of ${dataset}. Codes(zfs|pigz|rclone): ${pipe_statuses_rcat[0]}|${pipe_statuses_rcat[1]}|${pipe_statuses_rcat[2]}. Rclone stderr: ${rcat_stderr_content}"; any_dataset_backup_failed_in_step1=true; if [ "${pipe_statuses_rcat[2]}" = "130" ]; then log "INFO: Rclone rcat interrupted."; fi; fi
        local ds_stat_line="Last snap: ${NEW_SNAPSHOT_NAME_SUFFIX} (${backup_type_for_manifest_final}, SET: ${current_set_id_for_this_backup})"; if ! $dataset_processed_successfully_this_run; then ds_stat_line+=" - FAILED THIS RUN"; fi; DATASET_STATS["${DATASET_SAFE_NAME}"]="${ds_stat_line}"
    done
    if [ "$any_dataset_backup_failed_in_step1" = true ] && [ "$OVERALL_JOB_STATUS" != "INIT_FAILURE" ]; then OVERALL_JOB_STATUS="PARTIAL_FAILURE"; fi

    # Step 2: Local ZFS snapshot cleanup.
    # Only keep the most recent snapshot for each dataset.
    CURRENT_SCRIPT_ACTION="LOCAL_CLEANUP"; log_and_summarize "--- Step 2: Cleaning Up Old Local ZFS Snapshots ---" # (Implementation as in v1.7.3)
    if [ "$OVERALL_JOB_STATUS" != "INIT_FAILURE" ]; then for dataset in "${SOURCE_DATASETS[@]}"; do DATASET_SAFE_NAME=$(echo "${dataset}" | tr '/' '_'); STATE_FILE="${STATE_DIR}/${DATASET_SAFE_NAME}.last_snap_suffix"; CURRENT_KEPT_LOCAL_SNAP_SUFFIX=$(cat "${STATE_FILE}" 2>/dev/null); if [ -z "${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}" ] || ! [[ "${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}" =~ ^${SNAPSHOT_PREFIX_LOCAL}_[0-9]{14}$ ]]; then log "LocalCleanup: Invalid or empty state for ${dataset} ('${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}'), skipping."; continue; fi; log "LocalCleanup: For ${dataset}, keeping local snap suffix '${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}'."; SNAPSHOTS_TO_DELETE_ON_SOURCE=$(${ZFS_CMD_BASE} list -H -o name -t snapshot -d 1 "${dataset}" | grep "@${SNAPSHOT_PREFIX_LOCAL}_" | grep -v "@${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}$"); if [ -n "${SNAPSHOTS_TO_DELETE_ON_SOURCE}" ]; then while IFS= read -r snap_full_name_to_delete; do if [[ "${snap_full_name_to_delete}" == "${dataset}@${CURRENT_KEPT_LOCAL_SNAP_SUFFIX}" ]]; then continue; fi; log "LocalCleanup: Destroying old local snapshot ${snap_full_name_to_delete}"; if ! ${ZFS_CMD_BASE} destroy "${snap_full_name_to_delete}"; then log_and_summarize "Warning: Failed to destroy ${snap_full_name_to_delete}."; fi; done <<< "${SNAPSHOTS_TO_DELETE_ON_SOURCE}"; else log "LocalCleanup: No old local snapshots to delete for ${dataset}."; fi; done; fi

    # Step 3: Per-dataset remote pruning based on set count.
    CURRENT_SCRIPT_ACTION="REMOTE_PRUNING"; if [ "$OVERALL_JOB_STATUS" != "INIT_FAILURE" ]; then
        log_and_summarize "--- Step 3: Per-Dataset Set-Count-Based Remote Cleanup ---" # (Implementation as in v1.7.3)
        for dataset_prune in "${SOURCE_DATASETS[@]}"; do local dataset_safe_prune=$(echo "${dataset_prune}" | tr '/' '_'); local remote_dataset_dir_prune="${RCLONE_BASE_PATH_ON_REMOTE}/${dataset_safe_prune}"; local full_rclone_dataset_dir_prune="${RCLONE_REMOTE_NAME}:${remote_dataset_dir_prune}"; log "SetCountPrune: Checking dataset ${dataset_safe_prune}"; local rclone_lsf_manifests_out_stderr; local rclone_lsf_manifests_out; rclone_lsf_manifests_out=$(${RCLONE_CMD_BASE} lsf --config "${RCLONE_CONFIG_PATH}" "${RCLONE_GLOBAL_OPTIONS_ARRAY[@]}" "${RCLONE_LSJSON_OPTIONS_ARRAY[@]}" "${full_rclone_dataset_dir_prune}" 2>&1); local rclone_lsf_status=$?; if [ $rclone_lsf_status -ne 0 ]; then if [[ "$rclone_lsf_manifests_out" != *"directory not found"* ]]; then log_and_summarize "SetCountPrune ERROR: rclone lsf failed for ${full_rclone_dataset_dir_prune} (status $rclone_lsf_status). Output: $rclone_lsf_manifests_out"; OVERALL_JOB_STATUS="PARTIAL_FAILURE";fi; DATASET_STATS["${dataset_safe_prune}"]+=", Remote Sets: ErrorListing"; continue; fi; mapfile -t current_set_ids < <(echo "${rclone_lsf_manifests_out}" | grep -Eo "manifest_set-([0-9]{14})_dataset-${dataset_safe_prune}\.json$" | sed -n 's/^manifest_set-\([0-9]{14\}\)_dataset-.*/\1/p' | sort -un); local num_sets=${#current_set_ids[@]}; log "SetCountPrune: Found ${num_sets} unique SET_IDs for ${dataset_safe_prune}. Max allowed: ${MAX_BACKUP_SETS_PER_DATASET}."; if [[ -n "${DATASET_STATS[${dataset_safe_prune}]}" ]]; then DATASET_STATS["${dataset_safe_prune}"]+=", Remote Sets: ${num_sets}"; else DATASET_STATS["${dataset_safe_prune}"]="Remote Sets: ${num_sets}"; fi; local num_sets_to_delete=$((num_sets - MAX_BACKUP_SETS_PER_DATASET)); if [ ${num_sets_to_delete} -gt 0 ]; then log_and_summarize "SetCountPrune: For ${dataset_safe_prune}, deleting ${num_sets_to_delete} oldest set(s)."; for i in $(seq 0 $((num_sets_to_delete - 1))); do local set_id_to_nuke="${current_set_ids[$i]}"; delete_backup_set_files "${dataset_safe_prune}" "${set_id_to_nuke}" || { OVERALL_JOB_STATUS="PARTIAL_FAILURE"; add_to_summary "ERROR: SetCountPrune: Failed to delete set ${set_id_to_nuke} for ${dataset_safe_prune}."; }; done; else log "SetCountPrune: Dataset ${dataset_safe_prune} within set count limit."; fi; done        # Step 4: Global remote pruning based on total size.
        log_and_summarize "--- Step 4: Global Total Size-Based Remote Cleanup ---"
        
        if [ "${ENABLE_TOTAL_SIZE_LIMIT_PRUNING}" = true ]; then
            log "GlobalSizePrune: Enabled. Max total: ${MAX_TOTAL_REMOTE_SIZE_BYTES} B."
            
            local current_total_size_for_global_prune
            current_total_size_for_global_prune=$(get_current_total_remote_size)
            local attempts_to_get_under_size_limit=0
            local MAX_GLOBAL_SIZE_PRUNE_ATTEMPTS=20
            
            while [ -n "${current_total_size_for_global_prune}" ] && \
                  [[ "${current_total_size_for_global_prune}" =~ ^[0-9]+$ ]] && \
                  [ "${current_total_size_for_global_prune}" -gt "${MAX_TOTAL_REMOTE_SIZE_BYTES}" ] && \
                  [ ${attempts_to_get_under_size_limit} -lt ${MAX_GLOBAL_SIZE_PRUNE_ATTEMPTS} ]; do
                
                log_and_summarize "GlobalSizePrune: Current ${current_total_size_for_global_prune} B > limit ${MAX_TOTAL_REMOTE_SIZE_BYTES} B. Pruning."
                
                # Get all sets sorted by age
                mapfile -t globally_sorted_sets < <(get_all_remote_sets_globally_sorted)
                
                # Debug: show what sets were found
                _log_internal "GlobalSizePrune: Found ${#globally_sorted_sets[@]} sets across all datasets"
                for debug_set in "${globally_sorted_sets[@]}"; do
                    _log_internal "GlobalSizePrune: Available set: ${debug_set}"
                done
                
                if [ ${#globally_sorted_sets[@]} -eq 0 ]; then
                    log_and_summarize "GlobalSizePrune WARNING: No sets to prune!"
                    OVERALL_JOB_STATUS="PARTIAL_FAILURE"
                    break
                fi
                
                # Get the oldest set
                local oldest_set_info="${globally_sorted_sets[0]}"
                local oldest_set_id_global oldest_ds_safe_global oldest_set_size_approx_unused
                read -r oldest_set_id_global oldest_ds_safe_global oldest_set_size_approx_unused <<< "${oldest_set_info}"
                
                if [ -z "${oldest_set_id_global}" ]; then
                    log_and_summarize "GlobalSizePrune WARNING: Could not determine oldest set from: '${oldest_set_info}'"
                    OVERALL_JOB_STATUS="PARTIAL_FAILURE"
                    break
                fi
                
                log_and_summarize "GlobalSizePrune: Globally oldest SET_ID is ${oldest_set_id_global} from ${oldest_ds_safe_global} (set size ~${oldest_set_size_approx_unused} B)."
                
                # Delete the oldest set
                if delete_backup_set_files "${oldest_ds_safe_global}" "${oldest_set_id_global}"; then
                    log "GlobalSizePrune: Successfully deleted set ${oldest_set_id_global}, recalculating total size..."
                    current_total_size_for_global_prune=$(get_current_total_remote_size)
                    log "GlobalSizePrune: New total size: ${current_total_size_for_global_prune} B"
                else
                    log_and_summarize "GlobalSizePrune ERROR: Failed to delete set. Aborting size pruning."
                    OVERALL_JOB_STATUS="PARTIAL_FAILURE"
                    break
                fi
                
                attempts_to_get_under_size_limit=$((attempts_to_get_under_size_limit + 1))
            done
            
            # Check final status
            if [ ${attempts_to_get_under_size_limit} -ge ${MAX_GLOBAL_SIZE_PRUNE_ATTEMPTS} ] && \
               [ "${current_total_size_for_global_prune}" -gt "${MAX_TOTAL_REMOTE_SIZE_BYTES}" ]; then
                log_and_summarize "GlobalSizePrune WARNING: Hit max attempts (${MAX_GLOBAL_SIZE_PRUNE_ATTEMPTS}) but still over limit."
                OVERALL_JOB_STATUS="PARTIAL_FAILURE"
            fi
            
            if [[ "${current_total_size_for_global_prune}" =~ ^[0-9]+$ ]] && \
               [ "${current_total_size_for_global_prune}" -le "${MAX_TOTAL_REMOTE_SIZE_BYTES}" ]; then
                log "GlobalSizePrune: Total size (${current_total_size_for_global_prune} B) now within limit."
            fi
        else
            log "GlobalSizePrune: Disabled."
            if [ "${ASSUME_RCLONE_BASE_PATH_DEDICATED}" = true ]; then
                get_current_total_remote_size >/dev/null
            fi
        fi
    fi
    if [[ "$OVERALL_JOB_STATUS" == "PENDING" ]]; then OVERALL_JOB_STATUS="SUCCESS"; fi

    # Update the timestamp if backup was successful
    if [[ "$OVERALL_JOB_STATUS" == "SUCCESS" ]]; then
        update_last_run_timestamp
    fi

    log_and_summarize "--- Backup Mode Finished. Overall Status: ${OVERALL_JOB_STATUS} ---"
}

# Function to check if the cooldown period is active
check_cooldown_period() {
    # Skip check if STATE_DIR doesn't exist yet (it will be created later)
    if [ ! -d "${STATE_DIR}" ]; then
        log "Cooldown check: STATE_DIR doesn't exist yet, skipping cooldown check"
        return 0
    fi

    # Check if the last run timestamp file exists
    if [ -f "${LAST_RUN_FILE}" ]; then
        last_run_time=$(cat "${LAST_RUN_FILE}" 2>/dev/null || echo 0)
        current_time=$(date +%s)
        
        # Validate the timestamp (should be a number)
        if [[ "${last_run_time}" =~ ^[0-9]+$ ]]; then
            time_diff=$((current_time - last_run_time))
            
            if [ ${time_diff} -lt ${COOLDOWN_PERIOD} ]; then
                # Cooldown period still active
                wait_time=$((COOLDOWN_PERIOD - time_diff))
                minutes_to_wait=$((wait_time / 60))
                log_and_summarize "COOLDOWN ACTIVE: Previous run completed ${time_diff} seconds ago ($(date -d @${last_run_time}))"
                log_and_summarize "COOLDOWN ACTIVE: Minimum cooldown is ${COOLDOWN_PERIOD} seconds ($(($COOLDOWN_PERIOD/60/60)) hours)"
                log_and_summarize "COOLDOWN ACTIVE: Exiting - next eligible run time: $(date -d @$((last_run_time + COOLDOWN_PERIOD)))"
                
                # Send notification about skipped run
                if [ "${ENABLE_MAILGUN_NOTIFICATIONS}" = true ]; then
                    send_mailgun_notification "Run Skipped - Cooldown Period Active" \
                    "ZFS backup skipped because cooldown period is active.\n\nPrevious run: $(date -d @${last_run_time})\nNext eligible run: $(date -d @$((last_run_time + COOLDOWN_PERIOD)))\nWait time: ${minutes_to_wait} minutes" \
                    "info"
                fi
                
                OVERALL_JOB_STATUS="SKIPPED_COOLDOWN"
                exit 0
            else
                log "Cooldown check: Last run was ${time_diff} seconds ago, exceeds cooldown period of ${COOLDOWN_PERIOD} seconds"
            fi
        else
            log "Cooldown check: Invalid last run timestamp, proceeding with backup"
        fi
    else
        log "Cooldown check: No record of previous successful run, proceeding with backup"
    fi
}

# Function to update the last run timestamp after a successful backup
update_last_run_timestamp() {
    if [ ! -d "${STATE_DIR}" ]; then
        mkdir -p "${STATE_DIR}" 2>/dev/null || sudo mkdir -p "${STATE_DIR}" 2>/dev/null
    fi
    
    date +%s > "${LAST_RUN_FILE}"
    log "Updated last successful run timestamp for cooldown tracking"
}

# =============================================================================
# Main Script Execution (Dispatcher)
# =============================================================================
# The main dispatcher parses arguments and calls the appropriate mode.
# Supported modes:
#   --backup (default): Run backup for all datasets.
#   --verify-latest-set <dataset|ALL>: Verify the latest backup set for a dataset or all datasets.
#   --verify-set <dataset|ALL> <SET_ID>: Verify a specific backup set.
#   --verify-all-latest-sets: Verify the latest set for all datasets.

CURRENT_SCRIPT_ACTION="NONE" # Will be set by main_dispatcher_logic

main_dispatcher_logic() {
    # Check if we need to respect cooldown period first
    check_cooldown_period
    local action_internal="BACKUP"
    local verify_dataset_filter_internal="ALL"; local verify_set_id_filter_internal="LATEST"

    # Parse command line arguments
    if [ "$#" -gt 0 ]; then 
        case "$1" in
            --verify-latest-set) action_internal="VERIFY"; if [ -n "$2" ]; then verify_dataset_filter_internal="$2"; else log_and_summarize "ERROR: --verify-latest-set <dataset|ALL>."; OVERALL_JOB_STATUS="INIT_FAILURE"; return 1; fi; verify_set_id_filter_internal="LATEST";;
            --verify-set) action_internal="VERIFY"; if [ -n "$2" ] && [ -n "$3" ]; then verify_dataset_filter_internal="$2"; verify_set_id_filter_internal="$3"; else log_and_summarize "ERROR: --verify-set <dataset|ALL> <SET_ID>."; OVERALL_JOB_STATUS="INIT_FAILURE"; return 1; fi;;
            --verify-all-latest-sets) action_internal="VERIFY"; verify_dataset_filter_internal="ALL"; verify_set_id_filter_internal="LATEST";;
            --backup) action_internal="BACKUP";;
            *) log_and_summarize "ERROR: Unknown option $1. Use --help (not implemented) or see script header."; OVERALL_JOB_STATUS="INIT_FAILURE"; return 1;;
        esac
    fi
    CURRENT_SCRIPT_ACTION="${action_internal}" 

    # Ensure the global run timestamp is set before proceeding
    if [ -z "${GLOBAL_SCRIPT_RUN_TIMESTAMP}" ]; then
        log_and_summarize "CRITICAL FATAL: GLOBAL_SCRIPT_RUN_TIMESTAMP is not set when dispatching action! This indicates a severe script error."
        OVERALL_JOB_STATUS="INIT_FAILURE"; return 1;
    fi

    # Ensure state and manifest directories exist with proper permissions
    log_and_summarize "Creating required directories if they don't exist"
    
    # First create the base state directory
    if [ ! -d "${STATE_DIR}" ]; then
        log "Creating state directory: ${STATE_DIR}"
        if ! mkdir -p "${STATE_DIR}" 2>/dev/null; then
            sudo mkdir -p "${STATE_DIR}" 2>/dev/null
        fi
        
        # Set proper permissions
        if [ -d "${STATE_DIR}" ]; then
            chmod 755 "${STATE_DIR}" 2>/dev/null || sudo chmod 755 "${STATE_DIR}"
        else
            log_and_summarize "CRITICAL ERROR: Failed to create state directory ${STATE_DIR}. Aborting."
            OVERALL_JOB_STATUS="INIT_FAILURE"; return 1;
        fi
    fi

    # Then create the manifest directory if needed
    if [ "${ENABLE_MANIFEST_GENERATION}" = true ] && [ ! -d "${MANIFEST_DIR}" ]; then
        log "Creating manifest directory: ${MANIFEST_DIR}"
        if ! mkdir -p "${MANIFEST_DIR}" 2>/dev/null; then
            sudo mkdir -p "${MANIFEST_DIR}" 2>/dev/null
        fi
        
        # Set proper permissions
        if [ -d "${MANIFEST_DIR}" ]; then
            chmod 755 "${MANIFEST_DIR}" 2>/dev/null || sudo chmod 755 "${MANIFEST_DIR}"
        else
            log_and_summarize "WARNING: Failed to create manifest directory ${MANIFEST_DIR}. Manifests will not be created."
        fi
    fi
    
    # Create MOTD status directory if enabled
    if [ "${ENABLE_MOTD_STATUS_FILE}" = true ] && [ -n "${MOTD_STATUS_FILE_PATH}" ]; then
        local motd_dir; motd_dir=$(dirname "${MOTD_STATUS_FILE_PATH}")
        if [ ! -d "${motd_dir}" ]; then
            log "Creating MOTD status directory: ${motd_dir}"
            if ! mkdir -p "${motd_dir}" 2>/dev/null; then
                sudo mkdir -p "${motd_dir}" 2>/dev/null
            fi
            
            # Set proper permissions
            if [ -d "${motd_dir}" ]; then
                chmod 755 "${motd_dir}" 2>/dev/null || sudo chmod 755 "${motd_dir}"
            fi
        fi
    fi

    # Dispatch to the appropriate function based on action
    if [ "${action_internal}" == "BACKUP" ]; then
        run_backup_mode "${GLOBAL_SCRIPT_RUN_TIMESTAMP}" # Pass the global timestamp
    elif [ "${action_internal}" == "VERIFY" ]; then
        run_verification_mode "${verify_dataset_filter_internal}" "${verify_set_id_filter_internal}"
    fi
    return $? # Return status of the called mode
}

# --- Script Entry Point ---
# Lockfile acquisition moved to very top, after Utility functions and Traps are defined.
# Initial checks moved after lock acquisition and basic logging is confirmed working.

# Define the lockfile location
LOCK_FILE="/var/lock/zfs_rclone_stream_backup.lock"

# Acquire the lock before proceeding
exec 200>"${LOCK_FILE}" || {
    log_and_summarize "ERROR: Could not open lockfile ${LOCK_FILE}. Check permissions."
    exit 1
}

if ! flock -n 200; then
    log_and_summarize "ERROR: Another instance of this script is already running. Exiting."
    exit 1
else
    log "Lock acquired on FD 200 for ${LOCK_FILE}."
fi

# Call main dispatcher with all script arguments
main_dispatcher_logic "$@"
# EXIT trap handles final cleanup and script exit code.