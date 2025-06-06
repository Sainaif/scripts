#!/bin/bash

# =============================================================================
# Rclone Directory Upload Script
# =============================================================================
#
# Script Name: rclone_directory_upload.sh
# Version: 1.0
# Date: 2025-05-28
#
# This script automates the process of uploading specified local directories
# to a remote storage location using rclone.
#
# Key Features:
#   - Uploads multiple specified directories.
#   - Uses rclone copy (non-encrypted).
#   - Prevents simultaneous runs using a lock file.
#   - Provides detailed logging.
#   - Sends email notifications via Mailgun on success or failure.
#
# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================
#
# 1. CONFIGURE: Edit the 'Configuration Section' below. You MUST set
#               RCLONE_REMOTE_NAME, MAILGUN_API_KEY, and MAILGUN_DOMAIN.
# 2. SAVE:      Save this script (e.g., /usr/local/bin/rclone_directory_upload.sh).
# 3. PERMISSIONS: Make it executable: sudo chmod +x /usr/local/bin/rclone_directory_upload.sh
# 4. RUN:       Execute manually (sudo ...) or schedule via cron/systemd.
#
# =============================================================================

# --- Configuration Section ---

# Directories to upload. Ensure paths are correct and quoted if they contain spaces.
SOURCE_DIRS=(
    "path/to/your/first/directory"
    "path/to/your/second/directory"
)

# Name of your rclone remote (e.g., "google_drive", "onedrive").
RCLONE_REMOTE_NAME="YOUR_RCLONE_REMOTE" # <--- !!! CHANGE THIS !!!

# Base path on the remote where the directories will be placed.
RCLONE_BASE_PATH="ServerBackups/DirectUploads" # <--- You can change this

# Path to your rclone configuration file.
RCLONE_CONFIG_PATH="path/to/your/rclone.conf" # <--- !!! CHANGE THIS !!!

# Log file for script output.
LOG_FILE="/var/log/rclone_directory_upload.log"

# Lock file to prevent multiple instances from running.
LOCK_FILE="/var/lock/rclone_directory_upload.lock"


# Email notification settings (Mailgun):
ENABLE_MAILGUN_NOTIFICATIONS=true             # Set to false to disable emails.
NOTIFY_ON_SUCCESS=true                        # Send email on success?
NOTIFY_ON_FAILURE=true                        # Send email on failure?
MAILGUN_API_KEY="YOUR_MAILGUN_API_KEY"        # <--- !!! CHANGE THIS !!!
MAILGUN_DOMAIN="your.mailgun.domain.com"      # <--- !!! CHANGE THIS !!!
MAILGUN_API_BASE_URL="https://api.eu.mailgun.net/v3" # Change to "https://api.mailgun.net/v3" if using in US.
MAILGUN_RECIPIENT_EMAIL="your-email@example.com" # <--- !!! CHANGE THIS !!!
MAILGUN_SENDER_EMAIL="rclone-uploader@${MAILGUN_DOMAIN}" # Sender email address for notifications.
MAILGUN_SUBJECT_PREFIX="[Rclone Upload]" # Prefix for email subjects.

# External commands
readonly RCLONE_CMD_BASE="rclone"
readonly CURL_CMD_BASE="curl"

# Rclone base command and options for the upload.
RCLONE_OPTIONS=(
    "--config" "${RCLONE_CONFIG_PATH}" 
    "--tpslimit" "20" 
    "--tpslimit-burst" "10" 
    "--transfers" "8" 
    "--checkers" "16" 
    "--drive-pacer-min-sleep" "5ms" 
    "--drive-pacer-burst" "10" 
    "--retries" "20" 
    "--low-level-retries" "15"
    "-v"                    
    "--stats=1m"             
    "--stats-one-line"
    "--fast-list"            
)

# --- End of Configuration ---

# --- Script Internals ---
OPERATION_START_TIME=$(date +%s)
OVERALL_JOB_STATUS="PENDING"
JOB_SUMMARY_LOG=""

# --- Utility Functions ---

# Logs a message to the log file and stdout.
log() {
    echo "$(date +'%F %T') - $1" | sudo tee -a "$LOG_FILE"
}

# Adds a message to the summary log for notifications.
add_to_summary() {
    JOB_SUMMARY_LOG+="$(date +'%F %T') - $1\n"
}

# Logs a message and adds it to the summary.
log_and_summarize() {
    log "$1"
    add_to_summary "$1"
}

# Sends an email notification using Mailgun API.
send_mailgun_notification() {
    local subject="$1"
    local body="$2"
    local notification_type="${3:-critical_failure}" # Default to failure

    # Check if notifications are enabled and if this type should be sent
    if [ "${ENABLE_MAILGUN_NOTIFICATIONS}" != true ]; then return 0; fi
    if [ "${notification_type}" = "success" ] && [ "${NOTIFY_ON_SUCCESS}" != true ]; then log "Email suppressed (success off)"; return 0; fi
    if [[ "${notification_type}" == "critical_failure" || "${notification_type}" == "failure" ]] && [ "${NOTIFY_ON_FAILURE}" != true ]; then log "Email suppressed (failure off)"; return 0; fi

    local full_subject="${MAILGUN_SUBJECT_PREFIX} ${subject}"
    local mailgun_api_send_url="${MAILGUN_API_BASE_URL}/${MAILGUN_DOMAIN}/messages"

    log "Attempting Mailgun notification (Subject: ${full_subject})"

    local temp_text_file; temp_text_file=$(mktemp)
    echo -e "${body}" > "${temp_text_file}"

    local CURL_RESPONSE
    CURL_RESPONSE=$(${CURL_CMD_BASE} --silent --show-error -X POST \
        --user "api:${MAILGUN_API_KEY}" "${mailgun_api_send_url}" \
        -F from="${MAILGUN_SENDER_EMAIL}" \
        -F to="${MAILGUN_RECIPIENT_EMAIL}" \
        -F subject="${full_subject}" \
        -F text=@"${temp_text_file}" 2>&1)

    local curl_exit_status=$?
    rm -f "${temp_text_file}"

    if [ ${curl_exit_status} -eq 0 ] && ([[ "${CURL_RESPONSE}" == *"Queued. Thank you."* ]] || [[ "${CURL_RESPONSE}" == *"message.id"* ]]); then
        log "Mailgun notification sent."
    else
        log_and_summarize "ERROR: Mailgun notification failed. Code: ${curl_exit_status}. API Resp: ${CURL_RESPONSE}"
        OVERALL_JOB_STATUS="FAILURE" # Mark as failure if notification fails
    fi
}

# Cleanup function called on script exit.
script_exit_cleanup() {
    local exit_code=$?
    local final_status="${OVERALL_JOB_STATUS}"

    # If status is still PENDING, something went wrong.
    if [ "${final_status}" = "PENDING" ]; then
        final_status="UNEXPECTED_FAILURE (Code: ${exit_code})"
        add_to_summary "CRITICAL: Script finished unexpectedly or without setting a status. Code: ${exit_code}."
    elif [ $exit_code -ne 0 ] && [ "${final_status}" = "SUCCESS" ]; then
        final_status="UNEXPECTED_FAILURE (Code: ${exit_code})"
        add_to_summary "CRITICAL: Script exited non-zero despite reporting success. Code: ${exit_code}."
    fi

    OPERATION_END_TIME=$(date +%s)
    local duration=$((OPERATION_END_TIME - OPERATION_START_TIME))
    add_to_summary "Total script execution time: ${duration} seconds."
    log "Script finished. Status: ${final_status}."

    local notification_type="critical_failure"
    if [ "${final_status}" = "SUCCESS" ]; then
        notification_type="success"
    fi

    send_mailgun_notification "Upload Finished - ${final_status}" \
        "Rclone Upload Script status: ${final_status} on $(hostname -f).\n\nSummary:\n${JOB_SUMMARY_LOG}\nFull log: ${LOG_FILE}" \
        "${notification_type}"

    flock -u 200 # Release lock
    exec 200>&-  # Close file descriptor
}

# --- Main Script Execution ---

# 1. Acquire Lock
# -----------------
exec 200>"${LOCK_FILE}" || {
    echo "$(date +'%F %T') - CRITICAL: Cannot open lock file ${LOCK_FILE}." | sudo tee -a "$LOG_FILE"
    exit 1
}
if ! flock -n 200; then
    echo "$(date +'%F %T') - INFO: Another instance is running. Exiting." | sudo tee -a "$LOG_FILE"
    exit 0
fi

trap 'script_exit_cleanup' EXIT
log_and_summarize "--- Rclone Upload Script Started ---"
OVERALL_JOB_STATUS="SUCCESS" # Assume success unless something fails

# 2. Pre-flight Checks
# --------------------
if [ "${RCLONE_REMOTE_NAME}" = "YOUR_RCLONE_REMOTE" ]; then log_and_summarize "CRITICAL ERROR: RCLONE_REMOTE_NAME not set. Exiting."; OVERALL_JOB_STATUS="FAILURE"; exit 1; fi
if [ "${MAILGUN_API_KEY}" = "YOUR_MAILGUN_API_KEY" ]; then log_and_summarize "CRITICAL ERROR: MAILGUN_API_KEY not set. Exiting."; OVERALL_JOB_STATUS="FAILURE"; exit 1; fi
if [ "${MAILGUN_DOMAIN}" = "your.mailgun.domain.com" ]; then log_and_summarize "CRITICAL ERROR: MAILGUN_DOMAIN not set. Exiting."; OVERALL_JOB_STATUS="FAILURE"; exit 1; fi
if [ ! -f "${RCLONE_CONFIG_PATH}" ]; then log_and_summarize "CRITICAL ERROR: Rclone config not found at ${RCLONE_CONFIG_PATH}. Exiting."; OVERALL_JOB_STATUS="FAILURE"; exit 1; fi
if ! command -v curl &> /dev/null; then log_and_summarize "CRITICAL ERROR: 'curl' command not found, cannot send emails. Exiting."; OVERALL_JOB_STATUS="FAILURE"; exit 1; fi

# 3. Upload Loop
# --------------
for src_dir in "${SOURCE_DIRS[@]}"; do
    if [ -d "${src_dir}" ]; then
        dest_name=$(basename "${src_dir}")
        dest_path="${RCLONE_REMOTE_NAME}:${RCLONE_BASE_PATH}/${dest_name}"

        log_and_summarize "Uploading '${src_dir}' to '${dest_path}'..."

        # Execute rclone copy and capture its stderr to log
        rclone_output=$("${RCLONE_CMD_BASE}" copy "${RCLONE_OPTIONS[@]}" "${src_dir}" "${dest_path}" 2>&1)
        rclone_status=${PIPESTATUS[0]}

        # Log rclone output
        if [ -n "${rclone_output}" ]; then
             echo "${rclone_output}" | sudo tee -a "$LOG_FILE"
        fi

        if [ $rclone_status -eq 0 ]; then
            log_and_summarize "SUCCESS: Upload completed for '${src_dir}'."
        else
            log_and_summarize "ERROR: Upload FAILED for '${src_dir}' (Rclone Exit Code: ${rclone_status})."
            OVERALL_JOB_STATUS="FAILURE"
        fi
    else
        log_and_summarize "WARNING: Source directory '${src_dir}' not found. Skipping."
        OVERALL_JOB_STATUS="FAILURE"
    fi
    add_to_summary "----------------------------------------"
done

# 4. Finish
# -----------
log_and_summarize "--- Rclone Upload Script Finished. Overall Status: ${OVERALL_JOB_STATUS} ---"

# Trap handles calling cleanup, which sends email and sets exit code.
if [ "${OVERALL_JOB_STATUS}" = "SUCCESS" ]; then
    exit 0
else
    exit 1
fi