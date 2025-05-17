#!/bin/bash

# ZFS Backup Status Display Script
# This script reads the status files from both local and remote ZFS backup
# scripts and generates a consolidated status view suitable for MOTD.

# Configuration
LOCAL_STATUS_FILE="/var/lib/zfs-backup-status/local_backup_status.txt"
REMOTE_STATUS_FILE="/var/lib/zfs-backup-status/rclone_stream_backup_status.txt"
COLOR_OUTPUT=true
SHOW_SUMMARY_LINES=5  # Number of summary lines to show from each backup

# Function to print text with color
colorize() {
  local color="$1"
  local text="$2"
  
  if [[ "$COLOR_OUTPUT" == "true" && -t 1 ]]; then
    case "$color" in
      "red")     echo -e "\e[31m${text}\e[0m" ;;
      "green")   echo -e "\e[32m${text}\e[0m" ;;
      "yellow")  echo -e "\e[33m${text}\e[0m" ;;
      "blue")    echo -e "\e[34m${text}\e[0m" ;;
      "magenta") echo -e "\e[35m${text}\e[0m" ;;
      "cyan")    echo -e "\e[36m${text}\e[0m" ;;
      *)         echo "$text" ;;
    esac
  else
    echo "$text"
  fi
}

# Function to display section header
print_header() {
  local title="$1"
  local width=80
  local padding=$(( (width - ${#title} - 4) / 2 ))
  
  echo
  colorize "cyan" "$(printf '═%.0s' $(seq 1 $width))"
  colorize "cyan" "$(printf '%-'$width's' "═══ $title $(printf '═%.0s' $(seq 1 $padding)))"
  colorize "cyan" "$(printf '═%.0s' $(seq 1 $width))"
}

# Function to process a status file
process_status_file() {
  local file="$1"
  local type="$2"
  
  if [[ ! -f "$file" ]]; then
    colorize "red" "$type Backup Status: NOT AVAILABLE"
    colorize "yellow" "Status file not found: $file"
    return
  fi
  
  # Extract key information
  local status=$(grep -m1 "Overall Status:" "$file" | cut -d':' -f2- | xargs)
  local last_run=$(head -n1 "$file" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
  local size_info=$(grep -m1 "Total .* Size:" "$file" | xargs)
  
  # Determine status color
  local status_color="green"
  if [[ "$status" == *"FAILURE"* || "$status" == *"ERROR"* ]]; then
    status_color="red"
  elif [[ "$status" != "SUCCESS" ]]; then
    status_color="yellow"
  fi
  
  # Print status header
  colorize "blue" "$type Backup Status:"
  
  # Print status details
  echo "Last Run: $last_run"
  echo -n "Status: " 
  colorize "$status_color" "$status"
  echo "$size_info"
  
  # Print dataset status
  echo "Datasets:"
  local in_dataset_section=false
  local in_summary_section=false
  local summary_lines=0
  
  while IFS= read -r line; do
    if [[ "$line" == "Dataset: "* ]]; then
      in_dataset_section=true
      colorize "magenta" "  ${line#Dataset: }"
    elif [[ "$in_dataset_section" == true && "$line" != "" ]]; then
      if [[ "$line" == "  "* ]]; then
        echo "    ${line#  }"
      else
        in_dataset_section=false
      fi
    elif [[ "$line" == "Summary"* ]]; then
      in_summary_section=true
      echo
      colorize "blue" "$line"
    elif [[ "$in_summary_section" == true && "$summary_lines" -lt "$SHOW_SUMMARY_LINES" ]]; then
      echo "  $line"
      ((summary_lines++))
    fi
  done < "$file"
  
  echo
}

# Main function
main() {
  print_header "ZFS BACKUP STATUS"
  
  # Process local backup status
  process_status_file "$LOCAL_STATUS_FILE" "Local"
  
  # Process remote backup status
  process_status_file "$REMOTE_STATUS_FILE" "Remote"
  
  # Print footer
  colorize "cyan" "$(printf '═%.0s' $(seq 1 80))"
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-color)
      COLOR_OUTPUT=false
      shift
      ;;
    --summary-lines=*)
      SHOW_SUMMARY_LINES="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--no-color] [--summary-lines=N]"
      exit 1
      ;;
  esac
done

# Run main function
main