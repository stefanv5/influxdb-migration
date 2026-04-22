#!/bin/bash
# Data Setup Script for InfluxDB v1 E2E Tests
# This script prepares test data in the source InfluxDB instance

set -e

# Configuration
SOURCE_URL="${SOURCE_URL:-http://127.0.0.1:8084}"
SOURCE_DB="${SOURCE_DB:-test_source}"
SOURCE_RP="${SOURCE_RP:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prereqs() {
    log_info "Checking prerequisites..."

    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed"
        exit 1
    fi

    # Check source connection
    if ! curl -s "${SOURCE_URL}/ping" &> /dev/null; then
        log_error "Cannot connect to source InfluxDB at ${SOURCE_URL}"
        exit 1
    fi

    log_info "Prerequisites OK"
}

# Create database if not exists
create_database() {
    log_info "Creating database ${SOURCE_DB} if not exists..."

    curl -s -X POST "${SOURCE_URL}/query" \
        --data-urlencode "q=CREATE DATABASE IF NOT EXISTS ${SOURCE_DB}" \
        > /dev/null

    log_info "Database ${SOURCE_DB} ready"
}

# Write TC-S-F01 test data: single measurement, 100 records
write_tc_s_f01_data() {
    log_info "Writing TC-S-F01 test data (cpu measurement, 100 records)..."

    local timestamp="2025-04-22T00:00:00Z"
    local interval_minutes=1

    # Build line protocol data
    local lines=""
    for i in $(seq 1 100); do
        # Calculate timestamp (nanoseconds)
        local ts=$(date -d "${timestamp}" +%s%N 2>/dev/null || date -v+0S -j -f "%Y-%m-%dT%H:%M:%SZ" "${timestamp}" +%s%N 2>/dev/null)
        if [ -z "$ts" ] || [ "$ts" = "0" ]; then
            # Fallback: calculate manually
            local base_ts=1745280000000000000
            local offset=$(( (i - 1) * 60 * 1000000000))
            ts=$((base_ts + offset))
        fi

        # Generate random cpu_usage (0-100)
        local cpu_usage=$((RANDOM % 100 + 0)).$((RANDOM % 100))

        # Random status
        local status="ok"
        if [ $((i % 10)) -eq 0 ]; then
            status="warning"
        elif [ $((i % 20)) -eq 0 ]; then
            status="error"
        fi

        lines="${lines}cpu,host=server-001,region=us-west cpu_usage=${cpu_usage},status=\"${status}\" ${ts}"
        if [ $i -lt 100 ]; then
            lines="${lines}"$'\n'
        fi
    done

    # Write to InfluxDB
    local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
    if [ -n "${SOURCE_RP}" ]; then
        write_url="${write_url}&rp=${SOURCE_RP}"
    fi

    response=$(curl -s -w "\n%{http_code}" -X POST "${write_url}" \
        -H "Content-Type: text/plain" \
        --data-binary "${lines}")

    local http_code=$(echo "$response" | tail -n1)

    if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
        log_info "Successfully wrote 100 records to cpu measurement"
    else
        log_error "Failed to write data: HTTP ${http_code}"
        echo "$response"
        exit 1
    fi
}

# Write TC-S-F02 test data: 3 measurements, 100 records each
write_tc_s_f02_data() {
    log_info "Writing TC-S-F02 test data (cpu, memory, disk measurements, 100 records each)..."

    for meas in cpu memory disk; do
        local lines=""
        local base_ts=1745280000000000000

        for i in $(seq 1 100); do
            local offset=$(( (i - 1) * 60 * 1000000000))
            local ts=$((base_ts + offset))

            case $meas in
                cpu)
                    local value=$((RANDOM % 100 + 0)).$((RANDOM % 100))
                    lines="${lines}cpu,host=server-001,region=us-west cpu_usage=${value} ${ts}"
                    ;;
                memory)
                    local value=$((RANDOM % 64 + 0)).$((RANDOM % 100))
                    lines="${lines}memory,host=server-001,region=us-west memory_usage=${value} ${ts}"
                    ;;
                disk)
                    local value=$((RANDOM % 100 + 0)).$((RANDOM % 100))
                    lines="${lines}disk,host=server-001,region=us-west disk_usage=${value} ${ts}"
                    ;;
            esac

            if [ $i -lt 100 ]; then
                lines="${lines}"$'\n'
            fi
        done

        local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
        response=$(curl -s -w "\n%{http_code}" -X POST "${write_url}" \
            -H "Content-Type: text/plain" \
            --data-binary "${lines}")

        local http_code=$(echo "$response" | tail -n1)
        if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
            log_info "  - ${meas}: 100 records written"
        else
            log_warn "  - ${meas}: HTTP ${http_code}"
        fi
    done
}

# Verify data
verify_data() {
    log_info "Verifying data in ${SOURCE_DB}..."

    for meas in cpu memory disk; do
        local count=$(curl -s -G "${SOURCE_URL}/query" \
            --data-urlencode "db=${SOURCE_DB}" \
            --data-urlencode "q=SELECT COUNT(*) FROM ${meas}" | \
            grep -o '"value":[0-9]*' | grep -o '[0-9]*' || echo "0")

        if [ -n "$count" ] && [ "$count" != "0" ]; then
            log_info "  - ${meas}: ${count} records"
        fi
    done
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test data..."

    for meas in cpu memory disk; do
        curl -s -X POST "${SOURCE_URL}/query" \
            --data-urlencode "q=DROP MEASUREMENT ${meas}" \
            --data-urlencode "db=${SOURCE_DB}" > /dev/null
    done

    log_info "Cleanup complete"
}

# Main
main() {
    local command="${1:-setup}"

    case $command in
        setup)
            check_prereqs
            create_database
            write_tc_s_f01_data
            write_tc_s_f02_data
            verify_data
            ;;
        verify)
            verify_data
            ;;
        cleanup)
            cleanup
            ;;
        *)
            echo "Usage: $0 {setup|verify|cleanup}"
            echo "  setup   - Create database and write test data"
            echo "  verify  - Verify existing test data"
            echo "  cleanup - Remove test data"
            exit 1
            ;;
    esac
}

main "$@"
