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

# Write TC-M-F01 test data: 5 measurements, 5000 records each
write_tc_m_f01_data() {
    log_info "Writing TC-M-F01 test data (5 measurements, 5000 records each)..."

    local measurements=("cpu" "memory" "disk" "network" "process")
    local records_per_meas=5000
    local base_ts=1745280000000000000
    local tmpfile=$(mktemp)

    for meas in "${measurements[@]}"; do
        log_info "  - Writing ${meas} (${records_per_meas} records)..."

        local batch_size=1000
        for batch_start in $(seq 0 $batch_size $((records_per_meas - 1))); do
            > "$tmpfile"
            local batch_end=$((batch_start + batch_size - 1))
            if [ $batch_end -ge $records_per_meas ]; then
                batch_end=$((records_per_meas - 1))
            fi

            for i in $(seq $batch_start $batch_end); do
                local offset=$((i * 60 * 1000000000))
                local ts=$((base_ts + offset))
                local value=$((RANDOM % 100)).$((RANDOM % 100))

                case $meas in
                    cpu)
                        echo "cpu,host=server-$(printf "%03d" $((i % 5 + 1))),region=us-west cpu_usage=${value} ${ts}" >> "$tmpfile"
                        ;;
                    memory)
                        echo "memory,host=server-$(printf "%03d" $((i % 5 + 1))),region=us-west memory_usage=${value} ${ts}" >> "$tmpfile"
                        ;;
                    disk)
                        echo "disk,host=server-$(printf "%03d" $((i % 5 + 1))),region=us-west disk_usage=${value} ${ts}" >> "$tmpfile"
                        ;;
                    network)
                        echo "network,host=server-$(printf "%03d" $((i % 5 + 1))),region=us-west network_usage=${value} ${ts}" >> "$tmpfile"
                        ;;
                    process)
                        echo "process,host=server-$(printf "%03d" $((i % 5 + 1))),region=us-west process_count=$((RANDOM % 100)) ${ts}" >> "$tmpfile"
                        ;;
                esac
            done

            local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
            curl -s -X POST "${write_url}" \
                -H "Content-Type: text/plain" \
                --data-binary "@$tmpfile" > /dev/null
        done

        log_info "  - ${meas}: ${records_per_meas} records written"
    done

    rm -f "$tmpfile"
}

# Write TC-M-F02 test data: 1 measurement, 20 series, 1000 records each
write_tc_m_f02_data() {
    log_info "Writing TC-M-F02 test data (metrics, 20 series, 1000 records each)..."

    local meas="metrics"
    local series_count=20
    local records_per_series=1000
    local base_ts=1745280000000000000
    local hosts=("server-001" "server-002" "server-003" "server-004" "server-005")
    local regions=("us-west" "us-east" "eu-west" "ap-east")
    local tmpfile=$(mktemp)

    for s in $(seq 0 $((series_count - 1))); do
        local host="${hosts[$((s % 5))]}"
        local region="${regions[$((s % 4))]}"
        local tags="host=${host},region=${region},env=prod"

        local batch_size=500
        for batch_start in $(seq 0 $batch_size $((records_per_series - 1))); do
            > "$tmpfile"
            local batch_end=$((batch_start + batch_size - 1))
            if [ $batch_end -ge $records_per_series ]; then
                batch_end=$((records_per_series - 1))
            fi

            for i in $(seq $batch_start $batch_end); do
                local offset=$((i * 60 * 1000000000))
                local ts=$((base_ts + offset))
                local value=$((RANDOM % 100)).$((RANDOM % 100))
                echo "${meas},${tags} value=${value} ${ts}" >> "$tmpfile"
            done

            local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
            curl -s -X POST "${write_url}" \
                -H "Content-Type: text/plain" \
                --data-binary "@$tmpfile" > /dev/null
        done

        log_info "  - Series $((s + 1))/20 completed"
    done

    rm -f "$tmpfile"
    log_info "  - ${meas}: $((series_count * records_per_series)) total records"
}

# Write TC-L-F01 test data: ~100k records for shard-group testing
write_tc_l_f01_data() {
    log_info "Writing TC-L-F01 test data (~100k records for shard-group)..."

    local meas="metrics"
    local total_records=100000
    local base_ts=1745280000000000000  # 2025-04-22
    local hosts=("server-001" "server-002" "server-003" "server-004" "server-005")
    local regions=("us-west" "us-east" "eu-west" "ap-east")
    local tmpfile=$(mktemp)

    log_info "  - Target: ${total_records} records across 30 days"

    local batch_size=5000
    local batch_count=0

    for batch_start in $(seq 0 $batch_size $((total_records - 1))); do
        > "$tmpfile"
        local batch_end=$((batch_start + batch_size - 1))
        if [ $batch_end -ge $total_records ]; then
            batch_end=$((total_records - 1))
        fi

        for i in $(seq $batch_start $batch_end); do
            local offset=$((i * 25 * 1000000000))  # ~25 second intervals
            local ts=$((base_ts + offset))
            local host="${hosts[$((i % 5))]}"
            local region="${regions[$((i % 4))]}"
            local value=$((RANDOM % 100)).$((RANDOM % 100))
            echo "${meas},host=${host},region=${region} value=${value} ${ts}" >> "$tmpfile"
        done

        local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
        curl -s -X POST "${write_url}" \
            -H "Content-Type: text/plain" \
            --data-binary "@$tmpfile" > /dev/null

        batch_count=$((batch_count + 1))
        if [ $((batch_count % 5)) -eq 0 ]; then
            log_info "  - Progress: $((batch_start + batch_size))/${total_records}"
        fi
    done

    rm -f "$tmpfile"
    log_info "  - ${meas}: ${total_records} records written (spanning ~30 days)"
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

# Write TC-S-D05 test data: field/tag names with special characters
write_tc_s_d05_data() {
    log_info "Writing TC-S-D05 test data (special field/tag names)..."

    local meas="special_names"
    local base_ts=1745280000000000000
    local tmpfile=$(mktemp)

    # Build line protocol with special field names and tag names
    # Field names: cpu usage (space), field.name (dot), cpu-usage (dash), a=b (equals), x,y (comma)
    # Tag names: region/name (slash), env[prod] (brackets)
    for i in $(seq 1 100); do
        local offset=$(( (i - 1) * 60 * 1000000000))
        local ts=$((base_ts + offset))
        local value=$((RANDOM % 100)).$((RANDOM % 100))

        # Line protocol: measurement,TAG_KEYS FIELD_KEYS TIMESTAMP
        # Note: InfluxDB field names with special chars need escaping in line protocol
        # Space, comma, = are escaped as \, , \=
        echo "${meas},region\/name=us-west,env\[prod\]=prod cpu\\ usage=${value},field\\.name=${value},cpu-usage=${value},a\\=b=${value},x\\,y=${value} ${ts}" >> "$tmpfile"
    done

    local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
    response=$(curl -s -w "\n%{http_code}" -X POST "${write_url}" \
        -H "Content-Type: text/plain" \
        --data-binary "@$tmpfile")

    local http_code=$(echo "$response" | tail -n1)
    if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
        log_info "  - ${meas}: 100 records with special names written"
    else
        log_error "Failed to write data: HTTP ${http_code}"
        rm -f "$tmpfile"
        exit 1
    fi
}

# Write TC-S-D06 test data: tag values with separator characters
write_tc_s_d06_data() {
    log_info "Writing TC-S-D06 test data (special tag values)..."

    local meas="special_tag_values"
    local base_ts=1745280000000000000
    local tmpfile=$(mktemp)

    # Tag values with separators: us-west,1 (comma), host=server1 (equals), multi\nline (newline), _empty_ (placeholder for empty), _space_ (space in value)
    for i in $(seq 1 100); do
        local offset=$(( (i - 1) * 60 * 1000000000))
        local ts=$((base_ts + offset))
        local value=$((RANDOM % 100)).$((RANDOM % 100))

        # Note: comma and equals in tag values must be escaped
        # Note: empty tag value not allowed in InfluxDB, use _empty_ placeholder
        # Note: space in tag value not allowed without escaping, use _space_ placeholder
        echo "${meas},tag_v1=us-west\\,1,tag_v2=host\\=server1,tag_v3=multi\\nline,tag_v4=_empty_,tag_v5=_space_ value=${value} ${ts}" >> "$tmpfile"
    done

    local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
    response=$(curl -s -w "\n%{http_code}" -X POST "${write_url}" \
        -H "Content-Type: text/plain" \
        --data-binary "@$tmpfile")

    local http_code=$(echo "$response" | tail -n1)
    if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
        log_info "  - ${meas}: 100 records with special tag values written"
    else
        log_error "Failed to write data: HTTP ${http_code}"
        rm -f "$tmpfile"
        exit 1
    fi
}

# Write TC-S-D07 test data: special float values (NaN, Inf)
# Note: InfluxDB Line Protocol doesn't support NaN/Inf directly
# We write them as strings to test how the migrator handles them
write_tc_s_d07_data() {
    log_info "Writing TC-S-D07 test data (special float values)..."

    local meas="special_floats"
    local base_ts=1745280000000000000
    local tmpfile=$(mktemp)

    # InfluxDB doesn't support NaN/Inf in line protocol
    # We write them as special string values to test how the migrator handles them
    for i in $(seq 1 100); do
        local offset=$(( (i - 1) * 60 * 1000000000))
        local ts=$((base_ts + offset))

        # Write as string since InfluxDB line protocol doesn't support NaN/Inf
        echo "${meas},host=server-001 nan_val=\"NaN\",pos_inf=\"+Inf\",neg_inf=\"-Inf\" ${ts}" >> "$tmpfile"
    done

    local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
    response=$(curl -s -w "\n%{http_code}" -X POST "${write_url}" \
        -H "Content-Type: text/plain" \
        --data-binary "@$tmpfile")
    local http_code=$(echo "$response" | tail -n1)
    if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
        log_info "  - ${meas}: 100 records with special float values written"
    else
        log_error "Failed to write data: HTTP ${http_code}"
        rm -f "$tmpfile"
        exit 1
    fi

    rm -f "$tmpfile"
}

# Write TC-M-D05 test data: high cardinality (1500 series, 150000 records)
write_tc_m_d05_data() {
    log_info "Writing TC-M-D05 test data (high cardinality: 1500 series, 150000 records)..."

    local meas="metrics"
    local total_records=150000
    local base_ts=1745280000000000000
    local tmpfile=$(mktemp)

    # 50 hosts * 10 regions * 3 env = 1500 series
    local hosts=()
    for i in $(seq 1 50); do
        hosts+=("server-$(printf "%03d" $i)")
    done
    local regions=("us-west" "us-east" "eu-west" "ap-east" "sa-east" "ap-south" "eu-north" "us-central" "ap-northeast" "sa-north")
    local envs=("prod" "staging" "dev")

    local batch_size=5000
    local record_count=0

    log_info "  - Target: ${total_records} records across 1500 series"

    for batch_start in $(seq 0 $batch_size $((total_records - 1))); do
        > "$tmpfile"
        local batch_end=$((batch_start + batch_size - 1))
        if [ $batch_end -ge $total_records ]; then
            batch_end=$((total_records - 1))
        fi

        for i in $(seq $batch_start $batch_end); do
            local offset=$((i * 10 * 1000000000))  # 10 second intervals
            local ts=$((base_ts + offset))
            local host_idx=$((i % 50))
            local region_idx=$((i % 10))
            local env_idx=$((i % 3))
            local value=$((RANDOM % 100)).$((RANDOM % 100))

            echo "${meas},host=${hosts[$host_idx]},region=${regions[$region_idx]},env=${envs[$env_idx]} value=${value} ${ts}" >> "$tmpfile"
            record_count=$((record_count + 1))
        done

        local write_url="${SOURCE_URL}/write?db=${SOURCE_DB}"
        curl -s -X POST "${write_url}" \
            -H "Content-Type: text/plain" \
            --data-binary "@$tmpfile" > /dev/null

        if [ $(( (batch_start / batch_size + 1) % 5 )) -eq 0 ]; then
            log_info "  - Progress: $((batch_start + batch_size))/${total_records}"
        fi
    done

    rm -f "$tmpfile"
    log_info "  - ${meas}: ${record_count} records written (1500 series)"
}

# Verify data
verify_data() {
    log_info "Verifying data in ${SOURCE_DB}..."

    for meas in cpu memory disk network process metrics special_names special_tag_values special_floats; do
        local count=$(curl -s -G "${SOURCE_URL}/query" \
            --data-urlencode "db=${SOURCE_DB}" \
            --data-urlencode "q=SELECT COUNT(*) FROM ${meas}" 2>/dev/null | \
            grep -o '"value":[0-9]*' | grep -o '[0-9]*' || echo "0")

        if [ -n "$count" ] && [ "$count" != "0" ]; then
            log_info "  - ${meas}: ${count} records"
        fi
    done
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test data..."

    for meas in cpu memory disk network process metrics special_names special_tag_values special_floats; do
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
            write_tc_s_d05_data
            write_tc_s_d06_data
            write_tc_s_d07_data
            write_tc_m_f01_data
            write_tc_m_f02_data
            write_tc_l_f01_data
            write_tc_m_d05_data
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
