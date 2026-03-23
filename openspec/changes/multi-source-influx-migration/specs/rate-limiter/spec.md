## ADDED Requirements

### Requirement: Token Bucket Rate Limiter
The system SHALL implement token bucket algorithm for rate limiting.

#### Scenario: Rate limit calculation
- **WHEN** a batch requests N points
- **THEN** the rate limiter SHALL check if N tokens are available
- **AND** SHALL return true if tokens are sufficient
- **AND** SHALL deduct tokens from the bucket

### Requirement: Token Refill
The rate limiter SHALL refill tokens based on elapsed time.

#### Scenario: Token refill
- **WHEN** time passes since last request
- **THEN** the rate limiter SHALL add rate * elapsed_seconds tokens
- **AND** SHALL cap tokens at burst_size
- **AND** SHALL update last_time for next calculation

### Requirement: Burst Handling
The rate limiter SHALL support burst requests up to burst_size.

#### Scenario: Burst request
- **WHEN** a batch requests more tokens than current tokens but less than burst_size
- **THEN** the rate limiter SHALL grant the request if tokens will be available after refill
- **AND** SHALL handle the burst correctly

### Requirement: Global Rate Limit
The rate limiter SHALL apply to all migration tasks globally.

#### Scenario: Global limit
- **WHEN** multiple tasks are running
- **THEN** all tasks SHALL share the same rate limiter instance
- **AND** total points SHALL not exceed points_per_second
