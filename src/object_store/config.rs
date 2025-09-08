use std::time::Duration;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RequestConfig {
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub operation_timeout: Option<Duration>,
    pub operation_attempt_timeout: Option<Duration>,
    pub max_attempts: Option<u32>,
    pub initial_backoff: Option<Duration>,
    pub max_backoff: Option<Duration>,
}

impl RequestConfig {
    pub fn is_noop(&self) -> bool {
        self.connect_timeout.is_none()
            && self.read_timeout.is_none()
            && self.operation_timeout.is_none()
            && self.operation_attempt_timeout.is_none()
            && self.max_attempts.is_none()
            && self.initial_backoff.is_none()
            && self.max_backoff.is_none()
    }

    pub fn timeout_config(&self) -> aws_sdk_s3::config::timeout::TimeoutConfig {
        let mut builder = aws_sdk_s3::config::timeout::TimeoutConfig::builder();

        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(read_timeout) = self.read_timeout {
            builder = builder.read_timeout(read_timeout);
        }
        if let Some(timeout) = self.operation_timeout {
            builder = builder.operation_timeout(timeout);
        }
        if let Some(attempt_timeout) = self.operation_attempt_timeout {
            builder = builder.operation_attempt_timeout(attempt_timeout);
        }

        builder.build()
    }

    pub fn retry_config(&self) -> aws_sdk_s3::config::retry::RetryConfig {
        let mut config = aws_sdk_s3::config::retry::RetryConfig::standard();

        if let Some(max_attempts) = self.max_attempts {
            config = config.with_max_attempts(max_attempts);
        }
        if let Some(initial_backoff) = self.initial_backoff {
            config = config.with_initial_backoff(initial_backoff);
        }
        if let Some(max_backoff) = self.max_backoff {
            config = config.with_max_backoff(max_backoff);
        }

        config
    }
}
