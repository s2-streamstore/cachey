use std::time::Duration;

use aws_sdk_s3::config::{retry::RetryConfig, timeout::TimeoutConfig};

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
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.connect_timeout.is_none()
            && self.read_timeout.is_none()
            && self.operation_timeout.is_none()
            && self.operation_attempt_timeout.is_none()
            && self.max_attempts.is_none()
            && self.initial_backoff.is_none()
            && self.max_backoff.is_none()
    }

    fn has_timeout_overrides(&self) -> bool {
        self.connect_timeout.is_some()
            || self.read_timeout.is_some()
            || self.operation_timeout.is_some()
            || self.operation_attempt_timeout.is_some()
    }

    fn has_retry_overrides(&self) -> bool {
        self.max_attempts.is_some() || self.initial_backoff.is_some() || self.max_backoff.is_some()
    }

    #[must_use]
    pub fn merged_timeout_config(&self, base: Option<&TimeoutConfig>) -> Option<TimeoutConfig> {
        if !self.has_timeout_overrides() {
            return None;
        }

        let mut builder = base.map_or_else(TimeoutConfig::builder, TimeoutConfig::to_builder);

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

        Some(builder.build())
    }

    #[must_use]
    pub fn merged_retry_config(&self, base: Option<&RetryConfig>) -> Option<RetryConfig> {
        if !self.has_retry_overrides() {
            return None;
        }

        let mut config = base.cloned().unwrap_or_else(RetryConfig::standard);

        if let Some(max_attempts) = self.max_attempts {
            config = config.with_max_attempts(max_attempts);
        }
        if let Some(initial_backoff) = self.initial_backoff {
            config = config.with_initial_backoff(initial_backoff);
        }
        if let Some(max_backoff) = self.max_backoff {
            config = config.with_max_backoff(max_backoff);
        }

        Some(config)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_sdk_s3::config::{retry::RetryConfig, timeout::TimeoutConfig};

    use crate::object_store::config::RequestConfig;

    #[test]
    fn merged_timeout_config_preserves_unset_base_fields() {
        let base = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .operation_timeout(Duration::from_mins(1))
            .operation_attempt_timeout(Duration::from_secs(20))
            .build();
        let request = RequestConfig {
            connect_timeout: Some(Duration::from_secs(5)),
            ..RequestConfig::default()
        };

        let merged = request
            .merged_timeout_config(Some(&base))
            .expect("timeout overrides are set");

        assert_eq!(merged.connect_timeout(), Some(Duration::from_secs(5)));
        assert_eq!(merged.read_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(merged.operation_timeout(), Some(Duration::from_mins(1)));
        assert_eq!(
            merged.operation_attempt_timeout(),
            Some(Duration::from_secs(20))
        );
    }

    #[test]
    fn merged_timeout_config_without_timeout_overrides_is_none() {
        let request = RequestConfig {
            max_attempts: Some(5),
            ..RequestConfig::default()
        };

        assert!(request.merged_timeout_config(None).is_none());
    }

    #[test]
    fn merged_retry_config_preserves_unset_base_fields() {
        let base = RetryConfig::standard()
            .with_max_attempts(7)
            .with_initial_backoff(Duration::from_millis(250))
            .with_max_backoff(Duration::from_mins(1));
        let request = RequestConfig {
            max_attempts: Some(5),
            ..RequestConfig::default()
        };

        let merged = request
            .merged_retry_config(Some(&base))
            .expect("retry overrides are set");

        assert_eq!(merged.max_attempts(), 5);
        assert_eq!(merged.initial_backoff(), Duration::from_millis(250));
        assert_eq!(merged.max_backoff(), Duration::from_mins(1));
    }

    #[test]
    fn merged_retry_config_without_base_uses_standard_defaults() {
        let request = RequestConfig {
            max_attempts: Some(9),
            ..RequestConfig::default()
        };

        let merged = request
            .merged_retry_config(None)
            .expect("retry overrides are set");

        assert_eq!(merged.max_attempts(), 9);
        assert_eq!(merged.initial_backoff(), Duration::from_secs(1));
        assert_eq!(merged.max_backoff(), Duration::from_secs(20));
    }

    #[test]
    fn merged_retry_config_without_retry_overrides_is_none() {
        let request = RequestConfig {
            connect_timeout: Some(Duration::from_secs(5)),
            ..RequestConfig::default()
        };

        assert!(request.merged_retry_config(None).is_none());
    }
}
