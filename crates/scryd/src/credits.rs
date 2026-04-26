//! Credit-based settlement for embedding API calls (model-specific pricing via
//! `credits_per_million_tokens`).

use std::env;

/// How many credits to charge per 1_000_000 tokens reported or estimated for an embed call.
#[derive(Debug, Clone, Copy)]
pub struct EmbeddingCreditConfig {
    pub credits_per_million_tokens: f64,
}

impl Default for EmbeddingCreditConfig {
    fn default() -> Self {
        Self {
            credits_per_million_tokens: 1.0,
        }
    }
}

impl EmbeddingCreditConfig {
    pub fn from_env() -> Self {
        let credits_per_million_tokens = env::var("SCRYD_EMBED_CREDITS_PER_MILLION_TOKENS")
            .ok()
            .and_then(|raw| raw.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(1.0);
        Self {
            credits_per_million_tokens,
        }
    }

    /// Converts token usage to credits (fractional credits rounded up to a whole credit).
    pub fn credits_for_tokens(&self, total_tokens: u64) -> u64 {
        if total_tokens == 0 || self.credits_per_million_tokens <= 0.0 {
            return 0;
        }
        let product = (total_tokens as f64) * self.credits_per_million_tokens / 1_000_000.0;
        if !product.is_finite() || product <= 0.0 {
            return 0;
        }
        // Avoid UB from `as u64` when the float is out of range.
        if product >= u64::MAX as f64 {
            return u64::MAX;
        }
        product.ceil() as u64
    }
}

/// Charge for a single embedding RPC (document batch or query).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingCharge {
    pub total_tokens: u64,
    pub credits: u64,
    /// When true, `total_tokens` came from provider `usage`; otherwise a UTF-8 heuristic.
    pub from_upstream_usage: bool,
}

pub fn estimate_tokens_utf8(text: &str) -> u64 {
    let bytes = text.len() as u64;
    // Rough heuristic: ~4 bytes per token for mostly-ASCII; never zero for non-empty text.
    let est = bytes.div_ceil(4);
    if est == 0 && !text.is_empty() {
        1
    } else {
        est
    }
}

pub fn estimate_tokens_batch(inputs: &[String]) -> u64 {
    inputs.iter().map(|s| estimate_tokens_utf8(s)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_scales_per_million() {
        let cfg = EmbeddingCreditConfig {
            credits_per_million_tokens: 2.0,
        };
        assert_eq!(cfg.credits_for_tokens(500_000), 1);
        assert_eq!(cfg.credits_for_tokens(500_001), 2);
        assert_eq!(cfg.credits_for_tokens(1_000_000), 2);
    }

    #[test]
    fn zero_credits_when_rate_zero() {
        let cfg = EmbeddingCreditConfig {
            credits_per_million_tokens: 0.0,
        };
        assert_eq!(cfg.credits_for_tokens(1_000_000), 0);
    }

    #[test]
    fn credits_saturates_when_product_overflows_u64() {
        let cfg = EmbeddingCreditConfig {
            credits_per_million_tokens: 1.0e30,
        };
        assert_eq!(cfg.credits_for_tokens(1), u64::MAX);
    }
}
