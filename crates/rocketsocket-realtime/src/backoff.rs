//! Reconnect pacing.

use core::time::Duration;

/// Exponential backoff with full jitter.
///
/// Rocket.Chat bots are commonly deployed as a fleet that restarts together — a rolling
/// deploy, a server upgrade, a network blip — and unjittered backoff would synchronise
/// every one of them onto the same retry instants, turning a recovery into a stampede.
/// Full jitter (`sleep = random(0, window)`) spreads them out instead of merely spacing
/// each client's own attempts.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    cap: Duration,
    attempt: u32,
    rng: fastrand::Rng,
}

impl Backoff {
    /// The default first-retry window.
    pub const DEFAULT_BASE: Duration = Duration::from_secs(1);
    /// The default ceiling on the retry window.
    pub const DEFAULT_CAP: Duration = Duration::from_secs(64);

    /// A backoff with the default schedule, seeded from the thread-local RNG.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(Self::DEFAULT_BASE, Self::DEFAULT_CAP)
    }

    /// A backoff with an explicit schedule.
    #[must_use]
    pub fn with_limits(base: Duration, cap: Duration) -> Self {
        Self { base, cap, attempt: 0, rng: fastrand::Rng::new() }
    }

    /// A backoff with a deterministic RNG, for tests.
    #[must_use]
    pub fn with_seed(base: Duration, cap: Duration, seed: u64) -> Self {
        Self { base, cap, attempt: 0, rng: fastrand::Rng::with_seed(seed) }
    }

    /// How many failures have accumulated since the last [`reset`](Self::reset).
    ///
    /// Counts calls to [`next_delay`](Self::next_delay), which increments *before*
    /// returning — so immediately after scheduling the first retry this reads `1`, not `0`.
    /// A caller reporting "attempt N" to a user wants `attempt().saturating_sub(1)`.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The next delay, advancing the schedule.
    ///
    /// The first call returns [`Duration::ZERO`]: an established connection dropping once
    /// is usually a transient blip, and reconnecting immediately is what a user expects.
    /// Only a second consecutive failure starts backing off.
    pub fn next_delay(&mut self) -> Duration {
        let attempt = self.attempt;
        self.attempt = self.attempt.saturating_add(1);

        if attempt == 0 {
            return Duration::ZERO;
        }

        // Saturating throughout: `attempt` is unbounded, so the doubling must not overflow
        // and the multiplication must not wrap into a short sleep.
        let window = self
            .base
            .saturating_mul(1u32.checked_shl(attempt - 1).unwrap_or(u32::MAX))
            .min(self.cap);

        let millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        if millis == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis(self.rng.u64(0..=millis))
    }

    /// Clears the accumulated failures, after a connection is established and usable.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_retry_is_immediate() {
        let mut backoff = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(64), 1);
        assert_eq!(backoff.next_delay(), Duration::ZERO);
        assert_eq!(backoff.attempt(), 1);
    }

    #[test]
    fn delays_stay_within_the_doubling_window_and_the_cap() {
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(64);
        let mut backoff = Backoff::with_seed(base, cap, 42);

        let _immediate = backoff.next_delay();
        for attempt in 1..12u32 {
            let window =
                base.saturating_mul(1u32.checked_shl(attempt - 1).unwrap_or(u32::MAX)).min(cap);
            let delay = backoff.next_delay();
            assert!(delay <= window, "attempt {attempt}: {delay:?} exceeded window {window:?}");
            assert!(delay <= cap, "attempt {attempt}: {delay:?} exceeded cap {cap:?}");
        }
    }

    #[test]
    fn jitter_actually_spreads_clients_out() {
        // The point of full jitter is that two clients failing in lockstep do not retry in
        // lockstep. Different seeds must produce different schedules.
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(64);
        let schedule = |seed| {
            let mut backoff = Backoff::with_seed(base, cap, seed);
            (0..8).map(|_| backoff.next_delay()).collect::<Vec<_>>()
        };
        assert_ne!(schedule(1), schedule(2));
    }

    #[test]
    fn reset_returns_to_an_immediate_retry() {
        let mut backoff = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(64), 7);
        for _ in 0..5 {
            let _ = backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_delay(), Duration::ZERO);
    }

    #[test]
    fn a_very_long_outage_does_not_overflow_into_a_short_sleep() {
        // `attempt` is unbounded, so the shift and the multiply must both saturate; a wrap
        // would silently turn a capped backoff into a hot loop.
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(64);
        let mut backoff = Backoff::with_seed(base, cap, 3);
        for _ in 0..200 {
            assert!(backoff.next_delay() <= cap);
        }
        assert_eq!(backoff.attempt(), 200);
    }

    #[test]
    fn a_zero_cap_yields_no_delay_rather_than_panicking() {
        let mut backoff = Backoff::with_seed(Duration::from_secs(1), Duration::ZERO, 5);
        for _ in 0..4 {
            assert_eq!(backoff.next_delay(), Duration::ZERO);
        }
    }
}
