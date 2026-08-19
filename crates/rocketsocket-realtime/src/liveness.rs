//! Connection liveness policy.
//!
//! The DDP keepalive is `{"msg":"ping"}` / `{"msg":"pong"}` **inside text frames**, not
//! RFC 6455 ping frames — neither Rocket.Chat server implementation sends those. A
//! WebSocket-level ping is still worth sending to stop proxies idling the TCP connection
//! out, but it must never be mistaken for evidence the server is healthy: the `ws` library
//! answers control frames even when the event loop above it is wedged.

use core::time::Duration;
use std::time::Instant;

/// What the liveness policy wants next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Liveness {
    /// The connection has been active recently enough. Nothing to do.
    Healthy,
    /// Idle long enough to be worth probing. Send a DDP `ping`.
    SendPing,
    /// Silent past the deadline. Tear the connection down and reconnect.
    Dead,
}

/// Tracks how long a connection has been silent.
///
/// The defaults sit inside both servers' budgets — the monolith allows 15 s idle plus 15 s
/// to answer, the EE `ddp-streamer` 30 s plus 30 s — without being chatty.
#[derive(Debug, Clone)]
pub struct LivenessPolicy {
    idle_before_ping: Duration,
    idle_before_dead: Duration,
    last_seen: Instant,
    ping_in_flight: bool,
}

impl LivenessPolicy {
    /// Probe after this much silence.
    pub const DEFAULT_PING_AFTER: Duration = Duration::from_secs(25);
    /// Give up after this much silence.
    pub const DEFAULT_DEAD_AFTER: Duration = Duration::from_secs(45);

    /// A policy with the default thresholds, starting from `now`.
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self::with_thresholds(Self::DEFAULT_PING_AFTER, Self::DEFAULT_DEAD_AFTER, now)
    }

    /// A policy with explicit thresholds.
    ///
    /// # Panics
    /// Panics if `idle_before_dead` is not greater than `idle_before_ping`; a probe that is
    /// not given time to be answered would declare every connection dead.
    #[must_use]
    pub fn with_thresholds(
        idle_before_ping: Duration,
        idle_before_dead: Duration,
        now: Instant,
    ) -> Self {
        assert!(
            idle_before_dead > idle_before_ping,
            "the death deadline must leave time for a ping to be answered"
        );
        Self { idle_before_ping, idle_before_dead, last_seen: now, ping_in_flight: false }
    }

    /// Records inbound traffic.
    ///
    /// Call this for **every** frame, not just `pong`. Both servers reset their own
    /// heartbeat timer on any inbound message, and the client should mirror that: a busy
    /// connection is demonstrably alive and does not need probing.
    pub fn saw_traffic(&mut self, now: Instant) {
        self.last_seen = now;
        self.ping_in_flight = false;
    }

    /// How long the connection has been silent.
    #[must_use]
    pub fn idle_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.last_seen)
    }

    /// Classifies the connection, and marks a probe as sent when it asks for one.
    ///
    /// Asks for at most one ping per silent stretch: re-probing every poll would bury the
    /// server in pings precisely when it is already struggling.
    pub fn poll(&mut self, now: Instant) -> Liveness {
        let idle = self.idle_for(now);

        if idle >= self.idle_before_dead {
            return Liveness::Dead;
        }
        if idle >= self.idle_before_ping && !self.ping_in_flight {
            self.ping_in_flight = true;
            return Liveness::SendPing;
        }
        Liveness::Healthy
    }

    /// When the next poll could change the answer, so a runner can sleep until then.
    ///
    /// Returns `now` when the connection is already past the threshold, rather than a time
    /// in the past. A runner must therefore re-arm its timer from this after **every**
    /// poll — treating the value as a stable deadline and sleeping on it repeatedly would
    /// spin once the connection goes overdue.
    #[must_use]
    pub fn next_deadline(&self, now: Instant) -> Instant {
        let target =
            if self.ping_in_flight { self.idle_before_dead } else { self.idle_before_ping };
        self.last_seen + target.max(self.idle_for(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quiet_connection_is_probed_once_then_declared_dead() {
        let start = Instant::now();
        let mut policy = LivenessPolicy::with_thresholds(
            Duration::from_secs(25),
            Duration::from_secs(45),
            start,
        );

        assert_eq!(policy.poll(start), Liveness::Healthy);
        assert_eq!(policy.poll(start + Duration::from_secs(24)), Liveness::Healthy);
        assert_eq!(policy.poll(start + Duration::from_secs(25)), Liveness::SendPing);
        // Only one probe per silent stretch.
        assert_eq!(policy.poll(start + Duration::from_secs(26)), Liveness::Healthy);
        assert_eq!(policy.poll(start + Duration::from_secs(45)), Liveness::Dead);
    }

    #[test]
    fn any_inbound_frame_counts_as_liveness_not_just_a_pong() {
        // Both servers reset their heartbeat on any inbound message; mirroring that keeps a
        // busy bot from pinging a server it is already exchanging traffic with.
        let start = Instant::now();
        let mut policy = LivenessPolicy::with_thresholds(
            Duration::from_secs(25),
            Duration::from_secs(45),
            start,
        );

        assert_eq!(policy.poll(start + Duration::from_secs(24)), Liveness::Healthy);
        policy.saw_traffic(start + Duration::from_secs(24));
        // The clock that matters is now measured from the traffic, not from the start.
        assert_eq!(policy.idle_for(start + Duration::from_secs(48)), Duration::from_secs(24));
        assert_eq!(policy.poll(start + Duration::from_secs(48)), Liveness::Healthy);
        assert_eq!(policy.poll(start + Duration::from_secs(49)), Liveness::SendPing);
    }

    #[test]
    fn an_answered_ping_rearms_the_probe() {
        let start = Instant::now();
        let mut policy = LivenessPolicy::with_thresholds(
            Duration::from_secs(25),
            Duration::from_secs(45),
            start,
        );

        assert_eq!(policy.poll(start + Duration::from_secs(25)), Liveness::SendPing);
        policy.saw_traffic(start + Duration::from_secs(26));
        assert_eq!(policy.poll(start + Duration::from_secs(50)), Liveness::Healthy);
        assert_eq!(policy.poll(start + Duration::from_secs(51)), Liveness::SendPing);
    }

    #[test]
    fn death_wins_over_a_probe_when_both_are_due() {
        let start = Instant::now();
        let mut policy = LivenessPolicy::with_thresholds(
            Duration::from_secs(25),
            Duration::from_secs(45),
            start,
        );
        // Nothing polled in between, so the probe was never sent — it is still too late.
        assert_eq!(policy.poll(start + Duration::from_secs(60)), Liveness::Dead);
    }

    #[test]
    fn the_deadline_moves_out_after_a_probe_is_sent() {
        let start = Instant::now();
        let mut policy = LivenessPolicy::with_thresholds(
            Duration::from_secs(25),
            Duration::from_secs(45),
            start,
        );
        assert_eq!(policy.next_deadline(start), start + Duration::from_secs(25));
        policy.poll(start + Duration::from_secs(25));
        assert_eq!(
            policy.next_deadline(start + Duration::from_secs(25)),
            start + Duration::from_secs(45)
        );
    }

    #[test]
    #[should_panic(expected = "must leave time")]
    fn a_deadline_that_cannot_be_met_is_rejected() {
        let now = Instant::now();
        let _ =
            LivenessPolicy::with_thresholds(Duration::from_secs(30), Duration::from_secs(30), now);
    }
}
