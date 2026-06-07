use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

const NAT_HOLE_PEER_LIMIT_PER_ROLE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NatHoleRole {
    Server,
    Visitor,
}

impl NatHoleRole {
    pub fn opposite(self) -> Self {
        match self {
            NatHoleRole::Server => NatHoleRole::Visitor,
            NatHoleRole::Visitor => NatHoleRole::Server,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatHolePeer {
    pub transaction_id: String,
    pub proxy_name: String,
    pub run_id: String,
    pub role: NatHoleRole,
    pub observed_addr: SocketAddr,
    pub local_addrs: Vec<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatHoleCandidate {
    pub transaction_id: String,
    pub proxy_name: String,
    pub peer_run_id: String,
    pub peer_role: NatHoleRole,
    pub observed_addr: SocketAddr,
    pub local_addrs: Vec<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NatHoleOutcome {
    Waiting,
    Matched(NatHoleCandidate),
}

#[derive(Debug)]
pub struct NatHoleController {
    ttl: Duration,
    cleanup_interval: Duration,
    last_cleanup: Mutex<Instant>,
    sessions: Mutex<HashMap<String, NatHoleSession>>,
}

#[derive(Debug)]
struct NatHoleSession {
    proxy_name: String,
    servers: Vec<NatHolePeerEntry>,
    visitors: Vec<NatHolePeerEntry>,
    next_server: usize,
    next_visitor: usize,
    updated_at: Instant,
}

#[derive(Debug)]
struct NatHolePeerEntry {
    peer: NatHolePeer,
    updated_at: Instant,
}

impl Default for NatHoleController {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl NatHoleController {
    pub fn new(ttl: Duration) -> Self {
        Self::new_with_cleanup_interval(ttl, Duration::from_secs(10))
    }

    fn new_with_cleanup_interval(ttl: Duration, cleanup_interval: Duration) -> Self {
        Self {
            ttl,
            cleanup_interval,
            last_cleanup: Mutex::new(Instant::now()),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, peer: NatHolePeer) -> Result<NatHoleOutcome> {
        if peer.transaction_id.trim().is_empty() {
            bail!("nat hole transaction_id is empty");
        }
        if peer.proxy_name.trim().is_empty() {
            bail!("nat hole proxy_name is empty");
        }

        let now = Instant::now();
        let should_cleanup = self.should_cleanup(now);
        let mut sessions = self.sessions.lock().expect("nathole mutex poisoned");
        if should_cleanup {
            Self::cleanup_locked(&mut sessions, self.ttl, now);
        }

        let session = sessions
            .entry(peer.transaction_id.clone())
            .or_insert_with(|| NatHoleSession {
                proxy_name: peer.proxy_name.clone(),
                servers: Vec::new(),
                visitors: Vec::new(),
                next_server: 0,
                next_visitor: 0,
                updated_at: Instant::now(),
            });

        if session.proxy_name != peer.proxy_name {
            bail!(
                "nat hole transaction {} already belongs to proxy {}",
                peer.transaction_id,
                session.proxy_name
            );
        }

        match peer.role {
            NatHoleRole::Server => Self::upsert_peer(
                &mut session.servers,
                &mut session.next_server,
                peer.clone(),
                now,
            ),
            NatHoleRole::Visitor => Self::upsert_peer(
                &mut session.visitors,
                &mut session.next_visitor,
                peer.clone(),
                now,
            ),
        }
        session.updated_at = now;

        Ok(
            Self::candidate_from_session(session, peer.role, Some(&peer.run_id), self.ttl, now)
                .map(NatHoleOutcome::Matched)
                .unwrap_or(NatHoleOutcome::Waiting),
        )
    }

    pub fn candidate_for(
        &self,
        transaction_id: &str,
        role: NatHoleRole,
    ) -> Option<NatHoleCandidate> {
        let now = Instant::now();
        let should_cleanup = self.should_cleanup(now);
        let mut sessions = self.sessions.lock().expect("nathole mutex poisoned");
        if should_cleanup {
            Self::cleanup_locked(&mut sessions, self.ttl, now);
        }
        let mut remove_session = false;
        let candidate = sessions.get_mut(transaction_id).and_then(|session| {
            let candidate = Self::candidate_from_session(session, role, None, self.ttl, now);
            remove_session = Self::session_is_empty_and_expired(session, self.ttl, now);
            candidate
        });
        if remove_session {
            sessions.remove(transaction_id);
        }
        candidate
    }

    pub fn remove(&self, transaction_id: &str) -> bool {
        self.sessions
            .lock()
            .expect("nathole mutex poisoned")
            .remove(transaction_id)
            .is_some()
    }

    pub fn remove_run_id(&self, run_id: &str) -> usize {
        let mut sessions = self.sessions.lock().expect("nathole mutex poisoned");
        let mut removed = 0;

        sessions.retain(|_, session| {
            removed += Self::remove_peers_by_run_id(&mut session.servers, run_id);
            removed += Self::remove_peers_by_run_id(&mut session.visitors, run_id);
            session.next_server = session.next_server.min(session.servers.len());
            session.next_visitor = session.next_visitor.min(session.visitors.len());
            !session.servers.is_empty() || !session.visitors.is_empty()
        });

        removed
    }

    fn upsert_peer(
        peers: &mut Vec<NatHolePeerEntry>,
        next: &mut usize,
        peer: NatHolePeer,
        now: Instant,
    ) {
        if let Some(existing) = peers
            .iter_mut()
            .find(|existing| existing.peer.run_id == peer.run_id)
        {
            existing.peer = peer;
            existing.updated_at = now;
        } else {
            if peers.len() >= NAT_HOLE_PEER_LIMIT_PER_ROLE {
                if let Some(oldest_index) = peers
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, entry)| entry.updated_at)
                    .map(|(index, _)| index)
                {
                    peers.remove(oldest_index);
                    if *next > oldest_index {
                        *next = (*next).saturating_sub(1);
                    }
                }
            }
            peers.push(NatHolePeerEntry {
                peer,
                updated_at: now,
            });
        }
        *next = (*next).min(peers.len());
    }

    fn remove_peers_by_run_id(peers: &mut Vec<NatHolePeerEntry>, run_id: &str) -> usize {
        let before = peers.len();
        peers.retain(|peer| peer.peer.run_id != run_id);
        before - peers.len()
    }

    fn candidate_from_session(
        session: &mut NatHoleSession,
        role: NatHoleRole,
        current_run_id: Option<&str>,
        ttl: Duration,
        now: Instant,
    ) -> Option<NatHoleCandidate> {
        Self::prune_session_locked(session, ttl, now);
        let (peers, next) = match role.opposite() {
            NatHoleRole::Server => (&session.servers, &mut session.next_server),
            NatHoleRole::Visitor => (&session.visitors, &mut session.next_visitor),
        };
        if peers.is_empty() {
            return None;
        }

        let mut selected = None;
        for _ in 0..peers.len() {
            let index = *next % peers.len();
            *next = (*next).wrapping_add(1);
            let peer = &peers[index].peer;
            if current_run_id
                .map(|run_id| run_id != peer.run_id)
                .unwrap_or(true)
            {
                selected = Some(peer);
                break;
            }
        }
        let peer = selected?;

        Some(NatHoleCandidate {
            transaction_id: peer.transaction_id.clone(),
            proxy_name: peer.proxy_name.clone(),
            peer_run_id: peer.run_id.clone(),
            peer_role: peer.role,
            observed_addr: peer.observed_addr,
            local_addrs: peer.local_addrs.clone(),
        })
    }

    fn should_cleanup(&self, now: Instant) -> bool {
        let mut last_cleanup = self
            .last_cleanup
            .lock()
            .expect("nathole cleanup mutex poisoned");
        if now.duration_since(*last_cleanup) < self.cleanup_interval {
            return false;
        }
        *last_cleanup = now;
        true
    }

    fn cleanup_locked(sessions: &mut HashMap<String, NatHoleSession>, ttl: Duration, now: Instant) {
        sessions.retain(|_, session| {
            Self::prune_session_locked(session, ttl, now);
            !Self::session_is_empty_and_expired(session, ttl, now)
        });
    }

    fn prune_session_locked(session: &mut NatHoleSession, ttl: Duration, now: Instant) {
        session
            .servers
            .retain(|peer| now.duration_since(peer.updated_at) <= ttl);
        session
            .visitors
            .retain(|peer| now.duration_since(peer.updated_at) <= ttl);
        session.next_server = session.next_server.min(session.servers.len());
        session.next_visitor = session.next_visitor.min(session.visitors.len());
    }

    fn session_is_empty_and_expired(session: &NatHoleSession, ttl: Duration, now: Instant) -> bool {
        session.servers.is_empty()
            && session.visitors.is_empty()
            && now.duration_since(session.updated_at) > ttl
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn matches_two_peers_for_same_transaction() {
        let controller = NatHoleController::default();

        let first = controller
            .register(NatHolePeer {
                transaction_id: "tx-1".to_string(),
                proxy_name: "xtcp-ssh".to_string(),
                run_id: "owner".to_string(),
                role: NatHoleRole::Server,
                observed_addr: addr(7001),
                local_addrs: vec![addr(10001)],
            })
            .unwrap();
        assert_eq!(first, NatHoleOutcome::Waiting);

        let second = controller
            .register(NatHolePeer {
                transaction_id: "tx-1".to_string(),
                proxy_name: "xtcp-ssh".to_string(),
                run_id: "visitor".to_string(),
                role: NatHoleRole::Visitor,
                observed_addr: addr(7002),
                local_addrs: vec![addr(10002)],
            })
            .unwrap();

        assert_eq!(
            second,
            NatHoleOutcome::Matched(NatHoleCandidate {
                transaction_id: "tx-1".to_string(),
                proxy_name: "xtcp-ssh".to_string(),
                peer_run_id: "owner".to_string(),
                peer_role: NatHoleRole::Server,
                observed_addr: addr(7001),
                local_addrs: vec![addr(10001)],
            })
        );

        let owner_candidate = controller
            .candidate_for("tx-1", NatHoleRole::Server)
            .expect("owner should see visitor candidate");
        assert_eq!(owner_candidate.peer_run_id, "visitor");
        assert_eq!(owner_candidate.observed_addr, addr(7002));
    }

    #[test]
    fn keeps_multiple_peers_per_role_for_shared_transaction() {
        let controller = NatHoleController::default();

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "sudp-group".to_string(),
                    run_id: "owner-a".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7001),
                    local_addrs: vec![addr(10001)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "sudp-group".to_string(),
                    run_id: "visitor-a".to_string(),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(7002),
                    local_addrs: vec![addr(10002)],
                })
                .unwrap(),
            NatHoleOutcome::Matched(NatHoleCandidate {
                transaction_id: "tx-group".to_string(),
                proxy_name: "sudp-group".to_string(),
                peer_run_id: "owner-a".to_string(),
                peer_role: NatHoleRole::Server,
                observed_addr: addr(7001),
                local_addrs: vec![addr(10001)],
            })
        );
        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "sudp-group".to_string(),
                    run_id: "visitor-b".to_string(),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(7003),
                    local_addrs: vec![addr(10003)],
                })
                .unwrap(),
            NatHoleOutcome::Matched(NatHoleCandidate {
                transaction_id: "tx-group".to_string(),
                proxy_name: "sudp-group".to_string(),
                peer_run_id: "owner-a".to_string(),
                peer_role: NatHoleRole::Server,
                observed_addr: addr(7001),
                local_addrs: vec![addr(10001)],
            })
        );

        let first = controller
            .candidate_for("tx-group", NatHoleRole::Server)
            .unwrap();
        let second = controller
            .candidate_for("tx-group", NatHoleRole::Server)
            .unwrap();
        assert_eq!(first.peer_run_id, "visitor-a");
        assert_eq!(second.peer_run_id, "visitor-b");
    }

    #[test]
    fn round_robins_multiple_server_peers_for_visitors() {
        let controller = NatHoleController::default();

        for (run_id, port) in [("owner-a", 7001), ("owner-b", 7002)] {
            assert_eq!(
                controller
                    .register(NatHolePeer {
                        transaction_id: "tx-group".to_string(),
                        proxy_name: "xtcp-group".to_string(),
                        run_id: run_id.to_string(),
                        role: NatHoleRole::Server,
                        observed_addr: addr(port),
                        local_addrs: vec![addr(port + 1000)],
                    })
                    .unwrap(),
                NatHoleOutcome::Waiting
            );
        }

        let first = controller
            .register(NatHolePeer {
                transaction_id: "tx-group".to_string(),
                proxy_name: "xtcp-group".to_string(),
                run_id: "visitor-a".to_string(),
                role: NatHoleRole::Visitor,
                observed_addr: addr(8001),
                local_addrs: vec![addr(9001)],
            })
            .unwrap();
        let second = controller
            .register(NatHolePeer {
                transaction_id: "tx-group".to_string(),
                proxy_name: "xtcp-group".to_string(),
                run_id: "visitor-b".to_string(),
                role: NatHoleRole::Visitor,
                observed_addr: addr(8002),
                local_addrs: vec![addr(9002)],
            })
            .unwrap();

        match (first, second) {
            (
                NatHoleOutcome::Matched(first_candidate),
                NatHoleOutcome::Matched(second_candidate),
            ) => {
                assert_eq!(first_candidate.peer_run_id, "owner-a");
                assert_eq!(second_candidate.peer_run_id, "owner-b");
            }
            other => panic!("unexpected outcomes: {other:?}"),
        }
    }

    #[test]
    fn bounds_peers_per_role_for_shared_transaction() {
        let controller = NatHoleController::default();

        for idx in 0..=NAT_HOLE_PEER_LIMIT_PER_ROLE {
            assert_eq!(
                controller
                    .register(NatHolePeer {
                        transaction_id: "tx-bounded".to_string(),
                        proxy_name: "xtcp-group".to_string(),
                        run_id: format!("owner-{idx}"),
                        role: NatHoleRole::Server,
                        observed_addr: addr(7000 + idx as u16),
                        local_addrs: vec![addr(10000 + idx as u16)],
                    })
                    .unwrap(),
                NatHoleOutcome::Waiting
            );
        }

        {
            let sessions = controller.sessions.lock().unwrap();
            let session = sessions.get("tx-bounded").unwrap();
            assert_eq!(session.servers.len(), NAT_HOLE_PEER_LIMIT_PER_ROLE);
            assert!(!session
                .servers
                .iter()
                .any(|entry| entry.peer.run_id == "owner-0"));
            assert!(session.servers.iter().any(|entry| {
                entry.peer.run_id == format!("owner-{NAT_HOLE_PEER_LIMIT_PER_ROLE}")
            }));
        }

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-bounded".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: "visitor".to_string(),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(8001),
                    local_addrs: vec![addr(9001)],
                })
                .unwrap(),
            NatHoleOutcome::Matched(NatHoleCandidate {
                transaction_id: "tx-bounded".to_string(),
                proxy_name: "xtcp-group".to_string(),
                peer_run_id: "owner-1".to_string(),
                peer_role: NatHoleRole::Server,
                observed_addr: addr(7001),
                local_addrs: vec![addr(10001)],
            })
        );

        for idx in 0..=NAT_HOLE_PEER_LIMIT_PER_ROLE {
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-bounded".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: format!("visitor-{idx}"),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(11000 + idx as u16),
                    local_addrs: vec![addr(12000 + idx as u16)],
                })
                .unwrap();
        }

        let sessions = controller.sessions.lock().unwrap();
        let session = sessions.get("tx-bounded").unwrap();
        assert_eq!(session.visitors.len(), NAT_HOLE_PEER_LIMIT_PER_ROLE);
        assert!(!session
            .visitors
            .iter()
            .any(|entry| entry.peer.run_id == "visitor-0"));
        assert!(session.visitors.iter().any(|entry| {
            entry.peer.run_id == format!("visitor-{NAT_HOLE_PEER_LIMIT_PER_ROLE}")
        }));
    }

    #[test]
    fn refreshing_existing_peer_does_not_evict_when_role_is_full() {
        let controller = NatHoleController::default();

        for idx in 0..NAT_HOLE_PEER_LIMIT_PER_ROLE {
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-refresh".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: format!("owner-{idx}"),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7000 + idx as u16),
                    local_addrs: vec![addr(10000 + idx as u16)],
                })
                .unwrap();
        }

        controller
            .register(NatHolePeer {
                transaction_id: "tx-refresh".to_string(),
                proxy_name: "xtcp-group".to_string(),
                run_id: "owner-0".to_string(),
                role: NatHoleRole::Server,
                observed_addr: addr(9000),
                local_addrs: vec![addr(13000)],
            })
            .unwrap();

        {
            let sessions = controller.sessions.lock().unwrap();
            let session = sessions.get("tx-refresh").unwrap();
            assert_eq!(session.servers.len(), NAT_HOLE_PEER_LIMIT_PER_ROLE);
            assert!(session.servers.iter().any(|entry| {
                entry.peer.run_id == "owner-0" && entry.peer.observed_addr == addr(9000)
            }));
        }

        controller
            .register(NatHolePeer {
                transaction_id: "tx-refresh".to_string(),
                proxy_name: "xtcp-group".to_string(),
                run_id: "owner-new".to_string(),
                role: NatHoleRole::Server,
                observed_addr: addr(9100),
                local_addrs: vec![addr(13100)],
            })
            .unwrap();

        let sessions = controller.sessions.lock().unwrap();
        let session = sessions.get("tx-refresh").unwrap();
        assert_eq!(session.servers.len(), NAT_HOLE_PEER_LIMIT_PER_ROLE);
        assert!(session
            .servers
            .iter()
            .any(|entry| entry.peer.run_id == "owner-0"));
        assert!(!session
            .servers
            .iter()
            .any(|entry| entry.peer.run_id == "owner-1"));
        assert!(session
            .servers
            .iter()
            .any(|entry| entry.peer.run_id == "owner-new"));
    }

    #[test]
    fn prunes_stale_peers_inside_active_shared_transaction() {
        let controller = NatHoleController::new(Duration::from_millis(200));

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: "owner-stale".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7001),
                    local_addrs: vec![addr(10001)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );

        thread::sleep(Duration::from_millis(90));

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: "owner-fresh".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7002),
                    local_addrs: vec![addr(10002)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );

        thread::sleep(Duration::from_millis(130));

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-group".to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: "visitor".to_string(),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(8001),
                    local_addrs: vec![addr(9001)],
                })
                .unwrap(),
            NatHoleOutcome::Matched(NatHoleCandidate {
                transaction_id: "tx-group".to_string(),
                proxy_name: "xtcp-group".to_string(),
                peer_run_id: "owner-fresh".to_string(),
                peer_role: NatHoleRole::Server,
                observed_addr: addr(7002),
                local_addrs: vec![addr(10002)],
            })
        );
    }

    #[test]
    fn does_not_match_stale_peer_between_global_cleanups() {
        let controller = NatHoleController::new_with_cleanup_interval(
            Duration::from_millis(80),
            Duration::from_secs(10),
        );

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-local-prune".to_string(),
                    proxy_name: "xtcp-prune".to_string(),
                    run_id: "owner-stale".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7001),
                    local_addrs: vec![addr(10001)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
        thread::sleep(Duration::from_millis(120));

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-local-prune".to_string(),
                    proxy_name: "xtcp-prune".to_string(),
                    run_id: "visitor".to_string(),
                    role: NatHoleRole::Visitor,
                    observed_addr: addr(8001),
                    local_addrs: vec![addr(9001)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
    }

    #[test]
    fn throttles_global_cleanup_of_unrelated_stale_transactions() {
        let controller = NatHoleController::new_with_cleanup_interval(
            Duration::from_millis(80),
            Duration::from_millis(120),
        );

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-stale-unrelated".to_string(),
                    proxy_name: "xtcp-prune".to_string(),
                    run_id: "owner-stale".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7001),
                    local_addrs: vec![addr(10001)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
        thread::sleep(Duration::from_millis(90));

        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-fresh".to_string(),
                    proxy_name: "xtcp-prune".to_string(),
                    run_id: "owner-fresh".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7002),
                    local_addrs: vec![addr(10002)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
        assert!(controller
            .sessions
            .lock()
            .unwrap()
            .contains_key("tx-stale-unrelated"));

        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            controller
                .register(NatHolePeer {
                    transaction_id: "tx-trigger-cleanup".to_string(),
                    proxy_name: "xtcp-prune".to_string(),
                    run_id: "owner-trigger".to_string(),
                    role: NatHoleRole::Server,
                    observed_addr: addr(7003),
                    local_addrs: vec![addr(10003)],
                })
                .unwrap(),
            NatHoleOutcome::Waiting
        );
        assert!(!controller
            .sessions
            .lock()
            .unwrap()
            .contains_key("tx-stale-unrelated"));
    }

    #[test]
    fn remove_run_id_prunes_matching_peers_and_empty_sessions() {
        let controller = NatHoleController::default();

        for (tx, run_id, role, port) in [
            ("tx-shared", "owner-gone", NatHoleRole::Server, 7001),
            ("tx-shared", "owner-live", NatHoleRole::Server, 7002),
            ("tx-shared", "visitor-gone", NatHoleRole::Visitor, 8001),
            ("tx-empty", "owner-gone", NatHoleRole::Server, 7003),
        ] {
            controller
                .register(NatHolePeer {
                    transaction_id: tx.to_string(),
                    proxy_name: "xtcp-group".to_string(),
                    run_id: run_id.to_string(),
                    role,
                    observed_addr: addr(port),
                    local_addrs: vec![addr(port + 1000)],
                })
                .unwrap();
        }

        assert_eq!(controller.remove_run_id("owner-gone"), 2);

        let candidate = controller
            .candidate_for("tx-shared", NatHoleRole::Visitor)
            .expect("remaining visitor should still see live owner");
        assert_eq!(candidate.peer_run_id, "owner-live");
        assert!(controller
            .candidate_for("tx-empty", NatHoleRole::Visitor)
            .is_none());

        assert_eq!(controller.remove_run_id("visitor-gone"), 1);
        assert!(controller
            .candidate_for("tx-shared", NatHoleRole::Server)
            .is_none());
        assert_eq!(controller.remove_run_id("missing"), 0);
    }
}
