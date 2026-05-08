use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

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
    sessions: Mutex<HashMap<String, NatHoleSession>>,
}

#[derive(Debug)]
struct NatHoleSession {
    proxy_name: String,
    server: Option<NatHolePeer>,
    visitor: Option<NatHolePeer>,
    updated_at: Instant,
}

impl Default for NatHoleController {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl NatHoleController {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
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

        let mut sessions = self.sessions.lock().expect("nathole mutex poisoned");
        Self::cleanup_locked(&mut sessions, self.ttl);

        let session = sessions
            .entry(peer.transaction_id.clone())
            .or_insert_with(|| NatHoleSession {
                proxy_name: peer.proxy_name.clone(),
                server: None,
                visitor: None,
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
            NatHoleRole::Server => session.server = Some(peer.clone()),
            NatHoleRole::Visitor => session.visitor = Some(peer.clone()),
        }
        session.updated_at = Instant::now();

        Ok(Self::candidate_from_session(session, peer.role)
            .map(NatHoleOutcome::Matched)
            .unwrap_or(NatHoleOutcome::Waiting))
    }

    pub fn candidate_for(
        &self,
        transaction_id: &str,
        role: NatHoleRole,
    ) -> Option<NatHoleCandidate> {
        let mut sessions = self.sessions.lock().expect("nathole mutex poisoned");
        Self::cleanup_locked(&mut sessions, self.ttl);
        sessions
            .get(transaction_id)
            .and_then(|session| Self::candidate_from_session(session, role))
    }

    pub fn remove(&self, transaction_id: &str) -> bool {
        self.sessions
            .lock()
            .expect("nathole mutex poisoned")
            .remove(transaction_id)
            .is_some()
    }

    fn candidate_from_session(
        session: &NatHoleSession,
        role: NatHoleRole,
    ) -> Option<NatHoleCandidate> {
        let peer = match role.opposite() {
            NatHoleRole::Server => session.server.as_ref()?,
            NatHoleRole::Visitor => session.visitor.as_ref()?,
        };

        Some(NatHoleCandidate {
            transaction_id: peer.transaction_id.clone(),
            proxy_name: peer.proxy_name.clone(),
            peer_run_id: peer.run_id.clone(),
            peer_role: peer.role,
            observed_addr: peer.observed_addr,
            local_addrs: peer.local_addrs.clone(),
        })
    }

    fn cleanup_locked(sessions: &mut HashMap<String, NatHoleSession>, ttl: Duration) {
        let now = Instant::now();
        sessions.retain(|_, session| now.duration_since(session.updated_at) <= ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
