use frprs::nathole::{
    NatHoleCandidate, NatHoleController, NatHoleOutcome, NatHolePeer, NatHoleRole,
};
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[test]
fn nat_hole_controller_exchanges_candidates_between_roles() {
    let controller = NatHoleController::new(Duration::from_secs(60));

    let owner = NatHolePeer {
        transaction_id: "tx-e2e".to_string(),
        proxy_name: "xtcp-api".to_string(),
        run_id: "owner-run".to_string(),
        role: NatHoleRole::Server,
        observed_addr: addr(47001),
        local_addrs: vec![addr(57001)],
    };
    let visitor = NatHolePeer {
        transaction_id: "tx-e2e".to_string(),
        proxy_name: "xtcp-api".to_string(),
        run_id: "visitor-run".to_string(),
        role: NatHoleRole::Visitor,
        observed_addr: addr(47002),
        local_addrs: vec![addr(57002)],
    };

    assert_eq!(controller.register(owner).unwrap(), NatHoleOutcome::Waiting);
    assert_eq!(
        controller.register(visitor).unwrap(),
        NatHoleOutcome::Matched(NatHoleCandidate {
            transaction_id: "tx-e2e".to_string(),
            proxy_name: "xtcp-api".to_string(),
            peer_run_id: "owner-run".to_string(),
            peer_role: NatHoleRole::Server,
            observed_addr: addr(47001),
            local_addrs: vec![addr(57001)],
        })
    );

    let owner_candidate = controller
        .candidate_for("tx-e2e", NatHoleRole::Server)
        .expect("owner should receive visitor candidate");
    assert_eq!(owner_candidate.peer_run_id, "visitor-run");
    assert_eq!(owner_candidate.observed_addr, addr(47002));
}
