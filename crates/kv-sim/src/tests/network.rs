use kv_raft::{Message, NodeId};

use crate::clock::SimRng;
use crate::network::{Envelope, Network, NetworkConfig};

fn heartbeat(from: NodeId, to: NodeId) -> Envelope {
    Envelope {
        from,
        to,
        msg: Message::AppendEntries {
            term: 1,
            leader_id: from,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    }
}

/// Sends `count` messages and drains the network, returning how many arrived.
fn deliver_all(net: &mut Network, rng: &mut SimRng, count: usize, max_tick: u64) -> usize {
    for _ in 0..count {
        net.send(0, rng, heartbeat(1, 2));
    }
    (0..=max_tick).map(|t| net.take_due(t).len()).sum()
}

#[test]
fn a_perfect_network_delivers_everything_exactly_once() {
    let mut net = Network::new(NetworkConfig::default());
    let mut rng = SimRng::new(1);
    assert_eq!(deliver_all(&mut net, &mut rng, 50, 4), 50);
    assert_eq!(net.in_flight_count(), 0);
}

#[test]
fn nothing_is_delivered_before_its_time() {
    let mut net = Network::new(NetworkConfig { max_delay_ticks: 5, ..Default::default() });
    let mut rng = SimRng::new(1);
    net.send(0, &mut rng, heartbeat(1, 2));
    assert!(net.take_due(0).is_empty(), "a message sent at tick 0 cannot arrive at tick 0");
    assert_eq!(net.in_flight_count(), 1);
}

#[test]
fn drops_land_near_the_configured_rate() {
    let mut net = Network::new(NetworkConfig { drop_percent: 20, ..Default::default() });
    let mut rng = SimRng::new(9);
    let arrived = deliver_all(&mut net, &mut rng, 10_000, 4);
    assert!((7500..8500).contains(&arrived), "20% loss of 10k should leave ~8000, got {arrived}");
}

#[test]
fn duplicates_deliver_the_same_message_twice() {
    let mut net = Network::new(NetworkConfig {
        duplicate_percent: 100,
        max_delay_ticks: 3,
        ..Default::default()
    });
    let mut rng = SimRng::new(4);
    assert_eq!(deliver_all(&mut net, &mut rng, 100, 8), 200, "every message should be doubled");
}

#[test]
fn delay_reorders_messages() {
    let mut net = Network::new(NetworkConfig { max_delay_ticks: 20, ..Default::default() });
    let mut rng = SimRng::new(11);
    // Tag each message by its destination so arrival order is observable.
    for to in 1..=40u64 {
        net.send(0, &mut rng, heartbeat(0, to));
    }
    let arrival: Vec<NodeId> = (0..=25).flat_map(|t| net.take_due(t)).map(|e| e.to).collect();

    assert_eq!(arrival.len(), 40);
    let sent: Vec<NodeId> = (1..=40).collect();
    assert_ne!(
        arrival, sent,
        "with delays up to 20 ticks, arrival order must differ from send order"
    );
}

#[test]
fn a_partition_blocks_traffic_across_it_and_allows_it_within() {
    let mut net = Network::new(NetworkConfig::default());
    let mut rng = SimRng::new(2);
    net.partition(&[&[1, 2], &[3]]);

    net.send(0, &mut rng, heartbeat(1, 3));
    net.send(0, &mut rng, heartbeat(1, 2));

    let arrived = net.take_due(1);
    assert_eq!(arrived.len(), 1, "only the within-group message may arrive");
    assert_eq!(arrived[0].to, 2);
}

#[test]
fn a_partition_raised_mid_flight_still_blocks_the_message() {
    // Filtering only at send time would let a message cross a partition by
    // happening to be in flight when it went up.
    let mut net = Network::new(NetworkConfig { max_delay_ticks: 10, ..Default::default() });
    let mut rng = SimRng::new(5);
    net.send(0, &mut rng, heartbeat(1, 3));
    net.partition(&[&[1, 2], &[3]]);

    let arrived: usize = (0..=12).map(|t| net.take_due(t).len()).sum();
    assert_eq!(arrived, 0, "the partition went up before delivery, so nothing may arrive");
}

#[test]
fn healing_restores_delivery() {
    let mut net = Network::new(NetworkConfig::default());
    let mut rng = SimRng::new(6);
    net.partition(&[&[1], &[2]]);
    net.heal();
    net.send(0, &mut rng, heartbeat(1, 2));
    assert_eq!(net.take_due(1).len(), 1);
}

#[test]
fn the_same_seed_produces_the_same_delivery_schedule() {
    let run = |seed| {
        let mut net = Network::new(NetworkConfig {
            drop_percent: 20,
            duplicate_percent: 10,
            max_delay_ticks: 8,
        });
        let mut rng = SimRng::new(seed);
        for to in 1..=100u64 {
            net.send(0, &mut rng, heartbeat(0, to));
        }
        (0..=12)
            .map(|t| net.take_due(t).into_iter().map(|e| e.to).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(run(77), run(77), "an unreliable network must still be a function of its seed");
    assert_ne!(run(77), run(78));
}
