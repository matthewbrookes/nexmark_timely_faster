use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, Stream};
use timely::state::primitives::ManagedMap;

use crate::event::{Auction, Bid};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterValue;

#[derive(Serialize, Deserialize)]
struct AuctionBids(Option<Auction>, Vec<Bid>);

impl FasterValue for AuctionBids {
    fn rmw(&self, modification: Self) -> Self {
        unimplemented!()
    }
}

pub fn q4_q6_common<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (Auction, Bid)> {
    let bids = input.bids(scope);
    let auctions = input.auctions(scope);

    bids.binary_frontier(
        &auctions,
        Exchange::new(|b: &Bid| b.auction as u64),
        Exchange::new(|a: &Auction| a.id as u64),
        "Q4 Auction close",
        |_capability, _info, state_handle| {
            let mut state: Box<ManagedMap<usize, AuctionBids>> =
                state_handle.get_managed_map("state");
            let mut opens = std::collections::BinaryHeap::new();

            let mut capability: Option<Capability<usize>> = None;
            use std::cmp::Reverse;

            fn is_valid_bid(bid: &Bid, auction: &Auction) -> bool {
                bid.price >= auction.reserve
                    && auction.date_time <= bid.date_time
                    && bid.date_time < auction.expires
            }

            move |input1, input2, output| {
                // Record each bid.
                // NB: We don't summarize as the max, because we don't know which are valid.
                input1.for_each(|time, data| {
                    for bid in data.iter().cloned() {
                        //                                        eprintln!("[{:?}] bid: {:?}", time.time().inner, bid);
                        let id = bid.auction;
                        let mut entry = state
                            .remove(&bid.auction)
                            .unwrap_or(AuctionBids(None, Vec::new()));
                        if let Some(ref auction) = entry.0 {
                            debug_assert!(entry.1.len() <= 1);
                            if is_valid_bid(&bid, auction) {
                                // bid must fall between auction creation and expiration
                                if let Some(existing) = entry.1.get(0).cloned() {
                                    if existing.price < bid.price {
                                        entry.1[0] = bid;
                                    }
                                } else {
                                    entry.1.push(bid);
                                }
                            }
                        } else {
                            opens.push((Reverse(bid.date_time), bid.auction));
                            if capability
                                .as_ref()
                                .map(|c| nt.to_nexmark_time(*c.time()) <= bid.date_time)
                                != Some(true)
                            {
                                capability =
                                    Some(time.delayed(&nt.from_nexmark_time(bid.date_time)));
                            }
                            entry.1.push(bid);
                        }
                        state.insert(id, entry);
                    }
                });

                // Record each auction.
                input2.for_each(|time, data| {
                    for auction in data.iter().cloned() {
                        let id = auction.id;
                        //                                        eprintln!("[{:?}] auction: {:?}", time.time().inner, auction);
                        if capability
                            .as_ref()
                            .map(|c| nt.to_nexmark_time(*c.time()) <= auction.expires)
                            != Some(true)
                        {
                            capability = Some(time.delayed(&nt.from_nexmark_time(auction.expires)));
                        }
                        opens.push((Reverse(auction.expires), auction.id));
                        let mut entry = state
                            .remove(&auction.id)
                            .unwrap_or(AuctionBids(None, Vec::new()));
                        debug_assert!(entry.0.is_none());
                        entry.1.retain(|bid| is_valid_bid(&bid, &auction));
                        if let Some(bid) = entry.1.iter().max_by_key(|bid| bid.price).cloned() {
                            entry.1.clear();
                            entry.1.push(bid);
                        }
                        entry.0 = Some(auction);
                        state.insert(id, entry);
                    }
                });

                // Use frontiers to determine which auctions to close.
                if let Some(ref capability) = capability {
                    let complete1 = input1
                        .frontier
                        .frontier()
                        .get(0)
                        .cloned()
                        .unwrap_or(usize::max_value());
                    let complete2 = input2
                        .frontier
                        .frontier()
                        .get(0)
                        .cloned()
                        .unwrap_or(usize::max_value());
                    let complete = std::cmp::min(complete1, complete2);

                    let mut session = output.session(capability);
                    while opens.peek().map(|x| {
                        complete == usize::max_value() || (x.0).0 < nt.to_nexmark_time(complete)
                    }) == Some(true)
                    {
                        //                                        eprintln!("[{:?}] opens.len(): {} state.len(): {} {:?}", capability.time().inner, opens.len(), state.len(), state.iter().map(|x| (x.1).1.len()).sum::<usize>());

                        let (Reverse(time), auction) = opens.pop().unwrap();
                        if let Some(mut auction_bids) = state.remove(&auction) {
                            let delete = {
                                if let Some(ref auction) = auction_bids.0 {
                                    if time == auction.expires {
                                        // Auction expired, clean up state
                                        if let Some(winner) = auction_bids.1.pop() {
                                            session.give((auction.clone(), winner));
                                        }
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    auction_bids.1.retain(|bid| bid.date_time > time);
                                    auction_bids.1.is_empty()
                                }
                            };
                            if !delete {
                                state.insert(auction, auction_bids);
                            }
                        }
                    }
                }

                // Downgrade capability.
                if let Some(head) = opens.peek() {
                    capability
                        .as_mut()
                        .map(|c| c.downgrade(&nt.from_nexmark_time((head.0).0)));
                } else {
                    capability = None;
                }
            }
        },
    )
}
