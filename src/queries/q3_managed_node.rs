use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Filter, Operator};
use timely::dataflow::{Scope, Stream};
use timely::state::StateHandle;
use timely::state::backends::FASTERNodeBackend;
use timely::state::primitives::ManagedMap;

use crate::event::{Auction, Person};

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q3_managed_node<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    node_state_handle: &StateHandle<FASTERNodeBackend>
) -> Stream<S, (String, String, String, usize)> {
    let auctions = input.auctions(scope).filter(|a| a.category == 10);

    let people = input
        .people(scope)
        .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

    let mut auctions_buffer = vec![];
    let mut people_buffer = vec![];

    let mut state1: Box<ManagedMap<usize, Vec<Auction>>> =
        node_state_handle.get_managed_map("state1");
    let mut state2: Box<ManagedMap<usize, Person>> = node_state_handle.get_managed_map("state2");

    auctions.binary(
        &people,
        Exchange::new(|a: &Auction| a.seller as u64 / 100),
        Exchange::new(|p: &Person| p.id as u64 / 100),
        "Q3 Join",
        move |_capability, _info, _state_handle| {

            move |input1, input2, output| {
                // Process each input auction.
                input1.for_each(|time, data| {
                    data.swap(&mut auctions_buffer);
                    let mut session = output.session(&time);
                    for auction in auctions_buffer.drain(..) {
                        if let Some(person) = state2.get(&auction.seller) {
                            session.give((
                                person.name.clone(),
                                person.city.clone(),
                                person.state.clone(),
                                auction.id,
                            ));
                        }
                        let seller = auction.seller;
                        //state1.rmw(auction.seller, vec![auction]);
                        let mut seller_auctions = state1.remove(&seller).unwrap_or(Vec::new());
                        seller_auctions.push(auction);
                        state1.insert(seller, seller_auctions);
                    }
                });

                // Process each input person.
                input2.for_each(|time, data| {
                    data.swap(&mut people_buffer);
                    let mut session = output.session(&time);
                    for person in people_buffer.drain(..) {
                        if let Some(auctions) = state1.get(&person.id) {
                            for auction in auctions.iter() {
                                session.give((
                                    person.name.clone(),
                                    person.city.clone(),
                                    person.state.clone(),
                                    auction.id,
                                ));
                            }
                        }
                        state2.insert(person.id, person);
                    }
                });
            }
        },
    )
}
