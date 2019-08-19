use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Filter, Operator};
use timely::dataflow::{Scope, Stream};
use timely::state::backends::FASTERBackend;
use timely::state::primitives::ManagedMap;

use crate::event::{Auction, Person};

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q3<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (String, String, String, usize)> {
    let auctions = input.auctions(scope).filter(|a| a.category == 10);

    let people = input
        .people(scope)
        .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

    let mut auctions_buffer = vec![];
    let mut people_buffer = vec![];

    auctions.binary(
        &people,
        Exchange::new(|a: &Auction| a.seller as u64 / 100),
        Exchange::new(|p: &Person| p.id as u64 / 100),
        "Q3 Join",
        |_capability, _info, state_handle| {
            let mut auctions_state = HashMap::new();
            let mut people_state   = HashMap::new();

            move |input1, input2, output| {
                // Process each input auction.
                input1.for_each(|time, data| {
                    data.swap(&mut auctions_buffer);
                    let mut session = output.session(&time);
                    for auction in auctions_buffer.drain(..) {
                        let maybe_person: Option<&Person> = people_state.get(&auction.seller);
                        if let Some(person) = maybe_person {
                            session.give((
                                person.name.clone(),
                                person.city.clone(),
                                person.state.clone(),
                                auction.id,
                            ));
                        }
                        let seller = auction.seller;
                        let mut auctions = auctions_state.remove(&seller).unwrap_or(Vec::new());
                        auctions.push(auction);
                        auctions_state.insert(seller, auctions);
                    }
                });

                // Process each input person.
                input2.for_each(|time, data| {
                    data.swap(&mut people_buffer);
                    let mut session = output.session(&time);
                    for person in people_buffer.drain(..) {
                        if let Some(auctions) = auctions_state.get(&person.id) {
                            for auction in auctions.iter() {
                                session.give((
                                    person.name.clone(),
                                    person.city.clone(),
                                    person.state.clone(),
                                    auction.id,
                                ));
                            }
                        }
                        people_state.insert(person.id, person);
                    }
                });
            }
        },
    )
}
