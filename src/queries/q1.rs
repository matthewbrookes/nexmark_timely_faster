use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};

use crate::event::Bid;

use {crate::queries::NexmarkInput, crate::queries::NexmarkTimer};

pub fn q1<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, Bid> {
    input
        .bids(scope)
        .map_in_place(|b| b.price = (b.price * 89) / 100)
}
