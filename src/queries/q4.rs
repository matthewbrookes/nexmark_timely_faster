use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use faster_rs::FasterRmw;
use {crate::queries::NexmarkInput, crate::queries::NexmarkTimer};

#[derive(Serialize, Deserialize)]
struct SumWithCount(usize, usize);

impl FasterRmw for SumWithCount {
    fn rmw(&self, modification: Self) -> Self {
        SumWithCount(self.0 + modification.0, self.1 + modification.1)
    }
}

pub fn q4<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    input
        .closed_auctions(scope)
        .map(|(a, b)| (a.category, b.price))
        .unary(
            Exchange::new(|x: &(usize, usize)| x.0 as u64),
            "Q4 Average",
            |_cap, _info, state_handle| {
                // Stores category -> (total, count)
                let mut state = state_handle.get_managed_map("categories");

                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (category, price) in data.iter().cloned() {
                            state.rmw(category, SumWithCount(price, 1));
                            let entry = state.get(&category).unwrap();
                            session.give((category, entry.0 / entry.1));
                        }
                    })
                }
            },
        )
}
