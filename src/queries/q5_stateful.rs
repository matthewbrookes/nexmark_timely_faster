use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

#[derive(Deserialize, Serialize)]
struct Counts(HashMap<usize, usize>);

impl FasterRmw for Counts {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on Counts not allowed!");
    }
}

pub fn q5_stateful<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    let mut state = scope.get_state_handle().get_managed_map("q5-state");
    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                ((*b.date_time / window_slide_ns) + 1) * window_slide_ns,
            )
        })
        // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Q5 Accumulate",
            None,
            move |input, output, notificator, state_handle| {
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of this epoch
                    notificator.notify_at(
                        time.delayed(&(((time.time() / window_slide_ns) + 1) * window_slide_ns)),
                    );
                    // Notify when data exits window
                    notificator.notify_at(time.delayed(
                        &(((time.time() / window_slide_ns) + 1 + window_slice_count)
                            * window_slide_ns),
                    ));
                    data.swap(&mut buffer);
                    for &(auction, a_time) in buffer.iter() {
                        let mut counts: Counts =
                            state.remove(&a_time).unwrap_or(Counts(HashMap::new()));
                        let count = counts.0.entry(auction).or_insert(0);
                        *count += 1;
                        state.insert(a_time, counts);
                    }
                });

                notificator.for_each(|cap, _, notificator| {
                    let mut counts = HashMap::new();
                    for i in 0..window_slice_count {
                        if let Some(slide_counts) = state.get(&(cap.time() - i * window_slide_ns)) {
                            for (auction, count) in slide_counts.0.iter() {
                                *counts.entry(*auction).or_insert(0) += *count;
                            }
                        }
                    }
                    let mut counts_vec: Vec<_> = counts.iter().collect();
                    counts_vec.sort_by(|a, b| b.1.cmp(a.1));
                    if counts_vec.len() > 0 {
                        output.session(&cap).give(*counts_vec[0].0);
                    }
                });
            },
        )
}
