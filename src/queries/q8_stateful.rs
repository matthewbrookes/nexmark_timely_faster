use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use std::collections::HashSet;
use {crate::queries::NexmarkInput, crate::queries::NexmarkTimer};

pub fn q8_stateful<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(move |a| {
        (
            a.seller,
            ((*a.date_time / window_size_ns) + 1) * window_size_ns,
        )
    });

    let people = input
        .people(scope)
        .map(move |p| (p.id, ((*p.date_time / window_size_ns) + 1) * window_size_ns));

    let mut auction_state = scope.get_state_handle().get_managed_map("q8-auctions");
    let mut people_state = scope.get_state_handle().get_managed_map("q8-people");

    people.binary_notify(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        None,
        move |a_input, p_input, output, notificator, _| {
            let mut a_buffer = Vec::new();
            a_input.for_each(|time, data| {
                data.swap(&mut a_buffer);
                for &(a_id, a_time) in a_buffer.iter() {
                    notificator.notify_at(time.delayed(&a_time));
                    auction_state.rmw(a_time, vec![a_id]);
                }
            });

            let mut p_buffer = Vec::new();
            p_input.for_each(|time, data| {
                data.swap(&mut p_buffer);
                for &(p_id, p_time) in p_buffer.iter() {
                    notificator.notify_at(time.delayed(&p_time));
                    people_state.rmw(
                        p_time,
                        [p_id].iter().map(|x| *x).collect::<HashSet<usize>>(),
                    );
                }
            });

            notificator.for_each(|cap, _, _| {
                if let Some(auctions_in_window) = auction_state.remove(cap.time()) {
                    if let Some(new_people) = people_state.remove(cap.time()) {
                        let mut session = output.session(&cap);
                        for seller in auctions_in_window {
                            if new_people.contains(&seller) {
                                session.give(seller);
                            }
                        }
                    }
                }
            });
        },
    )
}
