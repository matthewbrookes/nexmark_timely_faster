use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::event::Date;

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q5<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    let mut additions = HashMap::new();
    let mut deletions = HashMap::new();
    let mut accumulations = HashMap::new();

    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns),
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Q5 Accumulate",
            None,
            move |input, output, notificator, _state_handle| {
                let mut bids_buffer = vec![];
                input.for_each(|time, data| {
                    data.swap(&mut bids_buffer);
                    let slide = Date::new(
                        ((*nt.to_nexmark_time(*time.time()) / window_slide_ns) + 1)
                            * window_slide_ns,
                    );
                    let downgrade = time.delayed(&nt.from_nexmark_time(slide));
                    notificator.notify_at(downgrade.clone());

                    // Collect all bids in a different slide.
                    for &(auction, a_time) in bids_buffer.iter() {
                        if a_time != slide {
                            additions
                                .entry(time.delayed(&nt.from_nexmark_time(a_time)))
                                .or_insert_with(Vec::new)
                                .push(auction);
                            notificator.notify_at(time.delayed(&nt.from_nexmark_time(a_time)));
                        }
                    }
                    bids_buffer.retain(|&(_, a_time)| a_time == slide);

                    // Collect all bids in the same slide.
                    additions
                        .entry(downgrade)
                        .or_insert_with(Vec::new)
                        .extend(bids_buffer.drain(..).map(|(b, _)| b));
                });

                notificator.for_each(|time, _, notificator| {
                    if let Some(additions) = additions.remove(&time) {
                        for &auction in additions.iter() {
                            *accumulations.entry(auction).or_insert(0) += 1;
                        }
                        let new_time = time.time() + (window_slice_count * window_slide_ns);
                        deletions.insert(time.delayed(&new_time), additions);
                        notificator.notify_at(time.delayed(&new_time));
                    }
                    if let Some(deletions) = deletions.remove(&time) {
                        for auction in deletions.into_iter() {
                            use std::collections::hash_map::Entry;
                            match accumulations.entry(auction) {
                                Entry::Occupied(mut entry) => {
                                    *entry.get_mut() -= 1;
                                    if *entry.get_mut() == 0 {
                                        entry.remove();
                                    }
                                }
                                _ => panic!("entry has to exist"),
                            }
                        }
                    }
                    if let Some((count, auction)) =
                        accumulations.iter().map(|(&a, &c)| (c, a)).max()
                    {
                        output.session(&time).give((auction, count));
                    }
                })
            },
        )
        .unary_frontier(
            Exchange::new(|_| 0),
            "Q5 All-Accumulate",
            |_cap, _info, _state_handle| {
                let mut hot_items = HashMap::new();

                let mut buffer = Vec::new();
                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut buffer);
                        let current_hottest = hot_items.entry(time.retain()).or_insert((0, 0));
                        for &(auction, count) in buffer.iter() {
                            if count > current_hottest.1 {
                                *current_hottest = (auction, count);
                            }
                        }
                    });

                    for (time, (auction, _count)) in hot_items.iter() {
                        if !input.frontier.less_than(time.time()) {
                            output.session(&time).give(*auction);
                        }
                    }

                    hot_items.retain(move |time, _| input.frontier.less_than(time.time()));
                }
            },
        )
}
