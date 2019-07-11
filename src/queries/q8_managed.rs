use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q8_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(|a| (a.seller, a.date_time));

    let people = input.auctions(scope).map(|p| (p.id, p.date_time));

    let mut new_people = scope.get_state_handle().get_managed_map("q8-new_people");
    let mut auctions_state = scope.get_state_handle().get_managed_value("q8-auctions");

    auctions_state.set(Vec::new());

    people.binary_notify(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        None,
        move |input1, input2, output, notificator, _| {
            // Notice new people.
            input1.for_each(|time, data| {
                notificator.notify_at(time.retain());
                for (person, p_time) in data.iter().cloned() {
                    new_people.insert(person, p_time);
                }
            });

            // Notice new auctions.
            input2.for_each(|time, data| {
                let mut data_vec = vec![];
                data.swap(&mut data_vec);
                auctions_state.rmw(vec![(*time.time(), data_vec)]);
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|cap, _, _| {
                let mut auctions_vec = auctions_state.take().unwrap_or(Vec::new());
                for (capability_time, auctions) in auctions_vec.iter_mut() {
                    if *capability_time <= *cap.time() {
                        let mut session = output.session(&cap);
                        for &(person, time) in auctions.iter() {
                            if time < nt.to_nexmark_time(*cap.time()) {
                                if let Some(p_time) = new_people.get(&person) {
                                    if *time < **p_time + window_size_ns {
                                        session.give(person);
                                    }
                                }
                            }
                        }
                    }
                    auctions.retain(|&(_, time)| time >= nt.to_nexmark_time(*cap.time()));
                }
                auctions_vec.retain(|&(_, ref list)| !list.is_empty());
                auctions_state.set(auctions_vec);
            });
        },
    )
}
