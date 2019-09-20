use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use crate::event::Date;
use super::maybe_refresh_faster;
use faster_rs::{FasterKv, status};
use tempfile::TempDir;

pub fn q8<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(|a| (a.seller, a.date_time));

    let people = input.auctions(scope).map(|p| (p.id, p.date_time));

    people.binary_frontier(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        |_capability, _info| {
            let new_people_dir = TempDir::new_in(".").expect("Unable to create FASTER directory");
            let new_people = FasterKv::new_u64_store(1 << 24, 3 * 1024 * 1024 * 1024, new_people_dir.into_path().to_str().unwrap().to_owned()).unwrap();
            new_people.start_session();
            let mut new_people_serial = 0;
            let mut auctions: Vec<(usize, Vec<(usize, Date)>)> = Vec::new();

            let mut delayed = None;

            move |input1, input2, output| {
                // Notice new people.
                input1.for_each(|time, data| {
                    if delayed.is_none() {
                        delayed = Some(time.delayed(&time.time()));
                    }
                    for (person, time) in data.iter().cloned() {
                        new_people.upsert_u64(person as u64, nt.from_nexmark_time(time) as u64, new_people_serial);
                        maybe_refresh_faster(&new_people, &mut new_people_serial);
                    }
                });

                // Notice new auctions.
                input2.for_each(|time, data| {
                    if delayed.is_none() {
                        delayed = Some(time.delayed(&time.time()));
                    }
                    let mut data_vec = vec![];
                    data.swap(&mut data_vec);
                    auctions.push((*time.time(), data_vec));
                });

                // Determine least timestamp we might still see.
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

                for (auction_time, auctions) in auctions.iter_mut() {
                    if *auction_time < complete {
                        {
                            //let mut session = output.session(&capability);
                            let mut delayed = delayed.as_ref().unwrap().delayed(auction_time);
                            let mut session = output.session(&delayed);
                            for &(person, time) in auctions.iter() {
                                if time < nt.to_nexmark_time(complete) {
                                    let (res, recv) = new_people.read_u64(person as u64, new_people_serial);
                                    if res == status::PENDING {
                                        new_people.complete_pending(true);
                                    }
                                    maybe_refresh_faster(&new_people, &mut new_people_serial);
                                    if let Ok(p_time) = recv.recv() {
                                        if *time < (p_time as usize) + window_size_ns {
                                            session.give(person);
                                        }
                                    }
                                }
                            }
                            auctions.retain(|&(_, time)| time >= nt.to_nexmark_time(complete));
                        }
                        if let Some(minimum) = auctions.iter().map(|x| x.1).min() {
                            //capability.downgrade(&nt.from_nexmark_time(minimum));
                        }
                    }
                }
                delayed.as_mut().map(|cap| cap.downgrade(&complete));
                if complete == usize::max_value() {
                    delayed = None;
                }
                auctions.retain(|&(_, ref list)| !list.is_empty());
                // println!("auctions.len: {:?}", auctions.len());
                // for thing in auctions.iter() {
                //     println!("\t{:?} (len: {:?}) vs {:?}", thing.0, thing.1.len(), complete);
                // }
            }
        },
    )
}
