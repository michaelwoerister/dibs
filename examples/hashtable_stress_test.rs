
extern crate dibs;
extern crate rand;

use dibs::*;
use rand::{thread_rng, random, Rng};
use std::collections::HashMap;

fn main() {
    let (mut allocator, mut storage) = create_allocator_and_storage();

    let mut reference = HashMap::new();
    let mut table = HashTable::new();

    let mut next_table_size_to_report = 100;

    for iteration in 0 .. 1000000 {
        let action: u8 = random();
        let key: u16 = thread_rng().gen_range(0, 666);
        let key: [u8; 2] = [key as u8, (key >> 8) as u8];
        match action {
            0 ... 180 => {
                // Insert
                let value: [u8; 3] = [random(), random(), random()];

                reference.insert(key[..].to_owned(), value[..].to_owned());
                table.insert(&key, &value, &mut storage, &mut allocator);
            }
            181 ... 255 => {
                reference.remove(&key[..]);
                table.remove(&key, &mut storage, &mut allocator);
            }
            _ => unreachable!()
        }

        table.sanity_check_table();

        for (key, value) in reference.iter() {
            assert_eq!(table.find(&key[..], &storage), Some(&value[..]));
        }

        let mut data = HashMap::with_capacity(reference.len());
        table.iter(&storage, |key, value| {
            data.insert(key.to_owned(), value.to_owned());
        });

        let mut reference: Vec<(&Vec<u8>, &Vec<u8>)> = reference
            .iter()
            .collect();
        reference.sort_by_key(|&(k, _)| k);

        let mut data: Vec<(&Vec<u8>, &Vec<u8>)> = data
            .iter()
            .collect();
        data.sort_by_key(|&(k, _)| k);

        assert_eq!(reference, data);

        if (iteration + 1) % 50000 == 0 {
            println!("tested {} operations, table size = {}", iteration + 1, table.len());
        }

        if table.len() >= next_table_size_to_report {
            println!("table size = {}", table.len());
            next_table_size_to_report += 100;
        }
    }
}

fn create_allocator_and_storage() -> (Allocator, MemStore) {
    let mut allocator = Allocator::new(Size(10000000));
    let storage = MemStore::new(10000000);
    // Make sure we reserve the Null address.
    allocator.alloc(Size(10));

    (allocator, storage)
}
