// 食事する哲学者問題

use std::sync::Arc;
use std::{thread, time};

use crate::tl2::{ReadTrans, WriteTrans};
mod tl2;

#[macro_export]
macro_rules! load {
    ($t: ident, $a: expr) => {
        if let Some(v) = ($t).load($a) {
            v
        } else {
            return tl2::STMResult::Retry;
        }
    };
}

#[macro_export]
macro_rules! store {
    ($t: ident, $a: expr, $v: expr) => {
        $t.store($a, $v)
    };
}

const NUM_PHILOSOPHERS: usize = 8;

fn main() {
    let stm = Arc::new(tl2::STM::new());
    let mut to_be_joined = Vec::new();

    for i in 0..NUM_PHILOSOPHERS {
        let s = stm.clone();
        let th = std::thread::spawn(move || philosopher(s, i));
        to_be_joined.push(th);
    }

    let obs = std::thread::spawn(move || observer(stm));
    to_be_joined.push(obs);

    for th in to_be_joined {
        th.join().unwrap();
    }
}

fn philosopher(stm: Arc<tl2::STM>, n: usize) {
    // 箸用のメモリ
    let left = 8 * n;
    let right = 8 * ((n + 1) % NUM_PHILOSOPHERS);

    // 箸を拾う closure
    let pick_chopsticks = |tr: &mut WriteTrans<'_>| {
        let mut stick_left = load!(tr, left);
        let mut stick_right = load!(tr, right);
        if stick_left[0] == 0 && stick_right[0] == 0 {
            stick_left[0] = 1;
            stick_right[0] = 1;
            store!(tr, left, stick_left);
            store!(tr, right, stick_right);
            tl2::STMResult::Ok(true)
        } else {
            tl2::STMResult::Ok(false)
        }
    };

    // 箸を置く closure 
    let drop_chopsticks = |tr: &mut WriteTrans<'_>| {
        let mut stick_left = load!(tr, left);
        let mut stick_right = load!(tr, right);
        stick_left[0] = 0;
        stick_right[0] = 0;
        store!(tr, left, stick_left);
        store!(tr, right, stick_right);
        tl2::STMResult::Ok(())
    };

    for _ in 0..500000 {
        while !stm.write_transaction(pick_chopsticks).unwrap() {}      // 箸を拾えるまで繰り返す
        stm.write_transaction(drop_chopsticks);
    }
}

fn observer(stm: Arc<tl2::STM>) {
    for _ in 0..10000 {
        // 箸の状態を調べる closure
        let check_chopsticks = |tr: &mut ReadTrans<'_>| {
            let mut v = [0; NUM_PHILOSOPHERS];
            for i in 0..NUM_PHILOSOPHERS {
                v[i] = load!(tr, 8*i)[0];
            }

            tl2::STMResult::Ok(v)
        };

        let chopsticks = stm.read_transaction(check_chopsticks).unwrap();
        println!("{:?}", chopsticks);

        // 取り上げられている箸の数が奇数ならば、atomic でない -> panic
        let mut picked_up_chopsticks = 0;
        for c in &chopsticks {
            if *c == 1 {
                picked_up_chopsticks += 1;
            }
        }
        if picked_up_chopsticks & 1 != 0 {
            panic!("inconsistent");
        }

        // 一定時間ごとに観測
        let us = time::Duration::from_micros(100);
        thread::sleep(us);
    }
}