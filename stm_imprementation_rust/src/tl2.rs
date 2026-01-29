use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{fence, AtomicU64};
use std::sync::atomic::Ordering::{Relaxed, Release, Acquire, AcqRel, SeqCst};

// software transactional memory の TL2 実装
// todo: global_version_clock のオーバーフロー対策
// todo: オブジェクト単位での管理 => Garbage Collection
// todo: ライブロック回避のためのアクセス数制限 (Semaphore など)

const STRIPE_SIZE: usize = 8;   //   8 byte (2^n でなければならない)
const MEM_SIZE: usize = 512;    // 512 byte (2^n でなければならない)
// MEM_SIZE / STRIPE_SIZE 個のストライプを使用可能

pub struct Memory {
    mem: Vec<u8>,
    lock_ver: Vec<AtomicU64>,   // ストライプのロックとバージョン
    global_clock: AtomicU64,    
    shift_size: u32,            // メモリアドレスからストライプ番号への変換に用いる
}

impl Memory {
    pub fn new() -> Self {
        let mem = [0].repeat(MEM_SIZE);     // 全体のメモリを確保
        let shift = STRIPE_SIZE.trailing_zeros();   // (2^n).trailing_zeros() = n
        let mut lock_ver = Vec::new();
        for _ in 0..(MEM_SIZE >> shift) {       // 使用可能なストライプの個数
            lock_ver.push(AtomicU64::new(0));
        }

        Memory { 
            mem: mem, 
            lock_ver: lock_ver, 
            global_clock: AtomicU64::new(0), 
            shift_size: shift,
        }
    }

    // subroutines
    // global_clock を +1 してその値を返す
    fn inc_global_clock(&mut self) -> u64 {
        self.global_clock.fetch_add(1, AcqRel) + 1
    }

    // 対象のアドレスの version を取得
    fn get_version(&self, addr: usize) -> u64 {
        let stripe = addr >> self.shift_size;               // ストライプの index
        let n = self.lock_ver[stripe].load(Relaxed);    // version 値
        return n & !(1 << 63);      // 最上位 bit を落とす (最上位 bit は lock 用 bit として用いる)
    }

    // ロックされておらず、かつ addr の指す stripe の version: n が version 以下である (modify されていない) かどうか
    fn test_not_modify(&self, addr: usize, version: u64) -> bool {
        let stripe = addr >> self.shift_size;               // ストライプの index
        let n = self.lock_ver[stripe].load(Relaxed);    // version 値
        return n <= version;        // lock されていれば最上位 bit が on になるため、このように簡単に判別できる
    }

    // 対象アドレスのロックの獲得を試みる
    fn lock_addr(&mut self, addr: usize) -> bool {
        let stripe = addr >> self.shift_size;       // ストライプの index
        let lock_bit_setter = |val: u64| {
            let lock_bit = val & (1 << 63);
            if lock_bit == 0 {      // lock bit が設定されていない -> 設定
                Some(val | (1 << 63))   
            } else {                // lock bit が設定されている -> lock 失敗
                None
            }
        };
        // lock bit が設定されていなければ、設定して true を返す; 設定されていれば、false を返す
        match self.lock_ver[stripe].fetch_update(Relaxed, Relaxed, lock_bit_setter) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn unlock_addr(&mut self, addr: usize) {
        let stripe = addr >> self.shift_size;       // ストライプの index
        self.lock_ver[stripe].fetch_and(!(1 << 63), Relaxed);   // lock bit 消去
    }
}

pub struct ReadTrans<'a> {      // 読み込みトランザクション (= クリティカルセクションの読み込み) 時に作成  
    read_version: u64,
    conflict: bool,             // 競合発生中かどうか
    mem: &'a Memory,
}

impl<'a> ReadTrans<'a> {
    fn new(mem: &'a Memory) -> Self {
        ReadTrans { 
            read_version: mem.global_clock.load(Acquire),   // global_clock を copy
            conflict: false, 
            mem: mem, 
        }
    }

    // memory copy の前後で consistency check を行い、適合した場合のみ読み込み成功
    pub fn load(&mut self, addr: usize) -> Option<[u8; STRIPE_SIZE]> {
        assert_eq!(addr & (STRIPE_SIZE - 1), 0);    // address がストライプのアライメントに適合しない場合はエラー

        // consistency check
        if self.conflict {
            return None;
        } 
        if !self.mem.test_not_modify(addr, self.read_version) {
            self.conflict = true;
            return None;
        }

        // メモリコピー
        fence(Acquire);
        let mut mem = [0; STRIPE_SIZE];
        for i in 0..STRIPE_SIZE {
            mem[i] = self.mem.mem[addr + i];
        }

        fence(SeqCst);
        // consistency check: 読み込みメモリがロックされておらず、かつ read_version 以下であるかどうか
        if !self.mem.test_not_modify(addr, self.read_version) {
            self.conflict = true;
            return None;
        }

        Some(mem)
    }
}

pub struct WriteTrans<'a> {
    read_version: u64,
    read_set: HashSet<usize>,
    write_set: HashMap<usize, [u8; STRIPE_SIZE]>,
    locked: Vec<usize>,     // lock したアドレス (Drop するときのため覚えておく)
    conflict: bool,
    mem: &'a mut Memory,
}

impl<'a> WriteTrans<'a> {
    fn new(mem: &'a mut Memory) -> Self {
        WriteTrans { 
            read_version: mem.global_clock.load(Acquire),       // global_clock を copy
            read_set: HashSet::new(), 
            write_set: HashMap::new(), 
            locked: Vec::new(), 
            conflict: false, 
            mem: mem, 
        }
    }

    // メモリの変更内容 (val) を write_set に (一時) 保存
    pub fn store(&mut self, addr: usize, val: [u8; STRIPE_SIZE]) {
        assert_eq!(addr & (STRIPE_SIZE - 1), 0);
        self.write_set.insert(addr, val);
    }

    pub fn load(&mut self, addr: usize) -> Option<[u8; STRIPE_SIZE]> {
        assert_eq!(addr & (STRIPE_SIZE - 1), 0);    // address がストライプのアライメントに適合しない場合はエラー

        if self.conflict {
            return None;
        }

        self.read_set.insert(addr);     // 読み込みアドレス保存

        if let Some(m) = self.write_set.get(&addr) {    // データが write_set にあればそれを読み込み
            return Some(*m);
        }   // ない場合はメモリコピーを行う (ReadTrans の場合と同様)

        if !self.mem.test_not_modify(addr, self.read_version) {     // consistency check
            self.conflict = true;
            return None;
        }

        // メモリコピー
        fence(Acquire);
        let mut mem = [0; STRIPE_SIZE];
        for i in 0..STRIPE_SIZE {
            mem[i] = self.mem.mem[addr + i];
        }

        fence(SeqCst);
        // consistency check: 読み込みメモリがロックされておらず、かつ read_version 以下であるかどうか
        if !self.mem.test_not_modify(addr, self.read_version) {
            self.conflict = true;
            return None;
        }

        Some(mem)
    }

    // write_set に対応するメモリをロックしようと試みる
    fn lock_write_set(&mut self) -> bool {
        for (addr, _) in self.write_set.iter() {
            if self.mem.lock_addr(*addr) {      // lock 獲得に成功
                self.locked.push(*addr);        // drop 時のために覚えておく
            } else {
                return false;
            }
        }
        true
    }

    fn validate_read_set(&self) -> bool {                               // read_set 検証
        for addr in self.read_set.iter() {                          // メモリから読み込んだすべてのアドレスに対し
            if self.write_set.contains_key(addr) {                          // write していたならば
                let version = self.mem.get_version(*addr);             // 処理中に version が更新されていないか調べる
                if version > self.read_version {
                    return false;
                }
            } else {                                                        // write していないならば
                if !self.mem.test_not_modify(*addr, self.read_version) {    // 処理中に version が更新されていないか調べる
                    return false;
                }
            }
        }
        true
    }

    fn commit(&mut self, version: u64) {
        // メモリに書き込み (copy)
        for (addr, val) in self.write_set.iter() {
            let addr = *addr as usize;
            for i in 0..STRIPE_SIZE {
                self.mem.mem[addr + i] = val[i];
            }
        }
        fence(Release);

        for (addr, _) in self.write_set.iter() {
            let stripe = addr >> self.mem.shift_size;               // ストライプの index
            self.mem.lock_ver[stripe].store(version, Relaxed);  // version 更新
        }
        self.locked.clear();    // lock flag 解除
    }
}

impl<'a> Drop for WriteTrans<'a> {
    fn drop(&mut self) {    // locked に記録されたメモリのロックを解除
        for addr in self.locked.iter() {
            self.mem.unlock_addr(*addr);
        }
    }
}

pub enum STMResult<T> {
    Ok(T),
    Retry,
    Abort,
}

pub struct STM {
    mem: UnsafeCell<Memory>
}

unsafe impl Sync for STM {}
unsafe impl Send for STM {}

impl STM {
    pub fn new() -> Self {
        STM {mem: UnsafeCell::new(Memory::new())}
    }

    pub fn read_transaction<F, R>(&self, f: F) -> Option<R> 
    where F: Fn(&mut ReadTrans) -> STMResult<R> {
        loop {
            let mut read_trans = ReadTrans::new(unsafe {&*self.mem.get()});     // 排他的でないメモリの参照を与える

            // 投機的実行
            match f(&mut read_trans) {
                STMResult::Abort => return None,
                STMResult::Retry => {
                    if read_trans.conflict {
                        continue;       // retry
                    } else {
                        return None;
                    }
                },
                STMResult::Ok(val) => {
                    if read_trans.conflict {
                        continue;
                    } else {
                        return Some(val);
                    }
                }
            }
        }
    }

    pub fn write_transaction<F, R>(&self, f: F) -> Option<R>
    where F: Fn(&mut WriteTrans) -> STMResult<R> {
        loop {
            let mut write_trans = WriteTrans::new(unsafe {&mut *self.mem.get()});   // 排他的でないメモリの参照を与える

            // 投機的実行
            let result;
            match f(&mut write_trans) {
                STMResult::Abort => return None,
                STMResult::Retry => {
                    if write_trans.conflict {
                        continue;
                    } else {
                        return None;
                    }
                }
                STMResult::Ok(val) => {
                    if write_trans.conflict {
                        continue;
                    } else {
                        result = val;
                    }
                }
            }

            // version update
            if !write_trans.lock_write_set() {      // write lock 獲得を試みる
                continue;                          
            }   // 以下 write lock 獲得済み

            // version と 整合性を検証
            let new_version = write_trans.mem.inc_global_clock();
            if (write_trans.read_version + 1 != new_version) && !write_trans.validate_read_set() {
                continue;
            }

            // commit と return result
            write_trans.commit(new_version);
            return Some(result);
        }
    }
}