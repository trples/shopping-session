//! Shopping Sessions pipeline, native. Implements rules.md exactly.
//!
//! Reads  $DATA_DIR   (default /data)
//! Writes $OUTPUT_DIR (default /output): user_daily/ and campaign_daily/ as Parquet
//!
//! Shape of the work, all on one rayon pool:
//!   1. memory-map every input file; split big files into newline-aligned chunks
//!   2. links  -> union-find over interned IDs -> each linked ID's person name
//!   3. events -> parse + judge every copy -> scatter by hash(event_id)
//!             -> per bucket keep the copy with the smallest ingest_ts
//!             -> valid kept events, scattered by hash(user)
//!   4. clicks -> parse + validate -> scattered by hash(user)
//!   4b. per user bucket: resolve linked IDs against that bucket's small slice of the
//!      link map, scatter by hash(person)
//!   5. per person bucket: sort each person's events and clicks, sessionize,
//!      credit and assists with one sliding 7-day window over the clicks
//!   6. write Parquet
//! IDs are 16-byte keys copied out of the input while parsing (see K).

use std::fs::{self, File};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;

type S = &'static [u8];

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
/// Buckets for the hash scatters (event dedup and per-person work).
const NB: usize = 512;
const NO_CAT: u16 = u16::MAX;

// ------------------------------------------------------------------ logging

static mut T0: Option<Instant> = None;

fn log(msg: &str) {
    #[allow(static_mut_refs)]
    let t = unsafe { T0.unwrap() }.elapsed().as_secs_f64();
    eprintln!("[{t:7.3}s] {msg}");
}

// ------------------------------------------------------------------ input

fn map_file(p: &Path) -> S {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    // Pages are faulted in on demand by the parsing threads, in parallel (Linux
    // maps cached pages 16 at a time). Pre-faulting (MAP_POPULATE) or readahead
    // hints would serialize that work before parsing starts.
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// The data part of a file (everything after the header line), cut into pieces of
/// about `target` bytes that end on a newline.
fn chunks_of(data: S, target: usize, out: &mut Vec<S>) {
    let body = match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => return, // header only (or empty)
    };
    let mut start = 0;
    while start < body.len() {
        let mut end = (start + target).min(body.len());
        if end < body.len() {
            end = match memchr::memchr(b'\n', &body[end..]) {
                Some(i) => end + i + 1,
                None => body.len(),
            };
        }
        out.push(&body[start..end]);
        start = end;
    }
}

fn load_chunks(dir: &Path, target: usize) -> Vec<S> {
    let files = csv_files(dir);
    let maps: Vec<S> = files.par_iter().map(|p| map_file(p)).collect();
    let mut out = Vec::new();
    for m in maps {
        chunks_of(m, target, &mut out);
    }
    out
}

/// Iterate the lines of a chunk. Only `\n` ends a line; a `\r` stays in the field.
fn lines(chunk: S) -> impl Iterator<Item = S> {
    let mut start = 0usize;
    let mut done = false;
    let mut it = memchr::memchr_iter(b'\n', chunk);
    std::iter::from_fn(move || {
        if done {
            return None;
        }
        match it.next() {
            Some(i) => {
                let l = &chunk[start..i];
                start = i + 1;
                Some(l)
            }
            None => {
                done = true;
                Some(&chunk[start..]) // the last line, possibly empty
            }
        }
    })
}

/// The hot-path parser: one SIMD scan of the whole chunk for `,` `"` and `\n`,
/// calling `f` with the fields of every well-formed line. Same dialect as `fields`:
/// exactly N fields; a field holding quotes must be wrapped in exactly one pair
/// (quote first, quote last, none inside), and the quotes are stripped. A line with
/// the wrong field count, a stray quote, or nothing in it is skipped.
#[inline]
fn for_records<const N: usize>(chunk: S, mut f: impl FnMut([S; N])) {
    let mut out: [S; N] = [&[]; N];
    let mut nf = 0usize; // fields closed so far on this line
    let mut fs = 0usize; // current field start
    let mut qn = 0u32; // quotes seen in the current field
    let mut qf = 0usize; // position of its first quote
    let mut ql = 0usize; // position of its last quote
    let mut bad = false;
    // close the field [fs, end); false if the line is now malformed
    #[inline(always)]
    fn close<const N: usize>(
        chunk: S, out: &mut [S; N], nf: &mut usize, fs: usize, end: usize, qn: u32, qf: usize, ql: usize,
    ) -> bool {
        if *nf == N {
            return false; // too many fields
        }
        let fld = match qn {
            0 => &chunk[fs..end],
            2 if qf == fs && ql + 1 == end => &chunk[fs + 1..end - 1],
            _ => return false,
        };
        out[*nf] = fld;
        *nf += 1;
        true
    }
    // Every `,` `"` `\n` position in order, 8 bytes at a time: an exact per-byte
    // equality mask (SWAR), walked with trailing_zeros. Hits are dense (~1 per 8
    // bytes), where this beats a SIMD search that restarts after every hit.
    let mut step = |i: usize| {
        match chunk[i] {
            b',' => {
                if !bad && !close(chunk, &mut out, &mut nf, fs, i, qn, qf, ql) {
                    bad = true;
                }
                fs = i + 1;
                qn = 0;
            }
            b'"' => {
                if qn == 0 {
                    qf = i;
                }
                ql = i;
                qn += 1;
            }
            _ => {
                if !bad && close(chunk, &mut out, &mut nf, fs, i, qn, qf, ql) && nf == N {
                    f(out);
                }
                nf = 0;
                fs = i + 1;
                qn = 0;
                bad = false;
            }
        }
        };
    const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    #[inline(always)]
    fn zero_bytes(x: u64) -> u64 {
        // high bit set exactly in the bytes of x that are zero
        !(((x & LO7).wrapping_add(LO7)) | x | LO7)
    }
    let n = chunk.len();
    let mut base = 0usize;
    while base + 8 <= n {
        let w = u64::from_le_bytes(chunk[base..base + 8].try_into().unwrap());
        let mut m = zero_bytes(w ^ 0x2c2c_2c2c_2c2c_2c2c) | zero_bytes(w ^ 0x2222_2222_2222_2222)
            | zero_bytes(w ^ 0x0a0a_0a0a_0a0a_0a0a);
        while m != 0 {
            step(base + (m.trailing_zeros() / 8) as usize);
            m &= m - 1;
        }
        base += 8;
    }
    for i in base..n {
        if matches!(chunk[i], b',' | b'"' | b'\n') {
            step(i);
        }
    }
    // a last line without a trailing newline
    if fs < chunk.len() || nf > 0 {
        let end = chunk.len();
        if !bad && close(chunk, &mut out, &mut nf, fs, end, qn, qf, ql) && nf == N {
            f(out);
        }
    }
}

/// Apply the CSV dialect to one line: exactly `N` comma-separated fields, each
/// either quote-free or wrapped in one pair of quotes with none inside. Quotes are
/// stripped. An empty field (quoted or not) is an empty slice, meaning null.
#[inline]
fn fields<const N: usize>(line: S) -> Option<[S; N]> {
    let mut out: [S; N] = [&[]; N];
    let mut k = 0;
    let mut start = 0;
    for i in memchr::memchr_iter(b',', line) {
        if k == N - 1 {
            return None; // too many fields
        }
        out[k] = &line[start..i];
        k += 1;
        start = i + 1;
    }
    if k != N - 1 {
        return None; // too few fields (an empty line lands here too)
    }
    out[k] = &line[start..];
    for f in out.iter_mut() {
        if memchr::memchr(b'"', f).is_some() {
            let n = f.len();
            if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr::memchr(b'"', &f[1..n - 1]).is_none() {
                *f = &f[1..n - 1];
            } else {
                return None;
            }
        }
    }
    Some(out)
}

// ------------------------------------------------------------------ values

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let (a, b) = (s[i], s[i + 1]);
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') * 10 + (b - b'0')) as i64)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `s`, checked for shape and calendar.
#[inline]
fn civil_ms(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) { 29 } else { 28 }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 86400) + h * 3600 + mi * 60 + se) * 1000)
}

/// F1, F2 or F3 to epoch ms. None for anything else. F3 values too large for an
/// i64 are also None: they can only ever be outside the window.
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &b in s {
            v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        }
        return Some(v);
    }
    if n == 20 && s[19] == b'Z' {
        return civil_ms(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        let base = civil_ms(s)?;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    None
}

/// Digits only, value 1 to 10,000,000.
#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        v = v * 10 + (b - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

fn manifest_value(text: &str, key: &str) -> String {
    let k = format!("\"{key}\"");
    let i = text.find(&k).unwrap_or_else(|| panic!("manifest: no {key}")) + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = rest[a..].find('"').unwrap() + a;
    rest[a..b].to_string()
}

// ------------------------------------------------------------------ keys

/// A string from the input as a 16-byte key. Up to 15 bytes are stored inline
/// (byte 0 = length, then the bytes, then zeros), so equality and hashing are two
/// machine words and later phases never reach back into the mapped input. Longer
/// strings (tag LONG) keep a pointer into the input. Each string has exactly one
/// representation, so word equality on inline keys is byte equality: exact.
#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct K {
    w: [u64; 2],
}
const LONG: u8 = 0xFF;

impl K {
    #[inline]
    fn new(s: &[u8]) -> K {
        let mut b = [0u8; 16];
        if s.len() <= 15 {
            b[0] = s.len() as u8;
            b[1..1 + s.len()].copy_from_slice(s);
        } else {
            b[0] = LONG;
            b[4..8].copy_from_slice(&(s.len() as u32).to_le_bytes());
            b[8..16].copy_from_slice(&(s.as_ptr() as usize as u64).to_le_bytes());
        }
        K { w: [u64::from_le_bytes(b[..8].try_into().unwrap()), u64::from_le_bytes(b[8..].try_into().unwrap())] }
    }
    #[inline]
    fn tag(&self) -> u8 {
        (self.w[0] & 0xFF) as u8
    }
    #[inline]
    fn bytes(&self) -> &[u8] {
        let t = self.tag();
        if t != LONG {
            let p = self as *const K as *const u8;
            unsafe { std::slice::from_raw_parts(p.add(1), t as usize) }
        } else {
            let n = (self.w[0] >> 32) as usize;
            unsafe { std::slice::from_raw_parts(self.w[1] as usize as *const u8, n) } // leaked, never unmapped
        }
    }
}
impl PartialEq for K {
    #[inline]
    fn eq(&self, o: &K) -> bool {
        if self.tag() == LONG && o.tag() == LONG { self.bytes() == o.bytes() } else { self.w == o.w }
    }
}
impl Eq for K {}
impl std::hash::Hash for K {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        if self.tag() == LONG {
            self.bytes().hash(h)
        } else {
            h.write_u64(self.w[0]);
            h.write_u64(self.w[1]);
        }
    }
}
impl PartialOrd for K {
    fn partial_cmp(&self, o: &K) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for K {
    #[inline]
    fn cmp(&self, o: &K) -> std::cmp::Ordering {
        self.bytes().cmp(o.bytes())
    }
}

// ------------------------------------------------------------------ records

/// One parsed event copy, judged on its own (`ok`).
#[derive(Clone, Copy)]
struct RawEv {
    id: K,
    user: K,
    ts: i64,
    ing: i64,
    price: u32,
    cat: u16, // category of a valid purchase, NO_CAT otherwise
    ok: bool,
}

/// A surviving event keyed by user ID (before linked IDs are resolved), or by person.
#[derive(Clone, Copy)]
struct Ev {
    who: K,
    ts: i64,
    price: u32,
    cat: u16,
}

/// A valid click keyed by user ID, or by person.
#[derive(Clone, Copy)]
struct Clk {
    who: K,
    id: K,
    ts: i64,
    camp: u16,
}

struct UdRow {
    person: K,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16, // NO_CAT for null
}

// ------------------------------------------------------------------ scatter

/// Records grouped by bucket in one exact-size buffer (a counting sort), instead of
/// one growing Vec per bucket: far fewer allocations and page faults.
struct Scattered<T> {
    items: Vec<T>,
    off: Vec<u32>, // NB + 1 offsets
}

impl<T: Copy> Scattered<T> {
    fn new(src: Vec<T>, keys: Vec<u16>) -> Self {
        let mut off = vec![0u32; NB + 1];
        for &k in &keys {
            off[k as usize + 1] += 1;
        }
        for i in 0..NB {
            off[i + 1] += off[i];
        }
        let mut at: Vec<u32> = off[..NB].to_vec();
        let mut items: Vec<T> = Vec::with_capacity(src.len());
        let dst = items.as_mut_ptr();
        for (x, &k) in src.iter().zip(&keys) {
            let i = at[k as usize] as usize;
            at[k as usize] += 1;
            unsafe { dst.add(i).write(*x) };
        }
        unsafe { items.set_len(src.len()) };
        Scattered { items, off }
    }
    #[inline]
    fn get(&self, b: usize) -> &[T] {
        &self.items[self.off[b] as usize..self.off[b + 1] as usize]
    }
}

// ------------------------------------------------------------------ main

fn main() {
    unsafe { T0 = Some(Instant::now()) };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let ncpu: usize = env("NCPU", "0").parse().unwrap_or(0);
    let ncpu = if ncpu == 0 { std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8) } else { ncpu };
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let man = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(manifest_value(&man, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(manifest_value(&man, "window_end").as_bytes()).expect("window_end");
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;
    log(&format!("window {ws} .. {we}, {ndays} days, {ncpu} threads"));

    // ---- products and campaigns (small, clean)
    let mut cat_names: Vec<S> = Vec::new();
    let mut cat_idx: HashMap<S, u16> = HashMap::new();
    let mut products: HashMap<K, u16> = HashMap::new();
    let mut pchunks = Vec::new();
    chunks_of(map_file(&data.join("products.csv")), usize::MAX, &mut pchunks);
    for c in &pchunks {
        for line in lines(c) {
            if let Some([pid, cat]) = fields::<2>(line) {
                if pid.is_empty() {
                    continue;
                }
                let n = cat_names.len() as u16;
                let ci = *cat_idx.entry(cat).or_insert_with(|| {
                    cat_names.push(cat);
                    n
                });
                products.entry(K::new(pid)).or_insert(ci);
            }
        }
    }
    let unknown_cat = match cat_idx.get(&b"unknown"[..]) {
        Some(&i) => i,
        None => {
            cat_names.push(b"unknown");
            (cat_names.len() - 1) as u16
        }
    };
    let mut camp_ids: Vec<S> = Vec::new();
    let mut camp_ch: Vec<S> = Vec::new();
    let mut campaigns: HashMap<K, u16> = HashMap::new();
    let mut cchunks = Vec::new();
    chunks_of(map_file(&data.join("campaigns.csv")), usize::MAX, &mut cchunks);
    for c in &cchunks {
        for line in lines(c) {
            if let Some([cid, ch]) = fields::<2>(line) {
                let k = K::new(cid);
                if cid.is_empty() || campaigns.contains_key(&k) {
                    continue;
                }
                campaigns.insert(k, camp_ids.len() as u16);
                camp_ids.push(cid);
                camp_ch.push(ch);
            }
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp; // accumulator slot for organic
    log(&format!("{} products, {} categories, {ncamp} campaigns", products.len(), cat_names.len()));

    // ---- map every input
    const CH: usize = 8 << 20;
    let (link_chunks, (ev_chunks, clk_chunks)) = rayon::join(
        || load_chunks(&data.join("links"), CH),
        || rayon::join(|| load_chunks(&data.join("events"), CH), || load_chunks(&data.join("clicks"), CH)),
    );
    log(&format!("mapped: {} event, {} click, {} link chunks", ev_chunks.len(), clk_chunks.len(), link_chunks.len()));

    let part = foldhash::fast::FixedState::with_seed(0x5eed_1234_abcd_0001);
    let bucket = |k: &K| (part.hash_one(k) as usize) % NB;

    // ---- three independent inputs at once
    let (link_parts, (ev_by_user, clk_by_user)) = rayon::join(
        // links: each linked ID -> its person's name, as one small map per bucket(id).
        // IDs are numbered per bucket in parallel; pairs become integer edges via two
        // bucketed passes (each lookup hits one small map); union-find runs on u32s.
        || {
            let pairs: Vec<Scattered<(K, K)>> = link_chunks
                .par_iter()
                .map(|c| {
                    let mut items = Vec::new();
                    let mut keys = Vec::new();
                    for_records::<2>(c, |[a, b]| {
                        if !a.is_empty() && !b.is_empty() {
                            let (a, b) = (K::new(a), K::new(b));
                            keys.push(bucket(&a) as u16);
                            items.push((a, b));
                        }
                    });
                    Scattered::new(items, keys)
                })
                .collect();
            // pass 1, by bucket(a): number every `a` in its bucket, hand (ga, b) on by bucket(b)
            let numbered: Vec<(HashMap<K, u32>, Vec<K>, Scattered<(u32, K)>)> = (0..NB)
                .into_par_iter()
                .map(|bk| {
                    let mut ids: HashMap<K, u32> = HashMap::new();
                    let mut names: Vec<K> = Vec::new();
                    let mut items = Vec::new();
                    let mut keys = Vec::new();
                    for src in &pairs {
                        for &(a, b) in src.get(bk) {
                            let n = names.len() as u32;
                            let la = *ids.entry(a).or_insert_with(|| {
                                names.push(a);
                                n
                            });
                            keys.push(bucket(&b) as u16);
                            items.push((la, b));
                        }
                    }
                    (ids, names, Scattered::new(items, keys))
                })
                .collect();
            drop(pairs);
            // every `b` also needs a number in its own bucket; a's local ids are final
            // already, so b's that are new extend each bucket's list
            let mut ids: Vec<HashMap<K, u32>> = Vec::with_capacity(NB);
            let mut names_b: Vec<Vec<K>> = Vec::with_capacity(NB);
            let mut fwd: Vec<Scattered<(u32, K)>> = Vec::with_capacity(NB);
            for (m, n, f) in numbered {
                ids.push(m);
                names_b.push(n);
                fwd.push(f);
            }
            // pass 2, by bucket(b): number new b's, emit (bucket(a), local a, local b)
            let half: Vec<Vec<(u16, u32, u32)>> = ids
                .par_iter_mut()
                .zip(names_b.par_iter_mut())
                .enumerate()
                .map(|(bk, (m, names))| {
                    let mut out = Vec::new();
                    for (src_bucket, src) in fwd.iter().enumerate() {
                        for &(la, b) in src.get(bk) {
                            let n = names.len() as u32;
                            let lb = *m.entry(b).or_insert_with(|| {
                                names.push(b);
                                n
                            });
                            out.push((src_bucket as u16, la, lb));
                        }
                    }
                    out
                })
                .collect();
            drop(fwd);
            let mut off = vec![0u32; NB + 1];
            for bk in 0..NB {
                off[bk + 1] = off[bk] + names_b[bk].len() as u32;
            }
            let n = off[NB] as usize;
            let mut names: Vec<K> = Vec::with_capacity(n);
            for v in &names_b {
                names.extend_from_slice(v);
            }
            // union-find on integers; each root keeps the byte-wise smallest name
            let mut parent: Vec<u32> = (0..n as u32).collect();
            fn find(p: &mut [u32], mut x: u32) -> u32 {
                while p[x as usize] != x {
                    let g = p[p[x as usize] as usize];
                    p[x as usize] = g;
                    x = g;
                }
                x
            }
            for (bk, v) in half.iter().enumerate() {
                for &(sa, la, lb) in v {
                    let ga = off[sa as usize] + la;
                    let gb = off[bk] + lb;
                    let (ra, rb) = (find(&mut parent, ga), find(&mut parent, gb));
                    if ra != rb {
                        let (keep, drop) = if names[ra as usize] <= names[rb as usize] { (ra, rb) } else { (rb, ra) };
                        parent[drop as usize] = keep;
                    }
                }
            }
            drop(half);
            for i in 0..n as u32 {
                let r = find(&mut parent, i);
                parent[i as usize] = r;
            }
            let root = parent;
            let parts: Vec<HashMap<K, K>> = ids
                .into_par_iter()
                .enumerate()
                .map(|(bk, m)| m.into_iter().map(|(k, l)| (k, names[root[(off[bk] + l) as usize] as usize])).collect())
                .collect();
            log(&format!("links: {n} linked ids"));
            parts
        },
        || {
            rayon::join(
                // events: parse and judge every copy, scatter by event_id, dedup,
                // scatter the survivors by user ID
                || {
                    let raw: Vec<Scattered<RawEv>> = ev_chunks
                        .par_iter()
                        .map(|c| {
                            let est = c.len() / 60 + 16;
                            let mut items: Vec<RawEv> = Vec::with_capacity(est);
                            let mut keys: Vec<u16> = Vec::with_capacity(est);
                            for_records::<7>(c, |[id, user, ts, ing, et, prod, price]| {
                                if id.is_empty() {
                                    return;
                                }
                                let t = parse_ts(ts);
                                let g = parse_ts(ing);
                                let is_p = et == b"purchase";
                                let type_ok = is_p || et == b"view" || et == b"cart";
                                let pr = if is_p { parse_price(price) } else { Some(0) };
                                let ok = match (t, g) {
                                    (Some(t), Some(g)) => {
                                        !user.is_empty() && t >= ws && t < we && type_ok && !prod.is_empty()
                                            && pr.is_some() && g - t <= DAY
                                    }
                                    _ => false,
                                };
                                let cat = if ok && is_p {
                                    *products.get(&K::new(prod)).unwrap_or(&unknown_cat)
                                } else {
                                    NO_CAT
                                };
                                let k = K::new(id);
                                keys.push(bucket(&k) as u16);
                                items.push(RawEv {
                                    id: k,
                                    user: K::new(user),
                                    ts: t.unwrap_or(0),
                                    ing: g.unwrap_or(i64::MAX),
                                    price: if ok && is_p { pr.unwrap() } else { 0 },
                                    cat,
                                    ok,
                                });
                            });
                            Scattered::new(items, keys)
                        })
                        .collect();
                    log("events parsed");
                    // smallest ingest_ts wins; copies with equal ingest_ts are identical
                    let kept: Vec<Scattered<Ev>> = (0..NB)
                        .into_par_iter()
                        .map(|p| {
                            let n: usize = raw.iter().map(|c| c.get(p).len()).sum();
                            let mut refs: Vec<&RawEv> = Vec::with_capacity(n);
                            let mut best: HashMap<K, u32> = HashMap::with_capacity(n);
                            for c in &raw {
                                for r in c.get(p) {
                                    match best.entry(r.id) {
                                        hashbrown::hash_map::Entry::Occupied(e) => {
                                            let k = *e.get() as usize;
                                            if r.ing < refs[k].ing {
                                                refs[k] = r;
                                            }
                                        }
                                        hashbrown::hash_map::Entry::Vacant(e) => {
                                            e.insert(refs.len() as u32);
                                            refs.push(r);
                                        }
                                    }
                                }
                            }
                            let mut items: Vec<Ev> = Vec::with_capacity(refs.len());
                            let mut keys: Vec<u16> = Vec::with_capacity(refs.len());
                            for r in refs {
                                if r.ok {
                                    keys.push(bucket(&r.user) as u16);
                                    items.push(Ev { who: r.user, ts: r.ts, price: r.price, cat: r.cat });
                                }
                            }
                            Scattered::new(items, keys)
                        })
                        .collect();
                    log("events deduped");
                    kept
                },
                // clicks: no dedup needed (copies are identical and nothing below is
                // sensitive to them); validate, scatter by user ID
                || {
                    let v: Vec<Scattered<Clk>> = clk_chunks
                        .par_iter()
                        .map(|c| {
                            let est = c.len() / 45 + 16;
                            let mut items: Vec<Clk> = Vec::with_capacity(est);
                            let mut keys: Vec<u16> = Vec::with_capacity(est);
                            for_records::<4>(c, |[id, user, camp, ts]| {
                                if id.is_empty() || user.is_empty() {
                                    return;
                                }
                                let Some(t) = parse_ts(ts) else { return };
                                if t < ws - WEEK || t >= we {
                                    return;
                                }
                                let Some(&ci) = campaigns.get(&K::new(camp)) else { return };
                                let u = K::new(user);
                                keys.push(bucket(&u) as u16);
                                items.push(Clk { who: u, id: K::new(id), ts: t, camp: ci });
                            });
                            Scattered::new(items, keys)
                        })
                        .collect();
                    log("clicks parsed");
                    v
                },
            )
        },
    );

    // ---- user ID -> person, one small cache-resident piece of the link map at a
    //      time (every record in bucket u has its user ID in link_parts[u]'s range);
    //      then scatter by person
    let (ev_by_person, clk_by_person): (Vec<Scattered<Ev>>, Vec<Scattered<Clk>>) = (0..NB)
        .into_par_iter()
        .map(|u| {
            let lm = &link_parts[u];
            let n: usize = ev_by_user.iter().map(|s| s.get(u).len()).sum();
            let mut items: Vec<Ev> = Vec::with_capacity(n);
            let mut keys: Vec<u16> = Vec::with_capacity(n);
            for s in &ev_by_user {
                for e in s.get(u) {
                    let p = *lm.get(&e.who).unwrap_or(&e.who);
                    keys.push(bucket(&p) as u16);
                    items.push(Ev { who: p, ..*e });
                }
            }
            let evs = Scattered::new(items, keys);
            let n: usize = clk_by_user.iter().map(|s| s.get(u).len()).sum();
            let mut items: Vec<Clk> = Vec::with_capacity(n);
            let mut keys: Vec<u16> = Vec::with_capacity(n);
            for s in &clk_by_user {
                for c in s.get(u) {
                    let p = *lm.get(&c.who).unwrap_or(&c.who);
                    keys.push(bucket(&p) as u16);
                    items.push(Clk { who: p, ..*c });
                }
            }
            (evs, Scattered::new(items, keys))
        })
        .unzip();
    drop(ev_by_user);
    drop(clk_by_user);
    log("persons resolved");

    // ---- per person: sessions, credit, assists
    let nslot = (ncamp + 1) * ndays;
    let results: Vec<(Vec<UdRow>, Vec<[i64; 4]>)> = (0..NB)
        .into_par_iter()
        .fold(|| (Vec::new(), vec![[0i64; 4]; nslot]), |(mut rows, mut acc), q| {
            // group this bucket's events and clicks by person: number the persons,
            // counting-sort by number, then sort each person's (small) slice by time
            let n: usize = ev_by_person.iter().map(|s| s.get(q).len()).sum();
            let mut pid: HashMap<K, u32> = HashMap::with_capacity(n / 8 + 16);
            let mut ep: Vec<u32> = Vec::with_capacity(n);
            for src in &ev_by_person {
                for e in src.get(q) {
                    let next = pid.len() as u32;
                    ep.push(*pid.entry(e.who).or_insert(next));
                }
            }
            let np = pid.len();
            let group = |n: usize, ids: &[u32]| -> (Vec<u32>, Vec<u32>) {
                let mut off = vec![0u32; np + 1];
                for &i in ids {
                    off[i as usize + 1] += 1;
                }
                for i in 0..np {
                    off[i + 1] += off[i];
                }
                let mut at = off[..np].to_vec();
                let mut slot = Vec::with_capacity(n);
                for &i in ids {
                    slot.push(at[i as usize]);
                    at[i as usize] += 1;
                }
                (off, slot)
            };
            let (e_off, e_slot) = group(n, &ep);
            let mut evs: Vec<Ev> = Vec::with_capacity(n);
            {
                let dst = evs.as_mut_ptr();
                let mut k = 0;
                for src in &ev_by_person {
                    for e in src.get(q) {
                        unsafe { dst.add(e_slot[k] as usize).write(*e) };
                        k += 1;
                    }
                }
                unsafe { evs.set_len(n) };
            }
            let mut cp: Vec<u32> = Vec::new();
            let mut cl: Vec<Clk> = Vec::new();
            for src in &clk_by_person {
                for c in src.get(q) {
                    if let Some(&i) = pid.get(&c.who) {
                        cp.push(i); // clicks of persons without events credit nothing
                        cl.push(*c);
                    }
                }
            }
            let (c_off, c_slot) = group(cl.len(), &cp);
            let mut clks: Vec<Clk> = Vec::with_capacity(cl.len());
            {
                let dst = clks.as_mut_ptr();
                for (k, c) in cl.iter().enumerate() {
                    unsafe { dst.add(c_slot[k] as usize).write(*c) };
                }
                unsafe { clks.set_len(cl.len()) };
            }
            drop(cl);

            // acc slots: credited, purchasing, revenue, assisted
            let mut cnt = vec![0u32; ncamp];
            let mut pos = vec![0u32; ncamp];
            let mut active: Vec<u16> = Vec::new();
            let mut catsum: Vec<(u16, i64)> = Vec::new();

            let flush = |row: &UdRow, catsum: &mut Vec<(u16, i64)>, rows: &mut Vec<UdRow>| {
                if row.sessions > 0 {
                    let mut top = NO_CAT;
                    let mut best = -1i64;
                    for &(k, v) in catsum.iter() {
                        if v > best || (v == best && cat_names[k as usize] < cat_names[top as usize]) {
                            best = v;
                            top = k;
                        }
                    }
                    rows.push(UdRow { top, ..*row });
                }
                catsum.clear();
            };

            for me in 0..np {
                let e = &mut evs[e_off[me] as usize..e_off[me + 1] as usize];
                e.sort_unstable_by_key(|x| x.ts);
                let e = &*e;
                let c = &mut clks[c_off[me] as usize..c_off[me + 1] as usize];
                c.sort_unstable_by(|a, b| (a.ts, a.id).cmp(&(b.ts, b.id)));
                let c = &*c;
                let person = e[0].who;

                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day = i64::MIN;
                let mut row = UdRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: NO_CAT };

                let mut i = 0;
                while i < e.len() {
                    // one session: events while the gap to the previous one is <= 30 min
                    let start = e[i].ts;
                    let mut last = start;
                    let mut j = i;
                    let mut rev = 0i64;
                    let mut hasp = false;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        flush(&row, &mut catsum, &mut rows);
                        row = UdRow { person, day: day as i32, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
                        cur_day = day;
                    }
                    while j < e.len() && e[j].ts - last <= GAP {
                        let x = &e[j];
                        last = x.ts;
                        if x.cat != NO_CAT {
                            rev += x.price as i64;
                            hasp = true;
                            match catsum.iter_mut().find(|(k, _)| *k == x.cat) {
                                Some(slot) => slot.1 += x.price as i64,
                                None => catsum.push((x.cat, x.price as i64)),
                            }
                        }
                        j += 1;
                    }
                    row.sessions += 1;
                    row.events += (j - i) as i64;
                    row.revenue += rev;

                    // clicks in [start - 7d, start]
                    while hi < c.len() && c[hi].ts <= start {
                        let k = c[hi].camp as usize;
                        if cnt[k] == 0 {
                            pos[k] = active.len() as u32;
                            active.push(k as u16);
                        }
                        cnt[k] += 1;
                        hi += 1;
                    }
                    while lo < hi && c[lo].ts < start - WEEK {
                        let k = c[lo].camp as usize;
                        cnt[k] -= 1;
                        if cnt[k] == 0 {
                            let p = pos[k] as usize;
                            let moved = *active.last().unwrap();
                            active.swap_remove(p);
                            if p < active.len() {
                                pos[moved as usize] = p as u32;
                            }
                        }
                        lo += 1;
                    }
                    let didx = (day - ws_day) as usize;
                    let credited = if hi > lo {
                        // latest click; on a ts tie the smallest click_id (sorted first)
                        let t = c[hi - 1].ts;
                        let mut k = hi - 1;
                        while k > lo && c[k - 1].ts == t {
                            k -= 1;
                        }
                        c[k].camp as usize
                    } else {
                        organic
                    };
                    let a = &mut acc[credited * ndays + didx];
                    a[0] += 1;
                    a[1] += hasp as i64;
                    a[2] += rev;
                    for &k in &active {
                        acc[k as usize * ndays + didx][3] += 1;
                    }
                    i = j;
                }
                flush(&row, &mut catsum, &mut rows);
                // empty the window for the next person
                while lo < hi {
                    cnt[c[lo].camp as usize] -= 1;
                    lo += 1;
                }
                active.clear();
            }
            (rows, acc)
        })
        .collect();
    drop(ev_by_person);
    drop(clk_by_person);
    log("sessions done");

    // ---- campaign accumulators
    let mut acc = vec![[0i64; 4]; nslot];
    for (_, a) in &results {
        for (t, s) in acc.iter_mut().zip(a) {
            for k in 0..4 {
                t[k] += s[k];
            }
        }
    }

    // ---- write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir user_daily");
    fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let groups: Vec<Vec<&UdRow>> = {
        let ngroups = ncpu.max(1);
        let mut g: Vec<Vec<&UdRow>> = (0..ngroups).map(|_| Vec::new()).collect();
        let mut i = 0;
        for (rows, _) in results.iter() {
            for r in rows {
                g[i % ngroups].push(r);
                i += 1;
            }
        }
        g
    };
    rayon::join(
        || {
            groups.par_iter().enumerate().for_each(|(i, rows)| {
                out_parquet::user_daily(&ud.join(format!("part-{i:03}.parquet")), rows, &cat_names, NO_CAT);
            })
        },
        || {
            let mut rows = Vec::new();
            for k in 0..=ncamp {
                for d in 0..ndays {
                    let a = acc[k * ndays + d];
                    if a[0] > 0 || a[3] > 0 {
                        let (id, ch): (S, S) = if k == organic { (b"organic", b"organic") } else { (camp_ids[k], camp_ch[k]) };
                        rows.push((id, ch, (ws_day + d as i64) as i32, a));
                    }
                }
            }
            out_parquet::campaign_daily(&cd.join("part-000.parquet"), &rows);
        },
    );
    let n: usize = results.iter().map(|r| r.0.len()).sum();
    log(&format!("wrote user_daily ({n} rows) and campaign_daily"));
}

mod out_parquet {
    use super::*;
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    fn props() -> Arc<WriterProperties> {
        Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .build(),
        )
    }

    macro_rules! col {
        ($rg:expr, $ty:ty, $vals:expr, $def:expr) => {{
            let mut c = $rg.next_column().unwrap().unwrap();
            c.typed::<$ty>().write_batch($vals, $def, None).unwrap();
            c.close().unwrap();
        }};
    }

    const RG: usize = 1 << 20;

    pub fn user_daily(path: &Path, rows: &[&UdRow], cats: &[S], none: u16) {
        let schema = parse_message_type(
            "message user_daily {
                REQUIRED BYTE_ARRAY user_id (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 events;
                REQUIRED INT64 revenue_cents;
                OPTIONAL BYTE_ARRAY top_category (UTF8);
            }",
        )
        .unwrap();
        let f = File::create(path).expect("create user_daily part");
        let mut w = SerializedFileWriter::new(f, Arc::new(schema), props()).unwrap();
        let cat_ba: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(bytes::Bytes::copy_from_slice(c))).collect();
        for chunk in rows.chunks(RG) {
            let mut rg = w.next_row_group().unwrap();
            // one shared buffer for the names; each value is a cheap slice of it
            let mut buf: Vec<u8> = Vec::with_capacity(chunk.len() * 12);
            let mut ends: Vec<usize> = Vec::with_capacity(chunk.len());
            for r in chunk {
                buf.extend_from_slice(r.person.bytes());
                ends.push(buf.len());
            }
            let buf = bytes::Bytes::from(buf);
            let mut start = 0;
            let v: Vec<ByteArray> = ends
                .iter()
                .map(|&e| {
                    let x = ByteArray::from(buf.slice(start..e));
                    start = e;
                    x
                })
                .collect();
            col!(rg, ByteArrayType, &v, None);
            let v: Vec<i32> = chunk.iter().map(|r| r.day).collect();
            col!(rg, Int32Type, &v, None);
            let v: Vec<i64> = chunk.iter().map(|r| r.sessions).collect();
            col!(rg, Int64Type, &v, None);
            let v: Vec<i64> = chunk.iter().map(|r| r.events).collect();
            col!(rg, Int64Type, &v, None);
            let v: Vec<i64> = chunk.iter().map(|r| r.revenue).collect();
            col!(rg, Int64Type, &v, None);
            let def: Vec<i16> = chunk.iter().map(|r| (r.top != none) as i16).collect();
            let v: Vec<ByteArray> =
                chunk.iter().filter(|r| r.top != none).map(|r| cat_ba[r.top as usize].clone()).collect();
            col!(rg, ByteArrayType, &v, Some(&def));
            rg.close().unwrap();
        }
        w.close().unwrap();
    }

    pub fn campaign_daily(path: &Path, rows: &[(S, S, i32, [i64; 4])]) {
        let schema = parse_message_type(
            "message campaign_daily {
                REQUIRED BYTE_ARRAY campaign_id (UTF8);
                REQUIRED BYTE_ARRAY channel (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 purchasing_sessions;
                REQUIRED INT64 revenue_cents;
                REQUIRED INT64 assisted_sessions;
            }",
        )
        .unwrap();
        let f = File::create(path).expect("create campaign_daily part");
        let mut w = SerializedFileWriter::new(f, Arc::new(schema), props()).unwrap();
        for chunk in rows.chunks(RG) {
            let mut rg = w.next_row_group().unwrap();
            let v: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.0.to_vec())).collect();
            col!(rg, ByteArrayType, &v, None);
            let v: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.1.to_vec())).collect();
            col!(rg, ByteArrayType, &v, None);
            let v: Vec<i32> = chunk.iter().map(|r| r.2).collect();
            col!(rg, Int32Type, &v, None);
            for k in 0..4 {
                let v: Vec<i64> = chunk.iter().map(|r| r.3[k]).collect();
                col!(rg, Int64Type, &v, None);
            }
            rg.close().unwrap();
        }
        w.close().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SIMD tokenizer must accept exactly the lines the per-line reference
    /// parser accepts, with the same fields.
    fn check<const N: usize>(seed: u64, cases: usize) {
        let alphabet = [b'a', b'b', b',', b',', b'"', b'"', b'\n', b'\r', b' ', b'1'];
        let mut x = seed;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..cases {
            let len = (rnd() % 40) as usize;
            let v: Vec<u8> = (0..len).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize]).collect();
            let chunk: S = Box::leak(v.into_boxed_slice());
            let mut want: Vec<[S; N]> = Vec::new();
            for l in lines(chunk) {
                if let Some(r) = fields::<N>(l) {
                    want.push(r);
                }
            }
            let mut got: Vec<[S; N]> = Vec::new();
            for_records::<N>(chunk, |r| got.push(r));
            assert_eq!(want, got, "chunk {:?}", String::from_utf8_lossy(chunk));
        }
    }

    #[test]
    fn tokenizer_matches_reference() {
        check::<2>(1, 200_000);
        check::<4>(2, 200_000);
        check::<7>(3, 200_000);
    }

    #[test]
    fn timestamps() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        for bad in [&b"2027-02-29T00:00:00Z"[..], b"2026-13-01T00:00:00Z", b"2026-10-03T24:00:00Z",
                    b"2026-10-03T10:00:60Z", b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+05:60",
                    b"2026-10-03T10:00:00+0200", b"2026-10-03 10:00:00Z", b"2026-10-03t10:00:00Z",
                    b"1.79e12", b"-5", b"", b"179102160000000000000000"] {
            assert_eq!(parse_ts(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), parse_ts(b"2026-10-03T10:00:00Z"));
    }
}
