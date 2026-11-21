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
//!             -> valid kept events, mapped to their person, scattered by hash(person)
//!   4. clicks -> parse + validate -> mapped to their person, scattered by hash(person)
//!   5. per person bucket: sort each person's events and clicks, sessionize,
//!      credit and assists with one sliding 7-day window over the clicks
//!   6. write Parquet
//! Every string is a slice of the mapped input: nothing is copied until output.

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
    chunk.split(|&b| b == b'\n')
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

/// A string from the input, copied inline when it is short (every generated ID is),
/// so later phases never reach back into the mapped files: that random access is
/// what made the zero-copy version page-fault-bound. Longer strings keep a pointer
/// to the input. Equality, hashing and ordering are on the bytes, so exact.
#[derive(Clone, Copy)]
struct K {
    n: u8, // length if inline (<= 23), LONG otherwise
    b: [u8; 23],
}
const LONG: u8 = 255;

impl K {
    #[inline]
    fn new(s: S) -> K {
        let mut k = K { n: 0, b: [0; 23] };
        if s.len() <= 23 {
            k.n = s.len() as u8;
            k.b[..s.len()].copy_from_slice(s);
        } else {
            k.n = LONG;
            k.b[..8].copy_from_slice(&(s.as_ptr() as usize as u64).to_le_bytes());
            k.b[8..16].copy_from_slice(&(s.len() as u64).to_le_bytes());
        }
        k
    }
    #[inline]
    fn bytes(&self) -> &[u8] {
        if self.n != LONG {
            &self.b[..self.n as usize]
        } else {
            let p = u64::from_le_bytes(self.b[..8].try_into().unwrap()) as usize as *const u8;
            let n = u64::from_le_bytes(self.b[8..16].try_into().unwrap()) as usize;
            unsafe { std::slice::from_raw_parts(p, n) } // into a leaked, never-unmapped map
        }
    }
}
impl PartialEq for K {
    #[inline]
    fn eq(&self, o: &K) -> bool {
        self.bytes() == o.bytes()
    }
}
impl Eq for K {}
impl std::hash::Hash for K {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.bytes().hash(h)
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

#[derive(Clone, Copy)]
struct Ev {
    person: K,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Clk {
    person: K,
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
    let mut products: HashMap<S, u16> = HashMap::new();
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
                products.entry(pid).or_insert(ci);
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
    let mut campaigns: HashMap<S, u16> = HashMap::new();
    let mut cchunks = Vec::new();
    chunks_of(map_file(&data.join("campaigns.csv")), usize::MAX, &mut cchunks);
    for c in &cchunks {
        for line in lines(c) {
            if let Some([cid, ch]) = fields::<2>(line) {
                if cid.is_empty() || campaigns.contains_key(cid) {
                    continue;
                }
                campaigns.insert(cid, camp_ids.len() as u16);
                camp_ids.push(cid);
                camp_ch.push(ch);
            }
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp; // accumulator slot for organic
    log(&format!("{} products, {} categories, {ncamp} campaigns", products.len(), cat_names.len()));

    // ---- map every input; events and clicks start parsing while links resolve
    const CH: usize = 8 << 20;
    let (link_chunks, (ev_chunks, clk_chunks)) = rayon::join(
        || load_chunks(&data.join("links"), CH),
        || rayon::join(|| load_chunks(&data.join("events"), CH), || load_chunks(&data.join("clicks"), CH)),
    );
    log(&format!("mapped: {} event, {} click, {} link chunks", ev_chunks.len(), clk_chunks.len(), link_chunks.len()));

    let part = foldhash::fast::FixedState::with_seed(0x5eed_1234_abcd_0001);
    let bucket = |k: &[u8]| (part.hash_one(k) as usize) % NB;

    // ---- links: union-find; each linked ID -> its person's name
    let (link_map, ev_buckets) = rayon::join(
        || {
            let pairs: Vec<Vec<(S, S)>> = link_chunks
                .par_iter()
                .map(|c| {
                    lines(c)
                        .filter_map(|l| fields::<2>(l))
                        .filter(|[a, b]| !a.is_empty() && !b.is_empty())
                        .map(|[a, b]| (a, b))
                        .collect()
                })
                .collect();
            let mut idx: HashMap<S, u32> = HashMap::new();
            let mut names: Vec<S> = Vec::new();
            let mut parent: Vec<u32> = Vec::new();
            fn find(p: &mut [u32], mut x: u32) -> u32 {
                while p[x as usize] != x {
                    let g = p[p[x as usize] as usize];
                    p[x as usize] = g;
                    x = g;
                }
                x
            }
            let mut id_of = |s: S, names: &mut Vec<S>, parent: &mut Vec<u32>| -> u32 {
                *idx.entry(s).or_insert_with(|| {
                    names.push(s);
                    parent.push(parent.len() as u32);
                    (names.len() - 1) as u32
                })
            };
            for v in &pairs {
                for &(a, b) in v {
                    let ia = id_of(a, &mut names, &mut parent);
                    let ib = id_of(b, &mut names, &mut parent);
                    let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
                    if ra != rb {
                        // the root keeps the byte-wise smallest name of the merged component
                        let (keep, drop) = if names[ra as usize] <= names[rb as usize] { (ra, rb) } else { (rb, ra) };
                        parent[drop as usize] = keep;
                    }
                }
            }
            let mut map: HashMap<K, K> = HashMap::with_capacity(idx.len());
            for (s, i) in idx.iter() {
                let r = find(&mut parent, *i);
                map.insert(K::new(s), K::new(names[r as usize]));
            }
            log(&format!("links: {} linked ids", map.len()));
            map
        },
        || {
            // parse and judge every event copy; scatter by event_id
            let per_chunk: Vec<Scattered<RawEv>> = ev_chunks
                .par_iter()
                .map(|c| {
                    let est = c.len() / 60 + 16;
                    let mut items: Vec<RawEv> = Vec::with_capacity(est);
                    let mut keys: Vec<u16> = Vec::with_capacity(est);
                    for line in lines(c) {
                        let Some([id, user, ts, ing, et, prod, price]) = fields::<7>(line) else { continue };
                        if id.is_empty() {
                            continue;
                        }
                        let t = parse_ts(ts);
                        let g = parse_ts(ing);
                        let is_p = et == b"purchase";
                        let type_ok = is_p || et == b"view" || et == b"cart";
                        let pr = if is_p { parse_price(price) } else { Some(0) };
                        let ok = match (t, g) {
                            (Some(t), Some(g)) => {
                                !user.is_empty() && t >= ws && t < we && type_ok && !prod.is_empty() && pr.is_some()
                                    && g - t <= DAY
                            }
                            _ => false,
                        };
                        let cat = if ok && is_p { *products.get(prod).unwrap_or(&unknown_cat) } else { NO_CAT };
                        keys.push(bucket(id) as u16);
                        items.push(RawEv {
                            id: K::new(id),
                            user: K::new(user),
                            ts: t.unwrap_or(0),
                            ing: g.unwrap_or(i64::MAX),
                            price: if ok && is_p { pr.unwrap() } else { 0 },
                            cat,
                            ok,
                        });
                    }
                    Scattered::new(items, keys)
                })
                .collect();
            log("events parsed");
            per_chunk
        },
    );

    // ---- dedup events (smallest ingest_ts wins; equal ingest_ts copies are
    //      identical), map to persons, scatter by person
    let person_of = |u: K| -> K { *link_map.get(&u).unwrap_or(&u) };
    let ev_by_person: Vec<Scattered<Ev>> = (0..NB)
        .into_par_iter()
        .map(|p| {
            let n: usize = ev_buckets.iter().map(|c| c.get(p).len()).sum();
            let mut refs: Vec<&RawEv> = Vec::with_capacity(n);
            let mut best: HashMap<&[u8], u32> = HashMap::with_capacity(n);
            for c in &ev_buckets {
                for r in c.get(p) {
                    match best.entry(r.id.bytes()) {
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
                    let person = person_of(r.user);
                    keys.push(bucket(person.bytes()) as u16);
                    items.push(Ev { person, ts: r.ts, price: r.price, cat: r.cat });
                }
            }
            Scattered::new(items, keys)
        })
        .collect();
    drop(ev_buckets);
    log("events deduped");

    // ---- clicks: no dedup needed (copies are identical and nothing below is
    //      sensitive to them), validate, map to persons, scatter by person
    let clk_by_person: Vec<Scattered<Clk>> = clk_chunks
        .par_iter()
        .map(|c| {
            let est = c.len() / 45 + 16;
            let mut items: Vec<Clk> = Vec::with_capacity(est);
            let mut keys: Vec<u16> = Vec::with_capacity(est);
            for line in lines(c) {
                let Some([id, user, camp, ts]) = fields::<4>(line) else { continue };
                if id.is_empty() || user.is_empty() {
                    continue;
                }
                let Some(t) = parse_ts(ts) else { continue };
                if t < ws - WEEK || t >= we {
                    continue;
                }
                let Some(&ci) = campaigns.get(camp) else { continue };
                let person = person_of(K::new(user));
                keys.push(bucket(person.bytes()) as u16);
                items.push(Clk { person, id: K::new(id), ts: t, camp: ci });
            }
            Scattered::new(items, keys)
        })
        .collect();
    log("clicks parsed");

    // ---- per person: sessions, credit, assists
    let nslot = (ncamp + 1) * ndays;
    let results: Vec<(Vec<UdRow>, Vec<[i64; 4]>)> = (0..NB)
        .into_par_iter()
        .fold(|| (Vec::new(), vec![[0i64; 4]; nslot]), |(mut rows, mut acc), q| {
            // group this bucket's events and clicks by person
            let mut pid: HashMap<K, u32> = HashMap::new();
            let mut evs: Vec<(u32, Ev)> = Vec::new();
            for src in &ev_by_person {
                for e in src.get(q) {
                    let n = pid.len() as u32;
                    let i = *pid.entry(e.person).or_insert(n);
                    evs.push((i, *e));
                }
            }
            let mut clks: Vec<(u32, Clk)> = Vec::new();
            for src in &clk_by_person {
                for c in src.get(q) {
                    if let Some(&i) = pid.get(&c.person) {
                        clks.push((i, *c)); // clicks of persons without events credit nothing
                    }
                }
            }
            evs.sort_unstable_by_key(|(i, e)| (*i, e.ts));
            clks.sort_unstable_by(|(i, a), (j, b)| (i, a.ts, a.id).cmp(&(j, b.ts, b.id)));

            // acc slots: credited, purchasing, revenue, assisted
            let mut cnt = vec![0u32; ncamp];
            let mut pos = vec![0u32; ncamp];
            let mut active: Vec<u16> = Vec::new();
            let mut catsum: Vec<(u16, i64)> = Vec::new();

            let mut ei = 0;
            let mut ci = 0;
            while ei < evs.len() {
                let me = evs[ei].0;
                let mut ee = ei;
                while ee < evs.len() && evs[ee].0 == me {
                    ee += 1;
                }
                while ci < clks.len() && clks[ci].0 < me {
                    ci += 1;
                }
                let mut ce = ci;
                while ce < clks.len() && clks[ce].0 == me {
                    ce += 1;
                }
                let e = &evs[ei..ee];
                let c = &clks[ci..ce];
                let person = e[0].1.person;

                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day = i64::MIN;
                let mut row = UdRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
                let flush = |row: &mut UdRow, catsum: &mut Vec<(u16, i64)>, rows: &mut Vec<UdRow>| {
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

                let mut i = 0;
                while i < e.len() {
                    // one session: events while the gap to the previous one is <= 30 min
                    let start = e[i].1.ts;
                    let mut last = start;
                    let mut j = i;
                    let mut rev = 0i64;
                    let mut hasp = false;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        flush(&mut row, &mut catsum, &mut rows);
                        row = UdRow { person, day: day as i32, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
                        cur_day = day;
                    }
                    while j < e.len() && e[j].1.ts - last <= GAP {
                        let x = &e[j].1;
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
                    while hi < c.len() && c[hi].1.ts <= start {
                        let k = c[hi].1.camp as usize;
                        if cnt[k] == 0 {
                            pos[k] = active.len() as u32;
                            active.push(k as u16);
                        }
                        cnt[k] += 1;
                        hi += 1;
                    }
                    while lo < hi && c[lo].1.ts < start - WEEK {
                        let k = c[lo].1.camp as usize;
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
                        let t = c[hi - 1].1.ts;
                        let mut k = hi - 1;
                        while k > lo && c[k - 1].1.ts == t {
                            k -= 1;
                        }
                        c[k].1.camp as usize
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
                flush(&mut row, &mut catsum, &mut rows);
                // empty the window for the next person
                while lo < hi {
                    let k = c[lo].1.camp as usize;
                    cnt[k] -= 1;
                    lo += 1;
                }
                active.clear();
                ei = ee;
                ci = ce;
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
        for (q, (rows, _)) in results.iter().enumerate() {
            g[q % ngroups].extend(rows.iter());
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
        let cat_ba: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.to_vec())).collect();
        for chunk in rows.chunks(RG) {
            let mut rg = w.next_row_group().unwrap();
            let v: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.person.bytes().to_vec())).collect();
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
