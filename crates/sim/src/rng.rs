use sys::Rng;

pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimRng {
    s: [u64; 4],
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let mut sm = seed;
        SimRng {
            s: std::array::from_fn(|_| splitmix64(&mut sm)),
        }
    }

    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo)
    }

    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

impl Rng for SimRng {
    fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_reference_vector() {
        let mut s = 0;
        assert_eq!(splitmix64(&mut s), 0xe220_a839_7b1d_cdaf);
        assert_eq!(splitmix64(&mut s), 0x6e78_9e6a_a1b9_65f4);
    }

    #[test]
    fn same_seed_same_stream() {
        let a: Vec<u64> = {
            let mut r = SimRng::new(42);
            (0..100).map(|_| r.next_u64()).collect()
        };
        let mut r = SimRng::new(42);
        assert!(a.iter().all(|&x| x == r.next_u64()));
        assert_ne!(SimRng::new(43).next_u64(), a[0]);
    }

    #[test]
    fn range_and_chance_bounds() {
        let mut r = SimRng::new(7);
        for _ in 0..10_000 {
            let x = r.range(5, 9);
            assert!((5..9).contains(&x));
        }
        assert_eq!(r.range(3, 3), 3);
        assert!(!(0..1000).any(|_| r.chance(0.0)));
        assert!((0..1000).all(|_| r.chance(1.0)));
        let hits = (0..100_000).filter(|_| r.chance(0.1)).count();
        assert!((9_000..11_000).contains(&hits), "{hits}");
    }
}
