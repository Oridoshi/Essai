wit_bindgen::generate!({
    world: "primes",
    path: "../wit",
});

use exports::demo::primes::calcul::Guest;

struct Component;

impl Guest for Component {
    fn how_many_prime_in_range(start: u64, end: u64) -> u64 {
        count_primes(start, end)
    }
}

export!(Component);

// --- logique pure, testable sans Wasm ---

fn count_primes(start: u64, end: u64) -> u64 {
    let mut primes = 0;
    for n in start..=end {
        if is_prime(n) { primes += 1; }
    }
    primes
}

fn is_prime(n: u64) -> bool {
    if n <= 1 { return false; }
    for a in 2..=(n as f64).sqrt() as u64 {
        if n % a == 0 { return false; }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_stresse_calcul_works() {
        assert_eq!(count_primes(0, 100_000), 9_592);
        assert_eq!(count_primes(100_000, 1_000_000), 68_906);
        assert_eq!(count_primes(1_000, 1_000_000), 78_330);
    }
}