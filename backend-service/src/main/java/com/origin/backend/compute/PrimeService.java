package com.origin.backend.compute;

import org.springframework.stereotype.Service;

/**
 * CPU-heavy prime counting.
 *
 * CPU loader for CPU to react. Trial division is
 * pure CPU and scales predictably with the input.
 */
@Service
public class PrimeService {

    public long countPrimesUpTo(int limit) {
        long count = 0;
        for (int n = 2; n <= limit; n++) {
            if (isPrime(n)) {
                count++;
            }
        }
        return count;
    }

    private boolean isPrime(int n) {
        if (n < 2) {
            return false;
        }
        for (int divisor = 2; (long) divisor * divisor <= n; divisor++) {
            if (n % divisor == 0) {
                return false;
            }
        }
        return true;
    }
}
