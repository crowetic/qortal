#!/usr/bin/env python3
"""Reproduce report arithmetic; not a Qortal audit or protocol implementation."""

import math


def detection_with_replacement(missing_fraction, samples):
    """Probability of at least one hit under fixed, independent uniform sampling."""
    if not 0 <= missing_fraction <= 1 or samples < 0:
        raise ValueError("invalid fraction or sample count")
    if samples == 0 or missing_fraction == 0:
        return 0.0
    if missing_fraction == 1:
        return 1.0
    return -math.expm1(samples * math.log1p(-missing_fraction))


def samples_for_detection(missing_fraction, detection_target):
    if not 0 < missing_fraction < 1 or not 0 < detection_target < 1:
        raise ValueError("fractions must lie strictly between zero and one")
    return math.ceil(math.log1p(-detection_target) / math.log1p(-missing_fraction))


def detection_without_replacement(population, missing, samples):
    """Finite population result, evaluated without constructing huge combinations."""
    if population <= 0 or not 0 <= missing <= population or not 0 <= samples <= population:
        raise ValueError("invalid population, missing count or sample count")
    if samples == 0 or missing == 0:
        return 0.0
    if samples > population - missing:
        return 1.0
    log_miss = math.fsum(math.log1p(-missing / (population - i)) for i in range(samples))
    return -math.expm1(log_miss)


def main():
    print("| Missing fraction | 32 samples | 64 samples | 128 samples | Samples for 99% detection |")
    print("| --- | --- | --- | --- | --- |")
    for fraction in (0.5, 0.1, 0.01, 0.001):
        probabilities = [detection_with_replacement(fraction, k) for k in (32, 64, 128)]
        columns = " | ".join(f"{p * 100:.4f}%" for p in probabilities)
        print(f"| {fraction * 100:g}% | {columns} | {samples_for_detection(fraction, 0.99)} |")

    print()
    print(f"One missing chunk in 512, 64 distinct samples: {100 * detection_without_replacement(512, 1, 64):.4f}% detection")
    payload_size = 256 * 1024 * 1024
    samples = 64
    for block_size in (512 * 1024, 4 * 1024):
        leaves = (payload_size + block_size - 1) // block_size
        height = (leaves - 1).bit_length()
        data_bytes = samples * block_size
        sibling_bytes = samples * height * 32
        tree_bytes = (2 * leaves - 1) * 32
        print(f"{block_size}-byte blocks: {leaves} leaves; height {height}; data {data_bytes} bytes; "
              f"unshared siblings {sibling_bytes} bytes; complete tree hashes {tree_bytes} bytes")


if __name__ == "__main__":
    main()
