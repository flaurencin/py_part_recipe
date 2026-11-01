import pytest
from py_part_recipe.spacer import (
    ChunkableSpace as CS,
    BlockChunk as BC,
    has_minmun_space,
    optimal_size,
    qualify_chunks,
)


@pytest.mark.parametrize(
    "size,block_size,expected_size", ((2, 10, 10), (12, 10, 20), (20, 10, 20))
)
def test_optimal_size(size, block_size, expected_size):
    assert optimal_size(size, block_size) == expected_size


@pytest.mark.parametrize("size,block_size", ((-2, 10), (12, -10), (-20, -10)))
def test_optimal_size_negative_params(size, block_size):
    with pytest.raises(ValueError):
        optimal_size(size, block_size)


@pytest.mark.parametrize(
    "size,block_size,expected_size", ((2, 10, 0), (12, 10, 10), (20, 10, 20))
)
def test_optimal_size_downward(size, block_size, expected_size):
    assert optimal_size(size, block_size, upward=False) == expected_size


@pytest.mark.parametrize(
    "space,block_size,chunks,has_enough_space",
    (
        (200, 10, (BC(1000, 2000, 20), BC(1000, 2000, 20)), True),
        (200, 10, (BC(1000, 2000, 20), BC(1001, 2000, 20)), False),
    ),
)
def test_has_minmun_space(space, block_size, chunks, has_enough_space):
    assert has_minmun_space(CS(space, block_size), chunks) == has_enough_space


@pytest.mark.parametrize(
    "space,chunks,chunk_sizes",
    (
        (CS(200, 10), (BC(1000, 2000, 20), BC(1000, 2000, 20)), (1000, 1000)),
        (CS(300, 10), (BC(1000, 2000, 20), BC(1000, 2000, 20)), (1500, 1500)),
        (CS(300, 10), (BC(1000, 2000, 10), BC(1000, 2000, 20)), (1330, 1670)),
        (CS(300, 10), (BC(1500, 2000, 20), BC(1000, 2000, 20)), (1660, 1340)),
        (CS(300, 10), (BC(1500, 2000, 10), BC(1000, 2000, 20)), (1600, 1400)),
        (CS(300, 10), (BC(1500, 1608, 10), BC(1000, 1106, 20)), (1600, 1100)),
        (CS(300, 10), (BC(1500, 1500, 10), BC(1000, 1000, 20)), (1500, 1000)),
        (
            CS(300_000, 4096),
            (BC(800_000_000, 1_000_000_000, 10), BC(200_000_000, 400_000_000, 20)),
            (876265472, 352534528),
        ),
    ),
)
def test_qualify_chunks(space, chunks, chunk_sizes):
    chunks = qualify_chunks(space, chunks)
    calculated_sizes = tuple((chunk.optimal_final_size for chunk in chunks))
    assert calculated_sizes == chunk_sizes
    for chunk in chunks:
        assert chunk.optimal_final_size <= chunk.optimal_max_size
        assert chunk.optimal_min_size <= chunk.optimal_final_size


# ----- no chunk goes above its maximum


MIB = 1024 * 1024


def example_sizes(space_mib):
    """Sizes of the first group of examples/example_recipe.yml (512 byte blocks)."""
    chunks = [
        BC(2 * MIB, 5 * MIB, 5000),
        BC(20 * MIB, 200 * MIB, 500),
        BC(20 * MIB, 2048 * MIB, 10),
    ]
    qualify_chunks(CS(space_mib * MIB // 512, 512), chunks)
    return tuple(chunk.optimal_final_size // MIB for chunk in chunks)


@pytest.mark.parametrize(
    "space_mib,sizes",
    (
        # the heavy chunks take their maximum, the light one gets the rest
        (511, (5, 200, 306)),
        (300, (5, 200, 95)),
        # 18 MiB above the minimums, shared as 15000 : 90000 : 20280 (weight x
        # room): 2.15, 12.99 and 2.91 MiB, nobody reaches its maximum
        (60, (4, 32, 22)),
        (42, (2, 20, 20)),
        # everything fits at its maximum
        (4096, (5, 200, 2048)),
    ),
)
def test_example_group_sizes(space_mib, sizes):
    assert example_sizes(space_mib) == sizes


def test_a_chunk_at_its_maximum_leaves_the_rest_to_the_others():
    chunks = [BC(100, 200, 1000), BC(100, 1000, 1), BC(100, 1000, 1)]
    qualify_chunks(CS(100, 10), chunks)  # 1000 octets
    sizes = tuple(chunk.optimal_final_size for chunk in chunks)
    # the first can not take more than 200, the others share the rest
    assert sizes == (200, 400, 400)


@pytest.mark.parametrize("weight", (0, -1))
def test_a_weight_must_be_above_zero(weight):
    with pytest.raises(ValueError, match="weight"):
        qualify_chunks(CS(300, 10), [BC(1000, 2000, 20), BC(1000, 2000, weight)])


def test_sizes_always_respect_the_rules():
    """Whatever the weights and the limits: aligned, between min and max, and
    all the space used when the chunks compete for it."""
    import random

    generator = random.Random(2024)
    for _ in range(2000):
        block_size = generator.choice((1, 10, 512, 4096))
        chunks = []
        for _ in range(generator.randint(1, 6)):
            low = generator.randint(1, 50) * block_size
            high = low + generator.randint(0, 400) * block_size
            chunks.append(BC(low, high, generator.choice((1, 2, 10, 500, 5000))))
        minimum = sum(chunk.min_size for chunk in chunks)
        space = CS(minimum // block_size + generator.randint(0, 600), block_size)
        qualify_chunks(space, chunks)
        for chunk in chunks:
            assert chunk.optimal_final_size % block_size == 0
            assert chunk.optimal_min_size <= chunk.optimal_final_size
            assert chunk.optimal_final_size <= chunk.optimal_max_size
        maximum = sum(chunk.optimal_max_size for chunk in chunks)
        total = sum(chunk.optimal_final_size for chunk in chunks)
        assert total == min(space.size_in_octets, maximum), (space, chunks)
