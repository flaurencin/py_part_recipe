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
