from dataclasses import dataclass, field
from typing import List


@dataclass
class BlockChunk:
    min_size: int
    max_size: int
    weight: int
    optimal_min_size: int = field(default=-1, init=False)
    optimal_max_size: int = field(default=-1, init=False)
    optimal_final_size: int = field(default=-1, init=False)
    adjusted_delta: int = field(default=-1, init=False)

    @property
    def delta_max_min(self):
        if any([self.optimal_max_size == -1, self.optimal_min_size == -1]):
            raise ValueError("optimal max and min haven't been set")
        return self.optimal_max_size - self.optimal_min_size


@dataclass
class ChunkableSpace:
    nb_block: int
    block_size: int

    @property
    def size_in_octets(self):
        return self.nb_block * self.block_size


def optimal_size(size: int, block_size: int, upward: bool = True) -> int:
    """Calculate optimal size based on specified block size.
    example:
        size is 10 block size is 8, appriatesize upward is 2 and downward is 1

    Args:
        size (int): expected size
        block_size (int): size matching a multiple of block size
        upward (bool, optional): find closest upward value else find closest downward value. Defaults to True.

    Raises:
        ValueError: in case of absurd values

    Returns:
        int: size matching a multiple of block size
    """
    size = int(size)
    block_size = int(block_size)
    if any([size < 1, block_size < 1]):
        raise ValueError(
            "To determine the optimal size all parameters in size, block_size must be > 0"
        )
    if size % block_size == 0:
        return size // block_size * block_size
    else:
        nb_blocks = size // block_size + 1 if upward else size // block_size
        final_size = nb_blocks * block_size
        return final_size


def has_minmun_space(block_space: ChunkableSpace, chunks: List[BlockChunk]) -> bool:
    """Checks that given chunks propsective at minimal value can fit in given space

    Args:
        block_space (ChunkableSpace): Available space description
        chunks (List[BlockChunk]): Chunks prospective to partition give block_space

    Returns:
        bool:
            True if sufficient space is available for prospectives set to minimum
            chunk size
            Else False
    """
    chunks_sizes: List[int] = [
        optimal_size(chunk.min_size, block_space.block_size) for chunk in chunks
    ]
    minimal_space = sum(chunks_sizes)
    return block_space.block_size * block_space.nb_block >= minimal_space


def _share_free_space(chunks: List[BlockChunk], free: float) -> List[float]:
    """Share `free` octets above their minimum between the chunks.

    Each chunk gets a part proportional to weight x (max - min), but never
    more than its maximum: a chunk that would go above it takes only what it
    can, and what it leaves is shared again between the others, until
    everything is shared or every chunk is at its maximum.
    """
    extra = [0.0] * len(chunks)
    active = [i for i, chunk in enumerate(chunks) if chunk.delta_max_min > 0]
    while active and free > 0:
        total = sum(chunks[i].weight * chunks[i].delta_max_min for i in active)
        proposed = {
            i: free * chunks[i].weight * chunks[i].delta_max_min / total for i in active
        }
        too_much = [i for i in active if proposed[i] > chunks[i].delta_max_min]
        if not too_much:
            for i in active:
                extra[i] = proposed[i]
            break
        for i in too_much:
            extra[i] = chunks[i].delta_max_min
            free -= extra[i]
        active = [i for i in active if i not in too_much]
    return extra


def qualify_chunks(
    block_space: ChunkableSpace, chunks: List[BlockChunk]
) -> List[BlockChunk]:
    """Cut available space using given hints about min_size, max_size, and weight

    When everything fits at its maximum size, each chunk gets its maximum.
    Otherwise each chunk starts at its minimum and the space left is shared in
    proportion to weight x (max_size - min_size), without any chunk going over
    its max_size. All the sizes are multiples of the block size and the whole
    space is used.

    Args:
        block_space (ChunkableSpace): space to be partitionned
        chunks (List[BlockChunk]): unqulified chunks yet with rules

    Raises:
        ValueError: if space is missing to respond to the minimum settings of the
            chunks, or a weight is not above 0

    Returns:
        List[BlockChunk]: qualified chunks, with final size
    """
    for chunk in chunks:
        if chunk.weight <= 0:
            raise ValueError(
                f"the weight of a chunk must be above 0, got {chunk.weight}"
            )
    if not has_minmun_space(block_space, chunks):
        raise ValueError("not enough space in block_space")
    for chunk in chunks:
        chunk.optimal_min_size = optimal_size(chunk.min_size, block_space.block_size)
        chunk.optimal_max_size = optimal_size(
            chunk.max_size, block_space.block_size, upward=False
        )
    sum_max_chunks = sum([chunk.optimal_max_size for chunk in chunks])
    if block_space.size_in_octets >= sum_max_chunks:
        for chunk in chunks:
            chunk.optimal_final_size = chunk.optimal_max_size
        return chunks

    # competition for the space: the minimums first, the rest is shared
    sum_min_chunks = sum([chunk.optimal_min_size for chunk in chunks])
    extra = _share_free_space(chunks, block_space.size_in_octets - sum_min_chunks)
    for chunk, part in zip(chunks, extra):
        chunk.adjusted_delta = int(part)
        chunk.optimal_final_size = optimal_size(
            chunk.optimal_min_size + part, block_space.block_size, upward=False
        )
    # what rounding to blocks left goes to the last chunks that have room
    left = block_space.size_in_octets - sum(
        chunk.optimal_final_size for chunk in chunks
    )
    for chunk in reversed(chunks):
        added = min(left, chunk.optimal_max_size - chunk.optimal_final_size)
        chunk.optimal_final_size += added
        left -= added
    # a float that rounded up by a block would use more than the space: take
    # it back from the last chunks that are above their minimum
    for chunk in reversed(chunks):
        taken = min(-left, chunk.optimal_final_size - chunk.optimal_min_size)
        if taken > 0:
            chunk.optimal_final_size -= taken
            left += taken
    return chunks
