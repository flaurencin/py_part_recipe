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
    size_in_blocks: int
    block_size: int

    @property
    def size_in_octets(self):
        return self.size_in_blocks * self.block_size


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
    return block_space.block_size * block_space.size_in_blocks >= minimal_space


def qualify_chunks(
    block_space: ChunkableSpace, chunks: List[BlockChunk]
) -> List[BlockChunk]:
    """Cut available space using given hints about min_size, max_size, and weight

    Args:
        block_space (ChunkableSpace): space to be partitionned
        chunks (List[BlockChunk]): unqulified chunks yet with rules

    Raises:
        ValueError: if spsce is missing to respond to the minimum settings of the chunk

    Returns:
        List[BlockChunk]: qualified chunks, with final size
    """
    if not has_minmun_space(block_space, chunks):
        raise ValueError("not enough space in block_space")
    for chunk in chunks:
        chunk.optimal_min_size = optimal_size(chunk.min_size, block_space.block_size)
        chunk.optimal_max_size = optimal_size(
            chunk.max_size, block_space.block_size, upward=False
        )
    sum_max_chunks = sum([chunk.optimal_max_size for chunk in chunks])
    minimum_free_space = block_space.size_in_octets - sum_max_chunks
    if minimum_free_space < 0:
        competition_for_space = True
        minimum_free_space = 0
    else:
        competition_for_space = False
    if not competition_for_space:
        for chunk in chunks:
            chunk.optimal_final_size = chunk.optimal_max_size
        return chunks

    sum_weight = sum([chunk.weight for chunk in chunks])
    for chunk in chunks:
        chunk.adjusted_delta = round(chunk.delta_max_min * chunk.weight / sum_weight)

    sum_deltas = sum([chunk.adjusted_delta for chunk in chunks])
    sum_min_chunks = sum([chunk.optimal_min_size for chunk in chunks])
    remaiming_space = block_space.size_in_octets - sum_min_chunks
    for index, chunk in enumerate(chunks):
        factor = chunk.adjusted_delta / sum_deltas
        if index == len(chunks) - 1:
            used_space = sum(
                [ch.optimal_final_size for ch in chunks if ch.optimal_final_size != -1]
            )
            chunk.optimal_final_size = block_space.size_in_octets - used_space
            return chunks
        chunk.optimal_final_size = optimal_size(
            (chunk.optimal_min_size + remaiming_space * factor),
            block_space.block_size,
            upward=False,
        )
    return chunks
