# What changed in this version of rdma-pipe

This document describes what the SC26-RDMA working copy of rdma-pipe adds to the code it was
built from. It covers the lineage of the code, the new features, the performance work, the
fixes, the measured results and the current limits.

Everything listed under "This version" was developed and measured in October 2026 on a 100 Gb/s
RoCEv2 testbed (see [Test environment](#test-environment)). Throughput figures inherited from
the upstream READMEs describe other hardware and are not results of this version.

## Lineage

| Stage | Where | What it is |
|---|---|---|
| Original | [kig/rdma-pipe](https://github.com/kig/rdma-pipe), Ilmari Heikkinen, 2018–2026 | `rdsend`, `rdrecv`, `rdcp` and `rdpipe`. Connection set up with `librdmacm` (`rdma_resolve_addr`, `rdma_resolve_route`, `rdma_connect`). Two 8 MiB buffers on the sender with one message in flight; the receiver copies each message out of its receive buffer with `write()`. Developed on InfiniBand ConnectX-3 adapters. |
| RoCEv2 fork | [yuhong-zhong/rdma-pipe](https://github.com/yuhong-zhong/rdma-pipe), Yuhong Zhong, March 2026 | Moves to plain `libibverbs` for RoCEv2: the queue pair (QP) number, PSN and GID are exchanged over a TCP socket, so `rdma_cm` and `ibacm` are no longer needed. The RoCEv2 GID is chosen to match the local IP address. 32 MiB chunks, `O_DIRECT` reads, a ring of receive buffers, and a zero-copy receiver: the destination file is memory-mapped and registered with On-Demand Paging (ODP), so the NIC writes each chunk at its final offset. |
| Base of this project | PR #1 (`7faeb49`, merged as `822d63b`), October 2026 | The fork merged and made to build, plus local hardening: argument validation (TCP port, `RDMA_BUF_SIZE` 1..2047 MiB, `RDMA_RING_SIZE` 2..8), helpers for complete TCP reads and writes, error messages that name the failed completion and vendor error, parallel `O_DIRECT` reads with OpenMP, progress reports and connection retries. |
| This version | PRs #2–#7 (merged at `28ed14b`), PR #8 in review | The changes described below. |

The original and the fork use different connection handshakes, and this version changes the
limits and defaults of the protocol parameters. Build `rdsend` and `rdrecv` from the same commit
on both hosts.

## Results

10 GiB file, measured on the [test environment](#test-environment). Gb/s = bytes × 8 / transfer
time. "End to end" includes the receiver's final `munmap`, `ftruncate` and `fdatasync`.

| Case | Base (`822d63b`) | This version |
|---|---|---|
| NVMe RAID → new file on tmpfs | 12.6 Gb/s | **97.3–97.5 Gb/s** (97.46 with no tuning variables) |
| RAM (tmpfs) → RAM (tmpfs) | — | **97.6 Gb/s** |
| tmpfs → XFS on NVMe RAID, end to end | — | **75.6 Gb/s** (fio `O_DIRECT` write ceiling of the RAID: 77.7) |
| 10 Gb/s path, 284 ms RTT, port paced at 9.4 Gb/s | — | **9.25 Gb/s** steady (8.39 average including ramp-up) |

A dash means the case was not measured on the base. Reference ceilings on the same hosts: `ib_send_bw` / `ib_write_bw` 97.7–97.9 Gb/s with one QP.
With the same 10 GiB file and destination, NVMe-oF reached about 17 Gb/s and RFTP 17–19 Gb/s.
`sha256sum` was checked in both directions.

## New features

**Direct-write receive mode for disk destinations** (`RDRECV_MODE`, `ee429e0`). Chunks land in a
ring of registered slots and writer threads (`RDRECV_WRITE_THREADS`, default 8; 16 measured best)
write them with `O_DIRECT`. A slot is reused only after it is on disk, so a slow disk slows the
sender down instead of filling the page cache. `auto` uses this mode for disk destinations and the
memory-mapped mode for tmpfs.

**Zero-copy send from tmpfs sources** (`RDSEND_ZEROCOPY`, `74e42d3`, `8e23283`). When the source
file is on tmpfs or shmem, the sender registers the file itself and sends from it, with no read
copy and no reader threads. Registration is ODP by default (`RDSEND_ZEROCOPY_MR=odp|pinned`), and
threads prefetch the pages ahead of the sends (`RDSEND_ZEROCOPY_THREADS`).

**Auto-configuration from the measured path** (`RDMA_AUTO`, `89f3fe9`, `36e3497`, `dc9a50a`). The
sender reads the RTT of the TCP control connection from the kernel (`TCP_INFO`) and the link rate
of the local interface (`RDMA_LINK_GBPS` overrides it), then:

- sizes the window to at least 1.5 × the bandwidth-delay product: 4 MiB × 64 up to about 10 ms at
  100 Gb/s, then 16 MiB chunks, up to 224 in flight (3.5 GiB);
- sets the RC ACK timeout on both sides to at least 2 × the RTT (exponent 14, about 67 ms, on a LAN);
- warns when the window is smaller than the bandwidth-delay product.

The receiver picks its mode and its memory registration from the destination file system. Every
choice can be overridden by its variable, `RDMA_AUTO=0` restores the fixed defaults (32 MiB × 4,
timeout 14), and `-v` prints what was chosen and why.

**Configurable RC ACK timeout and deeper windows** (`RDMA_QP_TIMEOUT`, `bbbbe9d`). The ACK timeout
exponent (4.096 µs × 2^N) can be set for long-haul paths, and the window can hold 2..256 chunks
(the base accepted 2..8).

**Destination block reservation** (`RDRECV_FALLOCATE`, `27f0939`). The receiver reserves the
destination blocks with `fallocate` before the transfer (`auto`: everywhere except tmpfs).

**Pinned registration of the destination** (`RDRECV_ODP`, `858a78e`, `dc9a50a`). The destination
can be registered pinned instead of with ODP. `auto` pins tmpfs destinations, which removes a
receive ceiling of about 79 Gb/s seen with ODP, and keeps ODP elsewhere.

**Timing and diagnostics** (`298ec27`, `b434c0c`, `13ca2e4`). Both sides time the transfer from the
same protocol events, so their transfer times agree within milliseconds. `-v` reports bandwidth in
Gb/s and GB/s, setup and finalization separately from the transfer, phase-relative timestamps,
read stalls on the sender, and prefault stalls and writeback backpressure on the receiver.

**Port rate pacing** (`RDSEND_PORT_RATE_GBPS`, PR #8, in review). RC queue pairs on ConnectX-6 Dx
have no per-QP rate limit, so the sender can apply the port's ETS shaper (`dcb maxrate`) for the
duration of a run and restore the previous setting at exit, Ctrl+C, SIGTERM or SIGHUP. The physical
port behind a VLAN is found automatically. Without the variable, the sender warns when the port
already has a limit.

## Performance optimizations

**Sliding send window** (`7ba1b80`). The base sent a batch of chunks, waited for every ACK and
only then read the next batch, so the link went idle on each round. The sender now posts chunk k
as soon as it has been read and the ACK of chunk k − W has arrived. Each ACK is sent only after
the receiver has posted the receive for the chunk that will use the freed slot, so the sender never
sends to a receive that is not posted.

**Parallel whole-chunk readers with read-ahead** (`d49992a`). Reader threads
(`RDSEND_READ_THREADS`, default 8) each read a whole chunk with `O_DIRECT`, so several chunks are
read at once, which a multi-NVMe RAID needs. Up to 256 MiB is read ahead of the in-flight window
(`RDSEND_READ_AHEAD`). A slot is reused only after its chunk has been sent and acknowledged.

**Destination pages prepared ahead of the data** (`RDRECV_PREFAULT`, `2359697`). With ODP, every
4 KiB page of a new destination file was allocated and zeroed by a page fault while the NIC
waited, which held the receiver near 12 Gb/s. Threads (`RDRECV_PREFAULT_THREADS`, default 8) now
allocate the pages with `MADV_POPULATE_WRITE` and load the NIC translations with
`ibv_advise_mr(PREFETCH_WRITE)`, ahead of the receive window (`ahead`, at most
`RDRECV_PREFAULT_AHEAD` chunks) or for the whole file before the start (`upfront`). For 10 GiB:
5.45 s with one thread, 0.98 s with 8, 0.44 s with 8 on a tmpfs mounted `huge=advise`.

**Writeback pipelined with the transfer** (`RDRECV_WRITEBACK_LAG`, `6064452`). In the memory-mapped
mode, each chunk starts writeback as soon as it arrives (`sync_file_range`), and the ACK of chunk k
waits for the writeback of chunk k − 8. Dirty memory stays bounded, the sender runs at disk speed,
and the final `fdatasync` on a ~0.3 GB/s disk dropped from 33 s to 0.6 s (total 46.6 s → 36.1 s).

**Completion polling on the sender.** The send loop polls the completion queue instead of sleeping
on the completion channel, so a chunk is posted the moment it is ready. This keeps one core busy.

**Block reservation on XFS.** `fallocate` alone took the memory-mapped mode from 13.4 to 71.0 Gb/s
into XFS, because writeback no longer allocates extents during the transfer.

## Fixes

- **Transfer time measured on the same events on both sides** (`298ec27`). The receiver counted
  from the moment its QP was ready, including the wait for the sender, and the sender stopped its
  clock before the receiver had written the data. Both now start at the key ACK and stop at the
  end-of-stream marker; setup and finalization are reported separately.
- **Sender no longer finishes long before the data is written** (`6064452`, `ee429e0`). With a disk
  destination, the sender reported 11.7 Gb/s while the disk wrote at 2.4 Gb/s; with backpressure
  both sides report the real rate.
- **Builds on non-x86 hosts** (`a731cc2`). The unused `-mavx` flag was removed from the Makefile.
- **Base build fixes** (PR #1). The merged fork did not compile as received.

## Configuration changes

- Default window: 4 MiB × 64 on a LAN, sized from the RTT on long paths (base: 32 MiB × 4).
  `RDMA_AUTO=0` restores the old defaults.
- `RDMA_RING_SIZE` accepts 2..256 (base: 2..8). A receiver built from the base rejects more than 8.
- New variables: `RDMA_AUTO`, `RDMA_LINK_GBPS`, `RDMA_QP_TIMEOUT`, `RDSEND_READ_THREADS`,
  `RDSEND_READ_AHEAD`, `RDSEND_ZEROCOPY`, `RDSEND_ZEROCOPY_MR`, `RDSEND_ZEROCOPY_THREADS`,
  `RDRECV_MODE`, `RDRECV_WRITE_THREADS`, `RDRECV_WRITE_AHEAD`, `RDRECV_ODP`, `RDRECV_FALLOCATE`,
  `RDRECV_PREFAULT`, `RDRECV_PREFAULT_THREADS`, `RDRECV_PREFAULT_AHEAD`, `RDRECV_WRITEBACK_LAG`,
  and `RDSEND_PORT_RATE_GBPS` (PR #8). The [README](README.md#tuning-environment-variables) has
  the full table.

## Operational findings

These are not code changes, but they decided the results:

- **NUMA placement.** Run each side on the NIC's NUMA node and put the sender's memory on a node
  with free RAM. On the sender host, the NIC's node had about 5 GB free; with memory there, reader
  threads ran at 1.37 GB/s each, against 4.8 GB/s with memory on the other node.
- **tmpfs with huge pages.** Mounting the destination tmpfs with `huge=advise` cut the final
  `munmap` of 10 GiB from 0.34 s to 0.002 s.
- **Long paths need pacing below the bottleneck.** On the 10 Gb/s, 284 ms path, a router drops
  traffic above 10 Gb/s, and RC recovers with go-back-N, resending the whole window. Pacing the
  port at 9.5 Gb/s gave 3–4 Gb/s, unstable; at 9.4 Gb/s the transfer held 9.25 Gb/s.

## Current limits

- One file per transfer; directory trees go through `rdcp`, one transfer per file.
- The key authenticates the connection; data is not encrypted.
- The memory-mapped receive mode with pinned registration needs the whole file to fit in free RAM.
- Stream mode (stdin source) uses the simpler one-message-in-flight path and was not retested after
  these changes.
- No resume and no built-in checksum.

## Test environment

Two hosts with NVIDIA ConnectX-6 Dx adapters (firmware 22.50.1002), RoCEv2 at 100 Gb/s, Ubuntu with
DOCA. Sender storage: RAID 0 of 10 PCIe 3 NVMe drives with XFS (fio: about 145 Gb/s read, 77.7 Gb/s
`O_DIRECT` write). Receiver storage: tmpfs. Long-haul tests: an international 10 Gb/s path with
284 ms RTT.

## Credits

Original code by Ilmari Heikkinen ([kig/rdma-pipe](https://github.com/kig/rdma-pipe)). RoCEv2 and
`libibverbs` implementation by Yuhong Zhong
([yuhong-zhong/rdma-pipe](https://github.com/yuhong-zhong/rdma-pipe)). SC26-RDMA changes by Rogerio
Motitsuki, developed with Claude. See [`LICENSE`](LICENSE).
