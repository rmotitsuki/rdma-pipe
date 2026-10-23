/*
 * build:
 * cc -o rdrecv rdrecv.c -libverbs
 *
 * usage:
 * rdrecv [-v] <port> <key> <filename>
 *
 * Receives an RDMA transfer from rdsend and writes it to <filename>.
 *
 * When the sender provides file_size (normal file-to-file transfer), the output
 * file is mmap'd and registered as the RDMA MR directly  the NIC DMA-writes
 * into the file pages with no CPU memcpy (zero-copy).
 *
 * For unknown-size sources (file_size == 0, e.g. stdin on sender side), falls
 * back to an anonymous ring buffer + write().
 */
/*
 * rdma-pipe  RDMA transfer utilities
 *
 * Original project: Copyright 2023 Ilmari Heikkinen
 * Original project license: Apache License 2.0 (see LICENSE).
 *
 * This variant incorporates implementation changes attributed to Yuhong
 * Zhong (GitHub account: yuhong-zhong). The exact fork URL and revision used
 * as the source for those changes have not yet been independently recorded;
 * see README.md for project provenance notes.
 *
 * Local modifications for the SC26-RDMA research project include additional
 * diagnostics, argument validation, and robust handling of TCP control
 * messages. This file has been modified from the upstream version.
 */

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <time.h>
#include <unistd.h>
#include <omp.h>
#include <pthread.h>
#include <sched.h>

#include <infiniband/verbs.h>

/* TCP is a byte stream: one read()/write() is not guaranteed to transfer
 * an entire control structure. Return 0 only when all bytes are transferred. */
static int read_full(int fd, void *buf, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = read(fd, (char *)buf + done, len - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    done += (size_t)n;
  }
  return 0;
}

static int write_full(int fd, const void *buf, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = write(fd, (const char *)buf + done, len - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    done += (size_t)n;
  }
  return 0;
}

#define IO_STRIPES 8

static int pwrite_full(int fd, const void *buf, size_t len, off_t offset) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = pwrite(fd, (const char *)buf + done, len - done,
                       offset + (off_t)done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    done += (size_t)n;
  }
  return 0;
}

/* Parallel disjoint writes for complete stream chunks. pwrite() gives every
 * worker an explicit range and avoids races on the shared file position. */
static int write_chunk_parallel(int fd, const void *buf, size_t len,
                                uint64_t offset, int direct_io) {
  if (len % IO_STRIPES != 0 || offset > (uint64_t)INT64_MAX) {
    errno = EINVAL;
    return -1;
  }
  const size_t stripe_len = len / IO_STRIPES;
  int errors[IO_STRIPES] = {0};
#pragma omp parallel for schedule(static, 1)
  for (int i = 0; i < IO_STRIPES; ++i) {
    uint64_t stripe_offset = offset + (uint64_t)i * stripe_len;
    if (stripe_offset > (uint64_t)INT64_MAX) {
      errors[i] = EOVERFLOW;
    } else if (direct_io) {
      ssize_t n;
      do {
        n = pwrite(fd, (const char *)buf + (size_t)i * stripe_len,
                   stripe_len, (off_t)stripe_offset);
      } while (n < 0 && errno == EINTR);
      if (n != (ssize_t)stripe_len) errors[i] = n < 0 ? errno : EIO;
    } else if (pwrite_full(fd, (const char *)buf + (size_t)i * stripe_len,
                           stripe_len, (off_t)stripe_offset) != 0) {
      errors[i] = errno ? errno : EIO;
    }
  }
  for (int i = 0; i < IO_STRIPES; ++i) {
    if (errors[i]) { errno = errors[i]; return -1; }
  }
  return 0;
}

struct qp_info {
  uint32_t qpn;
  uint32_t psn;
  union ibv_gid gid;
};

struct rdma_params {
  uint32_t buf_size;
  uint32_t ring_size;
  uint64_t file_size;
};

static double now_sec(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + t.tv_nsec * 1e-9;
}
/*
 * Timestamps are relative to the current phase, not to process start:
 *   "setup"    from the moment the sender connected (waiting for the sender
 *              is reported once, separately);
 *   "transfer" from the moment the key was verified (the transfer clock).
 */
#define TS(label) do { if (verbose) fprintf(stderr, "TS %-8s %7.3f s  %s\n", _ts_phase, now_sec() - _ts_ref, label); } while(0)

#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif

/*
 * Destination prefault. With an ODP MR on a fresh tmpfs/page-cache file, every
 * 4 KiB page is allocated, zeroed and mapped on the NIC's page fault while the
 * data is arriving, which caps throughput (~1.5 GB/s observed vs ~3.9 GB/s on
 * a pre-existing file). The prefault worker allocates and maps the pages
 * (MADV_POPULATE_WRITE) and pre-loads the NIC translation (ibv_advise_mr
 * PREFETCH_WRITE) ahead of the receive window.
 */
struct prefault_ctx {
  char *base;
  uint64_t chunks;            /* data slots */
  uint32_t buf_size;
  struct ibv_pd *pd;
  struct ibv_mr *mr;
  uint64_t ahead;             /* max chunks ahead of consumed; 0 = unlimited */
  uint64_t next;              /* next chunk ticket (atomic) */
  uint64_t consumed;          /* chunks completed by main loop (atomic) */
  uint8_t *ready;             /* per-chunk ready flag (atomic) */
  int stop;                   /* atomic */
  int advise_ok;              /* atomic */
  int touch_fallback;         /* atomic */
};

static void prefault_range(struct prefault_ctx *pf, char *addr, size_t len) {
  if (!__atomic_load_n(&pf->touch_fallback, __ATOMIC_RELAXED) &&
      madvise(addr, len, MADV_POPULATE_WRITE) != 0 && errno == EINVAL)
    __atomic_store_n(&pf->touch_fallback, 1, __ATOMIC_RELAXED); /* < 5.14 */
  if (__atomic_load_n(&pf->touch_fallback, __ATOMIC_RELAXED)) {
    /* Content is overwritten by RDMA anyway; read-modify-write keeps it. */
    for (size_t off = 0; off < len; off += 4096) {
      volatile char *c = addr + off;
      *c = *c;
    }
  }
  if (__atomic_load_n(&pf->advise_ok, __ATOMIC_RELAXED)) {
    struct ibv_sge sg = { .addr = (uintptr_t)addr, .length = (uint32_t)len,
                          .lkey = pf->mr->lkey };
    if (ibv_advise_mr(pf->pd, IBV_ADVISE_MR_ADVICE_PREFETCH_WRITE,
                      IBV_ADVISE_MR_FLAG_FLUSH, &sg, 1) != 0)
      __atomic_store_n(&pf->advise_ok, 0, __ATOMIC_RELAXED); /* unsupported */
  }
}

static void prefault_scratch(struct prefault_ctx *pf) {
  prefault_range(pf, pf->base + pf->chunks * (uint64_t)pf->buf_size,
                 2ULL * pf->buf_size);
}

/* Several workers take chunk tickets in order; each marks its chunk ready. */
static void *prefault_worker(void *arg) {
  struct prefault_ctx *pf = arg;
  for (;;) {
    uint64_t i = __atomic_fetch_add(&pf->next, 1, __ATOMIC_ACQ_REL);
    if (i >= pf->chunks) return NULL;
    while (pf->ahead &&
           i >= __atomic_load_n(&pf->consumed, __ATOMIC_ACQUIRE) + pf->ahead) {
      if (__atomic_load_n(&pf->stop, __ATOMIC_ACQUIRE)) return NULL;
      nanosleep((const struct timespec[]){{0, 50000L}}, NULL);
    }
    if (__atomic_load_n(&pf->stop, __ATOMIC_ACQUIRE)) return NULL;
    prefault_range(pf, pf->base + i * (uint64_t)pf->buf_size, pf->buf_size);
    __atomic_store_n(&pf->ready[i], 1, __ATOMIC_RELEASE);
  }
}

/* Block until chunk `seq` has been prefaulted; returns seconds waited. */
static double prefault_wait(struct prefault_ctx *pf, uint64_t seq) {
  if (__atomic_load_n(&pf->ready[seq], __ATOMIC_ACQUIRE)) return 0.0;
  struct timespec a, b;
  clock_gettime(CLOCK_MONOTONIC, &a);
  while (!__atomic_load_n(&pf->ready[seq], __ATOMIC_ACQUIRE)) sched_yield();
  clock_gettime(CLOCK_MONOTONIC, &b);
  return (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) * 1e-9;
}

/*
 * Direct-write receive mode (disk destinations). The NIC receives into a ring
 * of registered anonymous slots; writer threads write each slot to its file
 * offset with O_DIRECT, so the device DMAs straight from the slot (no CPU copy
 * and no page-cache writeback, which caps buffered XFS writes at ~1.7 GB/s
 * from one thread). A slot is reused only after its chunk has been written,
 * which is what throttles the sender to disk speed.
 */
struct dw_ctx {
  int dfd, bfd, verbose;
  char *base;
  uint32_t buf_size, nslots;
  uint64_t chunks, file_size;
  uint64_t next;              /* ticket (atomic) */
  uint64_t received;          /* chunks received so far (atomic) */
  uint64_t *slot_written;     /* per slot: chunk+1 once written (atomic) */
  int stop;                   /* atomic */
  int err;                    /* errno of a failed write (atomic) */
  uint64_t err_offset;
  double busy_seconds;
  uint64_t chunks_written;
  pthread_mutex_t lock;
};

static void *dw_worker(void *arg) {
  struct dw_ctx *d = arg;
  double busy = 0.0;
  uint64_t n = 0;
  for (;;) {
    uint64_t i = __atomic_fetch_add(&d->next, 1, __ATOMIC_ACQ_REL);
    if (i >= d->chunks) break;
    while (__atomic_load_n(&d->received, __ATOMIC_ACQUIRE) <= i) {
      if (__atomic_load_n(&d->stop, __ATOMIC_ACQUIRE)) goto out;
      sched_yield();
    }
    uint32_t slot = (uint32_t)(i % d->nslots);
    uint64_t off = i * (uint64_t)d->buf_size;
    size_t len = d->file_size - off < d->buf_size ? (size_t)(d->file_size - off) : d->buf_size;
    char *src = d->base + (size_t)slot * d->buf_size;
    double t0 = now_sec();
    int rc;
    if (d->dfd >= 0 && len == d->buf_size) {
      rc = pwrite_full(d->dfd, src, len, (off_t)off);
      if (rc != 0) {
        if (d->verbose)
          fprintf(stderr, "rdrecv: O_DIRECT write at %llu failed (%s); retrying buffered\n",
                  (unsigned long long)off, strerror(errno));
        rc = pwrite_full(d->bfd, src, len, (off_t)off);
      }
    } else {
      /* Partial tail (unaligned length) or no O_DIRECT: buffered write. */
      rc = pwrite_full(d->bfd, src, len, (off_t)off);
    }
    if (rc != 0) {
      d->err_offset = off;
      __atomic_store_n(&d->err, errno ? errno : EIO, __ATOMIC_RELEASE);
      break;
    }
    busy += now_sec() - t0;
    n++;
    __atomic_store_n(&d->slot_written[slot], i + 1, __ATOMIC_RELEASE);
  }
out:
  pthread_mutex_lock(&d->lock);
  d->busy_seconds += busy;
  d->chunks_written += n;
  pthread_mutex_unlock(&d->lock);
  return NULL;
}

/*
 * RDMA_QP_TIMEOUT: RC local ACK timeout exponent, 4.096 us * 2^N (1..30).
 * The default 14 (~67 ms) is fine on a LAN but shorter than a long-haul RTT,
 * which causes spurious retransmissions; use a value whose timeout is at
 * least ~2x the RTT (e.g. 17 = ~537 ms for a 115 ms RTT). Set it on both
 * sides; each side applies it to its own QP.
 */
static int qp_timeout_from_env(void) {
  const char *t = getenv("RDMA_QP_TIMEOUT");
  if (!t || !*t) return 14;
  char *end = NULL;
  long v = strtol(t, &end, 10);
  if (*end != '\0' || v < 1 || v > 30) return -1;
  return (int)v;
}

/*
 * Auto-configuration from the path RTT. The kernel's TCP RTT estimate for the
 * control connection (TCP_INFO) is used, so no extra protocol messages are
 * needed. RDMA_AUTO=0 disables it (fixed defaults).
 */
static double tcp_rtt_ms(int sock) {
  struct tcp_info ti;
  socklen_t len = sizeof(ti);
  memset(&ti, 0, sizeof(ti));
  if (getsockopt(sock, IPPROTO_TCP, TCP_INFO, &ti, &len) != 0) return -1.0;
  return ti.tcpi_rtt / 1000.0;   /* microseconds -> ms */
}

static int auto_enabled(void) {
  const char *a = getenv("RDMA_AUTO");
  return !(a && (strcmp(a, "0") == 0 || strcmp(a, "off") == 0));
}

/* Smallest exponent >= 14 whose ACK timeout (4.096 us * 2^N) is >= 2 x RTT. */
static int qp_timeout_for_rtt(double rtt_ms) {
  int n = 14;
  while (n < 30 && 4.096e-3 * (double)(1UL << n) < 2.0 * rtt_ms) n++;
  return n;
}

/* Explicit RDMA_QP_TIMEOUT wins; otherwise derived from the RTT (or 14). */
static int choose_qp_timeout(double rtt, int verbose) {
  int t = qp_timeout_from_env();
  if (t < 0) return -1;
  const char *env = getenv("RDMA_QP_TIMEOUT");
  const char *why = "default";
  if (env && *env) why = "RDMA_QP_TIMEOUT";
  else if (auto_enabled() && rtt >= 0) { t = qp_timeout_for_rtt(rtt); why = "auto: >= 2 x TCP RTT"; }
  if (verbose)
    fprintf(stderr, "QP ACK timeout: %d (%.1f ms; %s; TCP RTT %.3f ms)\n", t,
            4.096e-3 * (double)(1UL << t), why, rtt);
  return t;
}

/* Wait for one completion while using the completion channel safely. */
static int wait_cq_completion(struct ibv_cq *cq,
                              struct ibv_comp_channel *channel,
                              struct ibv_wc *wc,
                              uint32_t *event_count) {
  for (;;) {
    int n = ibv_poll_cq(cq, 1, wc);
    if (n == 1) return 0;
    if (n < 0) { errno = EIO; return -1; }
    struct ibv_cq *event_cq = NULL;
    void *event_ctx = NULL;
    if (ibv_get_cq_event(channel, &event_cq, &event_ctx)) return -1;
    (void)event_cq;
    if (event_count) (*event_count)++;
    if (ibv_req_notify_cq(cq, 0)) return -1;
  }
}

#define WR_ID_KEY       UINT64_MAX
#define WR_ID_SENTINEL  (UINT64_MAX - 1ULL)
#define WR_ID_ACK_BASE  (1ULL << 63)

/* Emit progress at most once per second, rather than logging every RDMA chunk. */
static void report_progress(int verbose, uint64_t total_bytes,
                            double transfer_start, double *last_report_time,
                            uint64_t *last_report_bytes) {
  if (!verbose) return;
  double now = now_sec();
  double interval = now - *last_report_time;
  if (interval < 1.0) return;

  double interval_mib_s =
      (double)(total_bytes - *last_report_bytes) / interval / (1024.0 * 1024.0);
  double elapsed = now - transfer_start;
  double average_mib_s = elapsed > 0.0
      ? (double)total_bytes / elapsed / (1024.0 * 1024.0) : 0.0;
  fprintf(stderr,
          "Progress: %.2f GiB received; elapsed %.1f s; %.1f Gb/s (avg %.1f Gb/s; %.0f MiB/s)\n",
          (double)total_bytes / (1024.0 * 1024.0 * 1024.0), elapsed,
          interval_mib_s * 1048576.0 * 8.0 / 1e9,
          average_mib_s * 1048576.0 * 8.0 / 1e9, average_mib_s);
  *last_report_time = now;
  *last_report_bytes = total_bytes;
}

static int gid_is_roce_v2(const char *devname, uint8_t port, int gidx) {
  char path[256], typebuf[32] = {};
  snprintf(path, sizeof(path),
           "/sys/class/infiniband/%s/ports/%d/gid_attrs/types/%d",
           devname, port, gidx);
  FILE *f = fopen(path, "r");
  if (!f) return 0;
  if (!fgets(typebuf, sizeof(typebuf), f)) { fclose(f); return 0; }
  fclose(f);
  typebuf[strcspn(typebuf, "\n")] = '\0';
  return strcmp(typebuf, "RoCE v2") == 0;
}

static struct ibv_context *find_rdma_device_by_ip(uint32_t local_ip,
                                                   uint8_t *port_out,
                                                   int *gid_idx_out,
                                                   union ibv_gid *gid_out) {
  int num_devices;
  struct ibv_device **devlist = ibv_get_device_list(&num_devices);
  if (!devlist) return NULL;

  for (int i = 0; i < num_devices; i++) {
    struct ibv_context *ctx = ibv_open_device(devlist[i]);
    if (!ctx) continue;
    struct ibv_device_attr attr;
    if (ibv_query_device(ctx, &attr)) { ibv_close_device(ctx); continue; }

    for (uint8_t port = 1; port <= attr.phys_port_cnt; port++) {
      struct ibv_port_attr pattr;
      if (ibv_query_port(ctx, port, &pattr)) continue;
      if (pattr.state != IBV_PORT_ACTIVE) continue;

      for (int gidx = 0; gidx < pattr.gid_tbl_len; gidx++) {
        union ibv_gid gid;
        if (ibv_query_gid(ctx, port, gidx, &gid)) continue;
        if (gid.raw[10] == 0xFF && gid.raw[11] == 0xFF) {
          uint32_t gid_ip;
          memcpy(&gid_ip, &gid.raw[12], 4);
          if (gid_ip == local_ip &&
              gid_is_roce_v2(ibv_get_device_name(devlist[i]), port, gidx)) {
            *port_out    = port;
            *gid_idx_out = gidx;
            *gid_out     = gid;
            ibv_free_device_list(devlist);
            return ctx;
          }
        }
      }
    }
    ibv_close_device(ctx);
  }
  ibv_free_device_list(devlist);
  return NULL;
}

int main(int argc, char *argv[]) {
  double _ts0 = now_sec();
  double _ts_ref = _ts0;
  const char *_ts_phase = "setup";

  int verbose = 0, argv_idx = 1;
  if (argc < 4) { fprintf(stderr, "USAGE: rdrecv [-v] <port> <key> <filename>\n"); return 1; }
  if (strcmp(argv[argv_idx], "-v") == 0) { verbose++; argv_idx++; }

  const char *port_text = argv[argv_idx++];
  char *port_end = NULL;
  errno = 0;
  long parsed_port = strtol(port_text, &port_end, 10);
  if (errno || port_text[0] == '\0' || *port_end != '\0' ||
      parsed_port < 1 || parsed_port > 65535) {
    fprintf(stderr, "rdrecv: invalid TCP port '%s' (expected 1..65535)\n", port_text);
    return EXIT_FAILURE;
  }
  int port = (int)parsed_port;

  char    *key    = argv[argv_idx++];
  uint32_t keylen = strlen(key);
  uint32_t keyIdx = 0;

  if (argv_idx >= argc) { fprintf(stderr, "USAGE: rdrecv [-v] <port> <key> <filename>\n"); return 1; }
  const char *filename = argv[argv_idx];

  /* Open without O_TRUNC: existing pages survive for warm rewrites */
  int fd = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd < 0) {
    fprintf(stderr, "rdrecv: cannot open destination '%s': %s\n", filename, strerror(errno));
    return EXIT_FAILURE;
  }

  /* TCP: accept and receive params */
  int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_sock < 0) {
    fprintf(stderr, "rdrecv: socket creation failed: %s\n", strerror(errno));
    close(fd);
    return EXIT_FAILURE;
  }
  int optval = 1;
  if (setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0)
    fprintf(stderr, "rdrecv: setsockopt(SO_REUSEADDR) failed: %s\n", strerror(errno));
  struct sockaddr_in sin = { .sin_family = AF_INET, .sin_port = htons(port),
                             .sin_addr.s_addr = INADDR_ANY };
  if (bind(listen_sock, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
    fprintf(stderr, "rdrecv: bind to TCP port %d failed: %s\n", port, strerror(errno));
    close(listen_sock); close(fd);
    return EXIT_FAILURE;
  }
  if (listen(listen_sock, 1) < 0) {
    fprintf(stderr, "rdrecv: listen on TCP port %d failed: %s\n", port, strerror(errno));
    close(listen_sock); close(fd);
    return EXIT_FAILURE;
  }

  struct sockaddr_in peer_addr;
  socklen_t peer_len = sizeof(peer_addr);
  int tcp_sock = accept(listen_sock, (struct sockaddr *)&peer_addr, &peer_len);
  close(listen_sock);
  if (tcp_sock < 0) {
    fprintf(stderr, "rdrecv: accept failed: %s\n", strerror(errno));
    close(fd);
    return EXIT_FAILURE;
  }
  double connect_time = now_sec();
  _ts_ref = connect_time;
  if (verbose) {
    char peer_ip[INET_ADDRSTRLEN] = "?";
    inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
    fprintf(stderr, "Sender %s connected after waiting %.3f s\n",
            peer_ip, connect_time - _ts0);
  }

  struct rdma_params params;
  int params_n = read_full(tcp_sock, &params, sizeof(params));
  if (params_n != 0) {
    fprintf(stderr, "rdrecv: failed to receive transfer parameters: %s\n", errno ? strerror(errno) : "peer closed connection before sending the full structure");
    close(tcp_sock); close(fd);
    return EXIT_FAILURE;
  }
  uint32_t buf_size  = params.buf_size;
  uint32_t ring_size = params.ring_size < 2 ? 2 : params.ring_size;
  int direct_fd = -1;
  if (ring_size > 256) {
    fprintf(stderr, "rdrecv: unsupported pipeline depth %u (maximum 256)\n", ring_size);
    close(tcp_sock); close(fd);
    return EXIT_FAILURE;
  }
  if (buf_size == 0) {
    fprintf(stderr, "rdrecv: sender advertised a zero-byte RDMA buffer\n");
    close(tcp_sock); close(fd);
    return EXIT_FAILURE;
  }

  /*
   * Buffer setup:
   *   file_size > 0   zero-copy: mmap the output file as the RDMA receive
   *                   buffer; NIC DMA-writes directly into file pages.
   *   file_size == 0   fallback: anonymous ring + write().
   *
   * For zero-copy, reserve data slots plus two scratch slots:
   *   [0 .. data_slots*buf_size-1] data pages (RDMA writes land here)
   *   [data_slots*buf_size .. +buf_size-1] key/sentinel receive
   *   [data_slots*buf_size+buf_size .. +buf_size-1] stable ACK payload
   */
  char  *rdma_buf      = NULL;   /* base of RDMA MR */
  size_t rdma_buf_size = 0;
  char  *key_buf       = NULL;   /* key/sentinel scratch slot */
  char  *ack_buf       = NULL;   /* stable ACK payload slot */
  uint64_t data_slots  = 0;
  char  *file_map      = NULL;   /* non-NULL if using zero-copy */
  size_t file_map_size = 0;
  int    direct_mode   = 0;      /* ring + O_DIRECT writer threads */
  uint32_t dw_nslots   = 0;

  /* fallback ring state (used only when file_map == NULL) */
  void **slots      = NULL;
  int    recv_slot  = 0;
  int    next_recv  = 0;

  if (params.file_size > 0) {
    TS("file mmap start");
    /* A receive is posted one full buf_size ahead before the current message
     * is processed. Reserve a full scratch slot after the last data slot;
     * file_size + buf_size is insufficient when the final chunk is partial. */
    data_slots = params.file_size / buf_size +
                 ((params.file_size % buf_size) != 0);
    if (data_slots > ((uint64_t)SIZE_MAX - 2ULL * buf_size) / buf_size ||
        data_slots > ((uint64_t)INT64_MAX - 2ULL * buf_size) / buf_size) {
      fprintf(stderr, "rdrecv: destination size is too large for receive mapping\n");
      close(tcp_sock); close(fd);
      return EXIT_FAILURE;
    }
    file_map_size = (size_t)(data_slots * (uint64_t)buf_size + 2ULL * buf_size);
    if (ftruncate(fd, (off_t)file_map_size) < 0) {
      fprintf(stderr, "rdrecv: ftruncate destination to %zu bytes failed: %s\\n",
              file_map_size, strerror(errno));
      return 1;
    }
    /*
     * RDRECV_FALLOCATE = auto (default) | on | off. Reserving the blocks up
     * front avoids block allocation during writeback and fragmentation on
     * disk filesystems (it helped the NVMe-oF tests). On tmpfs it would only
     * allocate pages single-threaded under the inode lock, which the
     * parallel prefault already does faster, so `auto` skips tmpfs.
     */
    {
      const char *fa = getenv("RDRECV_FALLOCATE");
      struct statfs sfs;
      int is_tmpfs = fstatfs(fd, &sfs) == 0 && sfs.f_type == 0x01021994; /* TMPFS_MAGIC */
      int do_fa;
      if (!fa || !*fa || strcmp(fa, "auto") == 0) do_fa = !is_tmpfs;
      else if (strcmp(fa, "on") == 0 || strcmp(fa, "1") == 0) do_fa = 1;
      else if (strcmp(fa, "off") == 0 || strcmp(fa, "0") == 0) do_fa = 0;
      else {
        fprintf(stderr, "rdrecv: RDRECV_FALLOCATE must be auto, on or off\n");
        return EXIT_FAILURE;
      }
      if (do_fa) {
        TS("fallocate start");
        double fa0 = now_sec();
        if (fallocate(fd, 0, 0, (off_t)file_map_size) != 0) {
          if (verbose)
            fprintf(stderr, "rdrecv: fallocate failed (%s); continuing without it\n",
                    strerror(errno));
        } else if (verbose) {
          fprintf(stderr, "fallocate: %.3f s for %.2f GiB%s\n", now_sec() - fa0,
                  (double)file_map_size / (1024.0 * 1024.0 * 1024.0),
                  is_tmpfs ? " (tmpfs)" : "");
        }
        TS("fallocate done");
      } else if (verbose) {
        fprintf(stderr, "fallocate: skipped%s\n", is_tmpfs ? " (tmpfs)" : "");
      }
    }
    /*
     * RDRECV_MODE = auto (default) | mmap | direct.
     *   mmap:   the NIC writes straight into the mapped file (page cache).
     *           Best for tmpfs; on disk filesystems buffered writeback caps
     *           throughput (~13 Gb/s measured on XFS over a 10x NVMe RAID0).
     *   direct: ring of registered slots + O_DIRECT writer threads.
     *   auto:   mmap for tmpfs, direct otherwise.
     */
    {
      const char *mode_env = getenv("RDRECV_MODE");
      struct statfs sfs2;
      int dest_tmpfs = fstatfs(fd, &sfs2) == 0 && sfs2.f_type == 0x01021994;
      if (!mode_env || !*mode_env || strcmp(mode_env, "auto") == 0) direct_mode = !dest_tmpfs;
      else if (strcmp(mode_env, "direct") == 0) direct_mode = 1;
      else if (strcmp(mode_env, "mmap") == 0) direct_mode = 0;
      else {
        fprintf(stderr, "rdrecv: RDRECV_MODE must be auto, mmap or direct\n");
        return EXIT_FAILURE;
      }
      if (direct_mode) {
        direct_fd = open(filename, O_WRONLY | O_DIRECT);
        if (direct_fd < 0) {
          fprintf(stderr, "rdrecv: O_DIRECT unavailable on destination (%s); "
                  "using mmap mode\n", strerror(errno));
          direct_mode = 0;
        }
      }
    }
  }
  if (params.file_size > 0 && direct_mode) {
    /* RDRECV_WRITE_AHEAD: slots beyond the receive window that may be waiting
     * for (or under) disk writes; default ring_size, at most 256 MiB. */
    uint32_t wa = ring_size;
    uint32_t wa_cap = (uint32_t)((256ULL * 1024 * 1024) / buf_size);
    if (wa_cap < 2) wa_cap = 2;
    if (wa > wa_cap) wa = wa_cap;
    const char *wa_env = getenv("RDRECV_WRITE_AHEAD");
    if (wa_env && *wa_env) wa = (uint32_t)strtoul(wa_env, NULL, 10);
    if (wa < 1) wa = 1;
    dw_nslots = ring_size + wa;
    TS("direct ring mmap start");
    rdma_buf_size = (size_t)buf_size * (dw_nslots + 2ULL);
    rdma_buf = mmap(NULL, rdma_buf_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (rdma_buf == MAP_FAILED) {
      fprintf(stderr, "rdrecv: mmap of %zu-byte receive ring failed: %s\n",
              rdma_buf_size, strerror(errno));
      return 1;
    }
    madvise(rdma_buf, rdma_buf_size, MADV_HUGEPAGE);
    memset(rdma_buf, 0, rdma_buf_size);   /* fault in before pinning */
    key_buf = rdma_buf + (size_t)dw_nslots * buf_size;
    ack_buf = key_buf + buf_size;
    TS("direct ring mmap done");
    if (verbose)
      fprintf(stderr, "Receive mode: direct (%u slots = %u window + %u write-ahead, %u MiB)\n",
              dw_nslots, ring_size, wa,
              (unsigned)((uint64_t)dw_nslots * buf_size / (1024U * 1024U)));
  } else if (params.file_size > 0) {
    if (verbose) fprintf(stderr, "Receive mode: mmap (NIC writes into the mapped file)\n");
    file_map = mmap(NULL, file_map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (file_map == MAP_FAILED) {
      fprintf(stderr, "rdrecv: mmap destination '%s' (%zu bytes) failed: %s\\n",
              filename, file_map_size, strerror(errno));
      return 1;
    }
    madvise(file_map, file_map_size, MADV_HUGEPAGE);
    key_buf       = file_map + data_slots * (uint64_t)buf_size;
    ack_buf       = key_buf + buf_size;
    rdma_buf      = file_map;
    rdma_buf_size = file_map_size;
    TS("file mmap done");
  } else {
    TS("ring mmap start");
    rdma_buf_size = (size_t)buf_size * ring_size;
    rdma_buf = mmap(NULL, rdma_buf_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (rdma_buf == MAP_FAILED) { perror("mmap ring"); return 1; }
    madvise(rdma_buf, rdma_buf_size, MADV_HUGEPAGE);
    slots = malloc(ring_size * sizeof(void *));
    if (!slots) { perror("malloc"); return 1; }
    for (uint32_t s = 0; s < ring_size; s++)
      slots[s] = rdma_buf + (size_t)s * buf_size;
    key_buf    = rdma_buf;           /* key goes to slot 0 */
    recv_slot  = 0;
    next_recv  = 1 % (int)ring_size;
    TS("ring mmap done");
    direct_fd = open(filename, O_WRONLY | O_DIRECT);
    if (direct_fd < 0 && verbose)
      fprintf(stderr, "rdrecv: O_DIRECT unavailable (%s); using buffered parallel writes\n",
              strerror(errno));
  }

  /* RDMA device */
  struct sockaddr_in local_addr;
  socklen_t local_len = sizeof(local_addr);
  getsockname(tcp_sock, (struct sockaddr *)&local_addr, &local_len);

  uint8_t port_num; int gid_idx; union ibv_gid local_gid;
  struct ibv_context *ctx =
      find_rdma_device_by_ip(local_addr.sin_addr.s_addr, &port_num, &gid_idx, &local_gid);
  if (!ctx) { fprintf(stderr, "rdrecv: no active RoCEv2 GID matches the local TCP interface IP; check link state, IP assignment, and GID table\n"); return 1; }

  struct ibv_pd *pd = ibv_alloc_pd(ctx);
  if (!pd) { fprintf(stderr, "rdrecv: ibv_alloc_pd failed: %s\n", strerror(errno)); return 1; }
  struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(ctx);
  if (!comp_chan) { fprintf(stderr, "rdrecv: ibv_create_comp_channel failed: %s\n", strerror(errno)); return 1; }
  struct ibv_cq *cq = ibv_create_cq(ctx, (int)(2 * ring_size + 8), NULL, comp_chan, 0);
  if (!cq) { fprintf(stderr, "rdrecv: ibv_create_cq failed: %s\n", strerror(errno)); return 1; }
  if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdrecv: ibv_req_notify_cq failed: %s\n", strerror(errno)); return 1; }

  TS("ibv_reg_mr start");

  int mr_access =
      IBV_ACCESS_LOCAL_WRITE |
      IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_WRITE;

  /*
   * RDRECV_ODP = on (default) | off for the file-backed receive mapping.
   *   on:  On-Demand Paging; the NIC faults destination pages in as it
   *        writes (prefault below hides most of that cost).
   *   off: classic pinned registration. Works for tmpfs/shmem (the kernel
   *        refuses long-term pins of most disk-filesystem pages; then we
   *        fall back to ODP). Pages are populated by worker threads first,
   *        because pinning alone would fault 10 GiB in from one thread.
   */
  /*
   * RDRECV_ODP default `auto`: pinned for tmpfs destinations (the file must
   * fit in RAM anyway, and the pinned MR measured 97 Gb/s against 79 Gb/s
   * with ODP + ahead prefault), ODP otherwise.
   */
  int use_odp = file_map != NULL;
  if (file_map != NULL) {
    const char *o = getenv("RDRECV_ODP");
    struct statfs sfs3;
    int dst_tmpfs = fstatfs(fd, &sfs3) == 0 && sfs3.f_type == 0x01021994;
    if (!o || !*o || strcmp(o, "auto") == 0) use_odp = !dst_tmpfs;
    else if (strcmp(o, "off") == 0 || strcmp(o, "0") == 0) use_odp = 0;
    else if (strcmp(o, "on") == 0 || strcmp(o, "1") == 0) use_odp = 1;
    else {
      fprintf(stderr, "rdrecv: RDRECV_ODP must be auto, on or off\n");
      return EXIT_FAILURE;
    }
  }
  if (file_map != NULL && !use_odp) {
    int nt = 8;
    const char *t = getenv("RDRECV_PREFAULT_THREADS");
    if (t && *t) nt = atoi(t);
    if (nt < 1) nt = 1;
    if (nt > 64) nt = 64;
    struct prefault_ctx pp = {0};
    pp.base = file_map; pp.chunks = data_slots; pp.buf_size = buf_size;
    pp.ahead = 0; pp.advise_ok = 0;
    pp.ready = calloc(data_slots ? data_slots : 1, 1);
    if (!pp.ready) { fprintf(stderr, "rdrecv: out of memory\n"); return EXIT_FAILURE; }
    TS("populate before pinning start");
    double p0 = now_sec();
    prefault_scratch(&pp);
    pthread_t th[64];
    int started = 0;
    for (int i = 0; i < nt; ++i) {
      if (pthread_create(&th[i], NULL, prefault_worker, &pp) != 0) break;
      started++;
    }
    if (started == 0) prefault_worker(&pp);
    for (int i = 0; i < started; ++i) pthread_join(th[i], NULL);
    free(pp.ready);
    if (verbose)
      fprintf(stderr, "Populate before pinning: %.3f s (%d threads)%s\n",
              now_sec() - p0, started ? started : 1,
              pp.touch_fallback ? " [touch fallback]" : "");
    TS("populate before pinning done");
  }
  if (use_odp) mr_access |= IBV_ACCESS_ON_DEMAND;

  double mr0 = now_sec();
  struct ibv_mr *mr =
      ibv_reg_mr(pd, rdma_buf, rdma_buf_size, mr_access);
  if (!mr && file_map != NULL && !use_odp) {
    fprintf(stderr, "rdrecv: pinned registration of the destination failed (%s); "
            "falling back to ODP\n", strerror(errno));
    use_odp = 1;
    mr_access |= IBV_ACCESS_ON_DEMAND;
    mr0 = now_sec();
    mr = ibv_reg_mr(pd, rdma_buf, rdma_buf_size, mr_access);
  }
  if (mr && verbose && file_map != NULL)
    fprintf(stderr, "Destination MR: %s, registered in %.3f s\n",
            use_odp ? "ODP" : "pinned", now_sec() - mr0);

  if (!mr) {
    fprintf(stderr,
        "rdrecv: ibv_reg_mr failed for %zu-byte receive region: %s\n",
        rdma_buf_size, strerror(errno));
    return 1;
  }

  TS("ibv_reg_mr done");

  /*
   * RDRECV_PREFAULT = ahead (default) | upfront | off
   * RDRECV_PREFAULT_THREADS = worker threads (default 8). A single thread
   *   allocates+zeroes tmpfs pages at only ~1.9 GB/s.
   * RDRECV_PREFAULT_AHEAD = max chunks prefaulted beyond the last completed
   *   one (default 64; 0 = unlimited). Bounds extra dirty page cache when the
   *   destination is a real disk.
   */
  enum { PF_OFF, PF_AHEAD, PF_UPFRONT } pf_mode = PF_AHEAD;
  struct prefault_ctx pf = {0};
  pthread_t pf_threads[64];
  int pf_nthreads = 8, pf_started = 0;
  double pf_wait_seconds = 0.0;
  if (file_map != NULL) {
    const char *m = getenv("RDRECV_PREFAULT");
    if (m && strcmp(m, "off") == 0) pf_mode = PF_OFF;
    else if (m && strcmp(m, "upfront") == 0) pf_mode = PF_UPFRONT;
    else if (m && *m && strcmp(m, "ahead") != 0) {
      fprintf(stderr, "rdrecv: RDRECV_PREFAULT must be ahead, upfront or off\n");
      return EXIT_FAILURE;
    }
    const char *t = getenv("RDRECV_PREFAULT_THREADS");
    if (t && *t) pf_nthreads = atoi(t);
    if (pf_nthreads < 1) pf_nthreads = 1;
    if (pf_nthreads > 64) pf_nthreads = 64;
    pf.base = file_map; pf.chunks = data_slots; pf.buf_size = buf_size;
    pf.pd = pd; pf.mr = mr; pf.advise_ok = 1; pf.ahead = 64;
    const char *a = getenv("RDRECV_PREFAULT_AHEAD");
    if (a && *a) pf.ahead = strtoull(a, NULL, 10);
    if (!use_odp) pf_mode = PF_OFF;   /* already populated and pinned */
    if (pf_mode == PF_UPFRONT) pf.ahead = 0;
    /* Window + in-flight tickets must fit in the prefault distance or the
     * workers and the main loop would wait on each other. */
    if (pf.ahead && pf.ahead <= ring_size + (uint64_t)pf_nthreads)
      pf.ahead = ring_size + (uint64_t)pf_nthreads + 1;
    pf.ready = calloc(data_slots ? data_slots : 1, 1);
    if (!pf.ready) { fprintf(stderr, "rdrecv: out of memory\n"); return EXIT_FAILURE; }

    if (pf_mode != PF_OFF) {
      prefault_scratch(&pf);
      if (pf_mode == PF_UPFRONT) TS("prefault (upfront) start");
      double t0 = now_sec();
      for (int i = 0; i < pf_nthreads; ++i) {
        if (pthread_create(&pf_threads[i], NULL, prefault_worker, &pf) != 0) break;
        pf_started++;
      }
      if (pf_started == 0) {
        fprintf(stderr, "rdrecv: prefault threads failed; continuing without prefault\n");
        pf_mode = PF_OFF;
      } else if (pf_mode == PF_UPFRONT) {
        for (int i = 0; i < pf_started; ++i) pthread_join(pf_threads[i], NULL);
        pf_started = 0;
        if (verbose)
          fprintf(stderr, "Prefault (upfront): %.3f s for %.2f GiB (%.2f GB/s)\n",
                  now_sec() - t0,
                  (double)data_slots * buf_size / (1024.0 * 1024.0 * 1024.0),
                  (double)data_slots * buf_size / (now_sec() - t0) / 1e9);
        TS("prefault (upfront) done");
      }
    }
    if (verbose)
      fprintf(stderr, "Prefault mode: %s (%d threads, ahead %llu chunks)%s%s\n",
              pf_mode == PF_OFF ? "off" : pf_mode == PF_UPFRONT ? "upfront" : "ahead",
              pf_mode == PF_OFF ? 0 : pf_nthreads, (unsigned long long)pf.ahead,
              pf.touch_fallback ? " [touch fallback]" : "",
              pf_mode != PF_OFF && !pf.advise_ok ? " [no ibv_advise_mr]" : "");
  }

  struct ibv_qp_init_attr qp_attr = {
    .send_cq = cq, .recv_cq = cq, .qp_type = IBV_QPT_RC,
    .cap = { .max_send_wr=ring_size + 2, .max_send_sge=1, .max_recv_wr=ring_size + 2, .max_recv_sge=1 },
  };
  struct ibv_qp *qp = ibv_create_qp(pd, &qp_attr);
  if (!qp) { fprintf(stderr, "rdrecv: ibv_create_qp failed: %s\n", strerror(errno)); return 1; }

  struct ibv_qp_attr init_attr = {
    .qp_state = IBV_QPS_INIT, .pkey_index = 0, .port_num = port_num,
    .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
  };
  if (ibv_modify_qp(qp, &init_attr,
                    IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS)) {
    fprintf(stderr, "rdrecv: failed to move QP to INIT: %s\n", strerror(errno));
    return 1;
  }

  /* Post initial recv for key chunk */
  struct ibv_sge sge = { .addr=(uintptr_t)key_buf, .length=buf_size, .lkey=mr->lkey };
  struct ibv_recv_wr recv_wr = { .wr_id=WR_ID_KEY, .sg_list=&sge, .num_sge=1 };
  struct ibv_recv_wr *bad_recv_wr;
  struct ibv_send_wr send_wr = { .opcode=IBV_WR_SEND, .send_flags=IBV_SEND_SIGNALED,
                                  .sg_list=&sge, .num_sge=1 };
  struct ibv_send_wr *bad_send_wr;
  if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) return 1;

  /* QP handshake */
  TS("QP ready");
  uint32_t local_psn = (uint32_t)(lrand48() & 0xFFFFFF);
  struct qp_info local_info  = { .qpn=qp->qp_num, .psn=local_psn, .gid=local_gid };
  struct qp_info remote_info;
  int hs_n = write_full(tcp_sock, &local_info, sizeof(local_info));
  if (hs_n != 0) {
    fprintf(stderr, "rdrecv: TCP QP handshake write failed: %s\n", strerror(errno));
    return 1;
  }
  hs_n = read_full(tcp_sock, &remote_info, sizeof(remote_info));
  if (hs_n != 0) {
    fprintf(stderr, "rdrecv: TCP QP handshake read failed: %s\n", errno ? strerror(errno) : "peer closed connection before sending the full structure");
    return 1;
  }
  /* Read the RTT while the control socket is still open (more samples now). */
  double ctrl_rtt_ms = tcp_rtt_ms(tcp_sock);
  close(tcp_sock);

  struct ibv_qp_attr rtr_attr = {
    .qp_state=IBV_QPS_RTR, .path_mtu=IBV_MTU_4096,
    .dest_qp_num=remote_info.qpn, .rq_psn=remote_info.psn,
    .max_dest_rd_atomic=1, .min_rnr_timer=12,
    .ah_attr={ .is_global=1,
               .grh={ .dgid=remote_info.gid, .sgid_index=gid_idx, .hop_limit=64 },
               .sl=0, .port_num=port_num },
  };
  if (ibv_modify_qp(qp, &rtr_attr,
                    IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|IBV_QP_DEST_QPN|
                    IBV_QP_RQ_PSN|IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER)) {
    fprintf(stderr, "rdrecv: failed to move QP to RTR: %s (check GID, port, and MTU)\n", strerror(errno));
    return 1;
  }

  int qp_timeout = choose_qp_timeout(ctrl_rtt_ms, verbose);
  if (qp_timeout < 0) {
    fprintf(stderr, "rdrecv: invalid RDMA_QP_TIMEOUT (expected integer 1..30)\n");
    return EXIT_FAILURE;
  }
  struct ibv_qp_attr rts_attr = {
    .qp_state=IBV_QPS_RTS, .timeout=(uint8_t)qp_timeout, .retry_cnt=7, .rnr_retry=7,
    .sq_psn=local_psn, .max_rd_atomic=1,
  };
  if (ibv_modify_qp(qp, &rts_attr,
                    IBV_QP_STATE|IBV_QP_TIMEOUT|IBV_QP_RETRY_CNT|
                    IBV_QP_RNR_RETRY|IBV_QP_SQ_PSN|IBV_QP_MAX_QP_RD_ATOMIC)) {
    fprintf(stderr, "rdrecv: failed to move QP to RTS: %s\n", strerror(errno));
    return 1;
  }

  struct ibv_wc wc;
  struct ibv_cq *evt_cq; void *cq_ctx;
  uint32_t event_count = 0;
  uint64_t total_bytes = 0;

  /*
   * Timing: the clock starts only once the sender has authenticated (key
   * received and verified), i.e. at the same protocol event at which rdsend
   * starts its own clock. Waiting for the sender to connect/set up is reported
   * separately as setup time. The data clock stops when the end-of-stream
   * sentinel arrives; persistence (munmap/ftruncate/fdatasync) is measured
   * separately because the sender does not wait for it.
   */
  double transfer_start = 0.0;
  double transfer_end = 0.0;
  double last_report_time = 0.0;
  uint64_t last_report_bytes = 0;

  if (file_map) {
    /*
     * Pipelined zero-copy path for regular files. Several receives may be
     * posted simultaneously; each RDMA SEND lands directly in its final file
     * offset. ACK sends are queued independently from receive processing.
     */
    const uint32_t window = ring_size;
    const uint64_t data_chunks = data_slots;
    if (verbose) fprintf(stderr, "RDMA pipeline depth: %u chunks (%u MiB each)\n",
                         window, buf_size / (1024U * 1024U));
    struct ibv_sge ack_sge = {
      .addr=(uintptr_t)ack_buf, .length=1, .lkey=mr->lkey
    };
    struct ibv_send_wr ack_wr = {
      .wr_id=WR_ID_ACK_BASE, .opcode=IBV_WR_SEND,
      .send_flags=IBV_SEND_SIGNALED, .sg_list=&ack_sge, .num_sge=1
    };
    struct ibv_send_wr *bad_ack_wr;
    uint64_t ack_pending = 0;
    uint64_t next_to_post = 0;
    uint64_t next_to_complete = 0;
    int sentinel_posted = 0;
    int sentinel_received = 0;

    /*
     * Pipelined writeback. RDMA (ODP) writes land in the page cache without
     * going through write(), so the kernel never throttles them: the whole
     * file becomes dirty and is only flushed by the final fdatasync. Instead,
     * start writeback of every chunk as soon as it completes, and wait for the
     * chunk `wb_lag` positions behind before ACKing. That bounds dirty memory
     * to ~wb_lag chunks and propagates disk speed back to the sender, so both
     * sides measure the same (durable) transfer rate.
     * RDRECV_WRITEBACK_LAG=<chunks> overrides the lag; 0 disables it.
     */
    uint64_t wb_lag = 8;
    const char *wb_env = getenv("RDRECV_WRITEBACK_LAG");
    if (wb_env && *wb_env) wb_lag = strtoull(wb_env, NULL, 10);
    double wb_wait_seconds = 0.0;
    if (verbose) {
      if (wb_lag)
        fprintf(stderr, "Pipelined writeback: lag %llu chunks (~%llu MiB dirty max)\n",
                (unsigned long long)wb_lag,
                (unsigned long long)(wb_lag * buf_size / (1024U * 1024U)));
      else
        fprintf(stderr, "Pipelined writeback: disabled\n");
    }

    /* Complete key exchange first. */
    if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0 ||
        wc.status != IBV_WC_SUCCESS || wc.opcode != IBV_WC_RECV ||
        wc.wr_id != WR_ID_KEY) {
      fprintf(stderr, "rdrecv: failed receiving authentication key\n");
      return EXIT_FAILURE;
    }
    if (wc.byte_len != keylen + 1 || memcmp(key_buf, key, keylen + 1) != 0) {
      fprintf(stderr, "Wrong key received\n");
      return EXIT_FAILURE;
    }
    TS("transfer start (key verified)");
    transfer_start = now_sec();
    _ts_ref = transfer_start; _ts_phase = "transfer";
    last_report_time = transfer_start;
    struct ibv_sge *data_sges = calloc(window, sizeof(*data_sges));
    struct ibv_recv_wr *data_wrs = calloc(window, sizeof(*data_wrs));
    if (!data_sges || !data_wrs) {
      fprintf(stderr, "rdrecv: unable to allocate pipeline receive descriptors\n");
      free(data_sges); free(data_wrs);
      return EXIT_FAILURE;
    }

    for (uint32_t slot = 0; slot < window && next_to_post < data_chunks; ++slot) {
      if (pf_mode != PF_OFF) pf_wait_seconds += prefault_wait(&pf, next_to_post);
      data_sges[slot].addr = (uintptr_t)(file_map + next_to_post * (uint64_t)buf_size);
      data_sges[slot].length = buf_size;
      data_sges[slot].lkey = mr->lkey;
      data_wrs[slot].wr_id = next_to_post;
      data_wrs[slot].sg_list = &data_sges[slot];
      data_wrs[slot].num_sge = 1;
      if (ibv_post_recv(qp, &data_wrs[slot], &bad_recv_wr)) {
        fprintf(stderr, "rdrecv: failed posting pipeline receive %llu: %s\n",
                (unsigned long long)next_to_post, strerror(errno));
        free(data_sges); free(data_wrs);
        return EXIT_FAILURE;
      }
      next_to_post++;
    }
    if (next_to_post == data_chunks) {
      sge.addr = (uintptr_t)key_buf;
      sge.length = buf_size;
      recv_wr.wr_id = WR_ID_SENTINEL;
      if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
        fprintf(stderr, "rdrecv: failed posting final sentinel receive: %s\n", strerror(errno));
        free(data_sges); free(data_wrs);
        return EXIT_FAILURE;
      }
      sentinel_posted = 1;
    }

    /* Only release the sender once the first data receive window is ready. */
    if (ibv_post_send(qp, &ack_wr, &bad_ack_wr)) {
      fprintf(stderr, "rdrecv: failed to acknowledge authentication key: %s\n", strerror(errno));
      free(data_sges); free(data_wrs);
      return EXIT_FAILURE;
    }
    ack_pending++;

    while (!sentinel_received || ack_pending != 0) {
      if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0) {
        fprintf(stderr, "rdrecv: waiting for pipeline completion failed: %s\n", strerror(errno));
        free(data_sges); free(data_wrs);
        return EXIT_FAILURE;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "rdrecv: pipeline completion failed: %s (vendor_err=%u, wr_id=%llu)\n",
                ibv_wc_status_str(wc.status), wc.vendor_err,
                (unsigned long long)wc.wr_id);
        free(data_sges); free(data_wrs);
        return EXIT_FAILURE;
      }

      if (wc.opcode == IBV_WC_SEND) {
        if (ack_pending > 0) ack_pending--;
        continue;
      }
      if (wc.opcode != IBV_WC_RECV) continue;

      if (wc.wr_id == WR_ID_SENTINEL) {
        if (wc.byte_len != 0) {
          fprintf(stderr, "rdrecv: expected zero-length sentinel, got %u bytes\n", wc.byte_len);
          free(data_sges); free(data_wrs);
          return EXIT_FAILURE;
        }
        sentinel_received = 1;
        transfer_end = now_sec();
      } else {
        uint64_t seq = wc.wr_id;
        if (seq != next_to_complete || seq >= data_chunks) {
          fprintf(stderr, "rdrecv: out-of-order data completion (got %llu, expected %llu)\n",
                  (unsigned long long)seq, (unsigned long long)next_to_complete);
          free(data_sges); free(data_wrs);
          return EXIT_FAILURE;
        }
        uint64_t expected = params.file_size - seq * (uint64_t)buf_size;
        if (expected > buf_size) expected = buf_size;
        if (wc.byte_len != expected) {
          fprintf(stderr, "rdrecv: chunk %llu length mismatch: got %u, expected %llu\n",
                  (unsigned long long)seq, wc.byte_len, (unsigned long long)expected);
          free(data_sges); free(data_wrs);
          return EXIT_FAILURE;
        }
        total_bytes += wc.byte_len;
        next_to_complete++;

        if (wb_lag) {
          off_t off = (off_t)(seq * (uint64_t)buf_size);
          double ws = now_sec();
          if (sync_file_range(fd, off, (off_t)wc.byte_len,
                              SYNC_FILE_RANGE_WRITE) < 0) {
            fprintf(stderr, "rdrecv: sync_file_range(start) chunk %llu failed: %s\n",
                    (unsigned long long)seq, strerror(errno));
            free(data_sges); free(data_wrs);
            return EXIT_FAILURE;
          }
          if (seq >= wb_lag) {
            off_t old_off = (off_t)((seq - wb_lag) * (uint64_t)buf_size);
            if (sync_file_range(fd, old_off, (off_t)buf_size,
                                SYNC_FILE_RANGE_WAIT_BEFORE |
                                SYNC_FILE_RANGE_WRITE |
                                SYNC_FILE_RANGE_WAIT_AFTER) < 0) {
              fprintf(stderr, "rdrecv: sync_file_range(wait) chunk %llu failed: %s\n",
                      (unsigned long long)(seq - wb_lag), strerror(errno));
              free(data_sges); free(data_wrs);
              return EXIT_FAILURE;
            }
          }
          wb_wait_seconds += now_sec() - ws;   /* start + wait: both can block */
        }

        report_progress(verbose, total_bytes, transfer_start,
                        &last_report_time, &last_report_bytes);

        /* Replenish the receive window before acknowledging this chunk. */
        if (pf_mode != PF_OFF)
          __atomic_store_n(&pf.consumed, next_to_complete, __ATOMIC_RELEASE);
        if (next_to_post < data_chunks) {
          if (pf_mode != PF_OFF) pf_wait_seconds += prefault_wait(&pf, next_to_post);
          uint32_t slot = (uint32_t)(seq % window);
          data_sges[slot].addr = (uintptr_t)(file_map + next_to_post * (uint64_t)buf_size);
          data_sges[slot].length = buf_size;
          data_wrs[slot].wr_id = next_to_post;
          if (ibv_post_recv(qp, &data_wrs[slot], &bad_recv_wr)) {
            fprintf(stderr, "rdrecv: failed replenishing receive window: %s\n", strerror(errno));
            free(data_sges); free(data_wrs);
            return EXIT_FAILURE;
          }
          next_to_post++;
          if (next_to_post == data_chunks && !sentinel_posted) {
            sge.addr = (uintptr_t)key_buf;
            sge.length = buf_size;
            recv_wr.wr_id = WR_ID_SENTINEL;
            if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
              fprintf(stderr, "rdrecv: failed posting final sentinel receive: %s\n", strerror(errno));
              free(data_sges); free(data_wrs);
              return EXIT_FAILURE;
            }
            sentinel_posted = 1;
          }
        }
      }

      if (ibv_post_send(qp, &ack_wr, &bad_ack_wr)) {
        fprintf(stderr, "rdrecv: failed posting pipeline ACK: %s\n", strerror(errno));
        free(data_sges); free(data_wrs);
        return EXIT_FAILURE;
      }
      ack_pending++;
    }
    free(data_sges);
    free(data_wrs);

    if (total_bytes != params.file_size) {
      fprintf(stderr, "rdrecv: received %llu bytes, expected %llu\n",
              (unsigned long long)total_bytes,
              (unsigned long long)params.file_size);
      return EXIT_FAILURE;
    }

    TS("final sentinel received; transfer done");
    TS("transfer data complete");
    if (verbose && wb_lag)
      fprintf(stderr, "Writeback backpressure (time blocked on disk): %.3f s\n",
              wb_wait_seconds);
    if (pf_started) {
      __atomic_store_n(&pf.stop, 1, __ATOMIC_RELEASE);
      for (int i = 0; i < pf_started; ++i) pthread_join(pf_threads[i], NULL);
      pf_started = 0;
    }
    free(pf.ready);
    pf.ready = NULL;
    if (verbose && pf_mode != PF_OFF)
      fprintf(stderr, "Prefault stall (receive window waited on prefault): %.3f s%s%s\n",
              pf_wait_seconds,
              pf.touch_fallback ? " [touch fallback]" : "",
              pf.advise_ok ? "" : " [no ibv_advise_mr]");
    TS("finalization start");
    TS("munmap start");
    if (munmap(file_map, file_map_size) < 0) {
      fprintf(stderr, "rdrecv: munmap destination failed: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    TS("munmap done");
    TS("ftruncate start");
    if (ftruncate(fd, (off_t)total_bytes) < 0) {
      fprintf(stderr, "rdrecv: final ftruncate to %llu bytes failed: %s\n",
              (unsigned long long)total_bytes, strerror(errno));
      return EXIT_FAILURE;
    }
    TS("ftruncate done");

  } else if (direct_mode) {
    /*
     * Direct-write path: receives land in ring slots (chunk k in slot
     * k mod dw_nslots); writer threads write them with O_DIRECT. Before a
     * slot is re-posted for chunk k, the chunk previously in it
     * (k - dw_nslots) must be on disk.
     */
    const uint32_t window = ring_size;
    const uint64_t data_chunks = data_slots;
    if (verbose) fprintf(stderr, "RDMA pipeline depth: %u chunks (%u MiB each)\n",
                         window, buf_size / (1024U * 1024U));
    struct ibv_sge ack_sge = {
      .addr=(uintptr_t)ack_buf, .length=1, .lkey=mr->lkey
    };
    struct ibv_send_wr ack_wr = {
      .wr_id=WR_ID_ACK_BASE, .opcode=IBV_WR_SEND,
      .send_flags=IBV_SEND_SIGNALED, .sg_list=&ack_sge, .num_sge=1
    };
    struct ibv_send_wr *bad_ack_wr;
    uint64_t ack_pending = 0, next_to_post = 0, next_to_complete = 0;
    int sentinel_posted = 0, sentinel_received = 0;
    double write_wait = 0.0;

    struct dw_ctx dw = {
      .dfd = direct_fd, .bfd = fd, .verbose = verbose, .base = rdma_buf,
      .buf_size = buf_size, .nslots = dw_nslots, .chunks = data_chunks,
      .file_size = params.file_size, .lock = PTHREAD_MUTEX_INITIALIZER,
    };
    dw.slot_written = calloc(dw_nslots, sizeof(*dw.slot_written));
    struct ibv_sge *data_sges = calloc(window, sizeof(*data_sges));
    struct ibv_recv_wr *data_wrs = calloc(window, sizeof(*data_wrs));
    if (!dw.slot_written || !data_sges || !data_wrs) {
      fprintf(stderr, "rdrecv: out of memory\n");
      return EXIT_FAILURE;
    }
    /* RDRECV_WRITE_THREADS: parallel O_DIRECT writers (default 8). */
    int nwriters = 8;
    const char *wt_env = getenv("RDRECV_WRITE_THREADS");
    if (wt_env && *wt_env) nwriters = atoi(wt_env);
    if (nwriters < 1) nwriters = 1;
    if (nwriters > 64) nwriters = 64;
    if ((uint32_t)nwriters > dw_nslots) nwriters = (int)dw_nslots;
    pthread_t writers[64];
    int nw_started = 0;
    for (int i = 0; i < nwriters; ++i) {
      if (pthread_create(&writers[i], NULL, dw_worker, &dw) != 0) break;
      nw_started++;
    }
    if (nw_started == 0) {
      fprintf(stderr, "rdrecv: failed to start writer threads\n");
      return EXIT_FAILURE;
    }
    if (verbose) fprintf(stderr, "Writers: %d threads, O_DIRECT\n", nw_started);

    if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0 ||
        wc.status != IBV_WC_SUCCESS || wc.opcode != IBV_WC_RECV ||
        wc.wr_id != WR_ID_KEY) {
      fprintf(stderr, "rdrecv: failed receiving authentication key\n");
      return EXIT_FAILURE;
    }
    if (wc.byte_len != keylen + 1 || memcmp(key_buf, key, keylen + 1) != 0) {
      fprintf(stderr, "Wrong key received\n");
      return EXIT_FAILURE;
    }
    TS("transfer start (key verified)");
    transfer_start = now_sec();
    _ts_ref = transfer_start; _ts_phase = "transfer";
    last_report_time = transfer_start;

    for (uint32_t w = 0; w < window && next_to_post < data_chunks; ++w) {
      data_sges[w].addr = (uintptr_t)(rdma_buf + (size_t)(next_to_post % dw_nslots) * buf_size);
      data_sges[w].length = buf_size;
      data_sges[w].lkey = mr->lkey;
      data_wrs[w].wr_id = next_to_post;
      data_wrs[w].sg_list = &data_sges[w];
      data_wrs[w].num_sge = 1;
      if (ibv_post_recv(qp, &data_wrs[w], &bad_recv_wr)) {
        fprintf(stderr, "rdrecv: failed posting receive %llu: %s\n",
                (unsigned long long)next_to_post, strerror(errno));
        return EXIT_FAILURE;
      }
      next_to_post++;
    }
    if (next_to_post == data_chunks) {
      sge.addr = (uintptr_t)key_buf;
      sge.length = buf_size;
      recv_wr.wr_id = WR_ID_SENTINEL;
      if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
        fprintf(stderr, "rdrecv: failed posting final sentinel receive: %s\n", strerror(errno));
        return EXIT_FAILURE;
      }
      sentinel_posted = 1;
    }
    if (ibv_post_send(qp, &ack_wr, &bad_ack_wr)) {
      fprintf(stderr, "rdrecv: failed to acknowledge authentication key: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    ack_pending++;

    int failed = 0;
    while (!failed && (!sentinel_received || ack_pending != 0)) {
      if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0) {
        fprintf(stderr, "rdrecv: waiting for completion failed: %s\n", strerror(errno));
        failed = 1; break;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "rdrecv: completion failed: %s (vendor_err=%u, wr_id=%llu)\n",
                ibv_wc_status_str(wc.status), wc.vendor_err, (unsigned long long)wc.wr_id);
        failed = 1; break;
      }
      if (wc.opcode == IBV_WC_SEND) { if (ack_pending > 0) ack_pending--; continue; }
      if (wc.opcode != IBV_WC_RECV) continue;

      if (wc.wr_id == WR_ID_SENTINEL) {
        if (wc.byte_len != 0) {
          fprintf(stderr, "rdrecv: expected zero-length sentinel, got %u bytes\n", wc.byte_len);
          failed = 1; break;
        }
        sentinel_received = 1;
        transfer_end = now_sec();
      } else {
        uint64_t seq = wc.wr_id;
        if (seq != next_to_complete || seq >= data_chunks) {
          fprintf(stderr, "rdrecv: out-of-order data completion (got %llu, expected %llu)\n",
                  (unsigned long long)seq, (unsigned long long)next_to_complete);
          failed = 1; break;
        }
        uint64_t expected = params.file_size - seq * (uint64_t)buf_size;
        if (expected > buf_size) expected = buf_size;
        if (wc.byte_len != expected) {
          fprintf(stderr, "rdrecv: chunk %llu length mismatch: got %u, expected %llu\n",
                  (unsigned long long)seq, wc.byte_len, (unsigned long long)expected);
          failed = 1; break;
        }
        total_bytes += wc.byte_len;
        next_to_complete++;
        __atomic_store_n(&dw.received, next_to_complete, __ATOMIC_RELEASE);
        report_progress(verbose, total_bytes, transfer_start,
                        &last_report_time, &last_report_bytes);

        if (next_to_post < data_chunks) {
          uint32_t rs = (uint32_t)(next_to_post % dw_nslots);
          if (next_to_post >= dw_nslots) {
            uint64_t need = next_to_post - dw_nslots + 1;
            if (__atomic_load_n(&dw.slot_written[rs], __ATOMIC_ACQUIRE) != need) {
              double w0 = now_sec();
              while (__atomic_load_n(&dw.slot_written[rs], __ATOMIC_ACQUIRE) != need) {
                if (__atomic_load_n(&dw.err, __ATOMIC_ACQUIRE)) break;
                sched_yield();
              }
              write_wait += now_sec() - w0;
            }
          }
          int werr = __atomic_load_n(&dw.err, __ATOMIC_ACQUIRE);
          if (werr) {
            fprintf(stderr, "rdrecv: destination write failed at offset %llu: %s\n",
                    (unsigned long long)dw.err_offset, strerror(werr));
            failed = 1; break;
          }
          uint32_t w = (uint32_t)(seq % window);
          data_sges[w].addr = (uintptr_t)(rdma_buf + (size_t)rs * buf_size);
          data_sges[w].length = buf_size;
          data_wrs[w].wr_id = next_to_post;
          if (ibv_post_recv(qp, &data_wrs[w], &bad_recv_wr)) {
            fprintf(stderr, "rdrecv: failed replenishing receive window: %s\n", strerror(errno));
            failed = 1; break;
          }
          next_to_post++;
          if (next_to_post == data_chunks && !sentinel_posted) {
            sge.addr = (uintptr_t)key_buf;
            sge.length = buf_size;
            recv_wr.wr_id = WR_ID_SENTINEL;
            if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
              fprintf(stderr, "rdrecv: failed posting final sentinel receive: %s\n", strerror(errno));
              failed = 1; break;
            }
            sentinel_posted = 1;
          }
        }
      }
      if (ibv_post_send(qp, &ack_wr, &bad_ack_wr)) {
        fprintf(stderr, "rdrecv: failed posting ACK: %s\n", strerror(errno));
        failed = 1; break;
      }
      ack_pending++;
    }
    TS("final sentinel received; transfer done");
    TS("transfer data complete");
    if (failed) __atomic_store_n(&dw.stop, 1, __ATOMIC_RELEASE);
    TS("finalization start");
    TS("waiting for writers");
    double j0 = now_sec();
    for (int i = 0; i < nw_started; ++i) pthread_join(writers[i], NULL);
    double join_s = now_sec() - j0;
    TS("writers done");
    free(data_sges); free(data_wrs); free(dw.slot_written);
    if (failed) return EXIT_FAILURE;
    if (dw.err) {
      fprintf(stderr, "rdrecv: destination write failed at offset %llu: %s\n",
              (unsigned long long)dw.err_offset, strerror(dw.err));
      return EXIT_FAILURE;
    }
    if (total_bytes != params.file_size) {
      fprintf(stderr, "rdrecv: received %llu bytes, expected %llu\n",
              (unsigned long long)total_bytes, (unsigned long long)params.file_size);
      return EXIT_FAILURE;
    }
    if (verbose) {
      fprintf(stderr, "Write backpressure (receive slot waited on disk write): %.3f s\n",
              write_wait);
      if (dw.busy_seconds > 0)
        fprintf(stderr, "Writer: %d threads, %.3f s busy in total, %.2f GB/s (%.1f Gb/s) per thread; "
                "last writes after sentinel %.3f s\n",
                nw_started, dw.busy_seconds,
                (double)dw.chunks_written * buf_size / dw.busy_seconds / 1e9,
                (double)dw.chunks_written * buf_size * 8.0 / dw.busy_seconds / 1e9, join_s);
    }
    TS("ftruncate start");
    if (ftruncate(fd, (off_t)total_bytes) < 0) {
      fprintf(stderr, "rdrecv: final ftruncate to %llu bytes failed: %s\n",
              (unsigned long long)total_bytes, strerror(errno));
      return EXIT_FAILURE;
    }
    TS("ftruncate done");

  } else {
    /*
     * Fallback path (file_size == 0): anonymous ring + write().
     * Uses ring_size slots; key goes to slot 0.
     */
    while (1) {
      if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_ctx)) { fprintf(stderr, "rdrecv: ibv_get_cq_event (receive) failed: %s\n", strerror(errno)); return 1; }
      if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdrecv: ibv_req_notify_cq (receive) failed: %s\n", strerror(errno)); return 2; }
      int polled = ibv_poll_cq(cq, 1, &wc);
      if (polled < 1) { fprintf(stderr, "rdrecv: ibv_poll_cq returned %d waiting for receive completion\n", polled); return 3; }
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "rdrecv: receive completion failed: %s (vendor_err=%u, wr_id=%llu)\n", ibv_wc_status_str(wc.status), wc.vendor_err, (unsigned long long)wc.wr_id); return 4;
      }
      uint32_t msgLen = wc.byte_len;

      if (msgLen > 0) {
        sge.addr   = (uintptr_t)slots[next_recv];
        sge.length = buf_size;
        if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) { fprintf(stderr, "rdrecv: ibv_post_recv failed for next receive buffer: %s\n", strerror(errno)); return 5; }
      }

      sge.length = 1;
      if (ibv_post_send(qp, &send_wr, &bad_send_wr)) { fprintf(stderr, "rdrecv: ibv_post_send failed while sending ACK: %s\n", strerror(errno)); return 6; }
      sge.length = buf_size;
      if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_ctx)) { fprintf(stderr, "rdrecv: ibv_get_cq_event (ACK send) failed: %s\\n", strerror(errno)); return 7; }
      if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdrecv: ibv_req_notify_cq (ACK send) failed: %s\\n", strerror(errno)); return 8; }
      polled = ibv_poll_cq(cq, 1, &wc);
      if (polled < 1) { fprintf(stderr, "rdrecv: ibv_poll_cq returned %d waiting for ACK completion\\n", polled); return 9; }
      if (wc.status != IBV_WC_SUCCESS) { fprintf(stderr, "rdrecv: ACK send completion failed: %s (vendor_err=%u)\\n", ibv_wc_status_str(wc.status), wc.vendor_err); return 10; }
      event_count += 2;

      if (msgLen <= buf_size) {
        char *cbuf = (char *)slots[recv_slot];
        uint32_t i;
        int key_was_pending = keyIdx < keylen + 1;
        for (i = 0; i < msgLen && keyIdx < keylen+1; i++, keyIdx++, cbuf++) {
          if (*cbuf != key[keyIdx]) {
            fprintf(stderr, "Wrong key received\n");
            ibv_ack_cq_events(cq, event_count);
            return 20;
          }
        }
        if (key_was_pending && keyIdx == keylen + 1) {
          TS("transfer start (key verified)");
          transfer_start = now_sec();
          _ts_ref = transfer_start; _ts_phase = "transfer";
          last_report_time = transfer_start;
        }
        if (i == 0 && msgLen > 0) {
          int write_rc;
          if (msgLen == buf_size && buf_size % IO_STRIPES == 0 &&
              total_bytes % (buf_size / IO_STRIPES) == 0) {
            write_rc = write_chunk_parallel(direct_fd >= 0 ? direct_fd : fd,
                                            cbuf, msgLen, total_bytes,
                                            direct_fd >= 0);
            if (write_rc != 0 && direct_fd >= 0) {
              int direct_error = errno;
              write_rc = write_chunk_parallel(fd, cbuf, msgLen, total_bytes, 0);
              if (write_rc == 0 && verbose)
                fprintf(stderr, "rdrecv: O_DIRECT chunk write failed (%s); retried buffered\n",
                        strerror(direct_error));
            }
          } else {
            write_rc = pwrite_full(fd, cbuf, msgLen, (off_t)total_bytes);
          }
          if (write_rc != 0) {
            fprintf(stderr, "rdrecv: destination write failed: %s\n", strerror(errno));
            ibv_ack_cq_events(cq, event_count);
            return EXIT_FAILURE;
          }
          total_bytes += msgLen;
          report_progress(verbose, total_bytes, transfer_start,
                          &last_report_time, &last_report_bytes);
        }
      }

      if (msgLen == 0) {
        transfer_end = now_sec();
        TS("final sentinel received; transfer done");
        break;
      }

      recv_slot = next_recv;
      next_recv = (recv_slot + 1) % (int)ring_size;
    }
    TS("transfer data complete");
    TS("finalization start");
    /* Unknown-length streams may overwrite a pre-existing larger file. */
    TS("stream ftruncate start");
    if (ftruncate(fd, (off_t)total_bytes) < 0) {
      fprintf(stderr, "rdrecv: final ftruncate to %llu bytes failed: %s\n",
              (unsigned long long)total_bytes, strerror(errno));
      return EXIT_FAILURE;
    }
    TS("stream ftruncate done");
  }

  TS("fdatasync start");
  double persist_start = now_sec();
  if (fdatasync(fd) < 0) {
    fprintf(stderr, "rdrecv: fdatasync destination failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  double persist_end = now_sec();
  double persist_seconds = persist_end - persist_start;
  TS("fdatasync done");
  if (verbose) {
    if (transfer_start == 0.0) transfer_start = transfer_end; /* key never seen */
    double xfer_seconds = transfer_end - transfer_start;
    double durable_seconds = persist_end - transfer_start;
    fprintf(stderr, "Setup time (sender connected -> transfer start): %.3f s "
            "(plus %.3f s waiting for the sender)\n",
            transfer_start - connect_time, connect_time - _ts0);
    fprintf(stderr, "Transfer time (elapsed): %.3f s\n", xfer_seconds);
    fprintf(stderr, "Transferred: %llu bytes (%.2f GiB)\n", (unsigned long long)total_bytes,
            (double)total_bytes / (1024.0 * 1024.0 * 1024.0));
    fprintf(stderr, "Bandwidth %.3f GB/s = %.2f Gb/s\n",
            xfer_seconds > 0 ? (total_bytes / xfer_seconds) / 1e9 : 0.0,
            xfer_seconds > 0 ? (total_bytes * 8.0 / xfer_seconds) / 1e9 : 0.0);
    fprintf(stderr, "Finalization time (sentinel -> fdatasync done): %.3f s "
            "(fdatasync %.3f s)\n", persist_end - transfer_end, persist_seconds);
    fprintf(stderr, "Transfer + persistence time: %.3f s; durable bandwidth %.3f GB/s = %.2f Gb/s\n",
            durable_seconds,
            durable_seconds > 0 ? (total_bytes / durable_seconds) / 1e9 : 0.0,
            durable_seconds > 0 ? (total_bytes * 8.0 / durable_seconds) / 1e9 : 0.0);
  }

  TS("file close start");
  if (close(fd) < 0) {
    fprintf(stderr, "rdrecv: close destination failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  if (direct_fd >= 0) close(direct_fd);
  TS("file close done");
  free(slots);

  ibv_ack_cq_events(cq, event_count);
  ibv_destroy_qp(qp);
  ibv_dereg_mr(mr);
  ibv_destroy_cq(cq);
  ibv_dealloc_pd(pd);
  ibv_destroy_comp_channel(comp_chan);
  ibv_close_device(ctx);

  if (!file_map)
    munmap(rdma_buf, rdma_buf_size);

  TS("resource cleanup done");
  if (verbose) fprintf(stderr, "Total command time: %.3f s (%.3f s of it waiting for the sender)\n",
                       now_sec() - _ts0, connect_time - _ts0);
  return 0;
}
