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
#include <time.h>
#include <unistd.h>
#include <omp.h>

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
#define TS(label) do { if (verbose) fprintf(stderr, "TS %.3f %s\n", now_sec() - _ts0, label); } while(0)

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
          "Progress: %.2f GiB received; elapsed %.1f s; %.1f MiB/s (avg %.1f MiB/s)\n",
          (double)total_bytes / (1024.0 * 1024.0 * 1024.0), elapsed,
          interval_mib_s, average_mib_s);
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
  if (ring_size > 8) {
    fprintf(stderr, "rdrecv: unsupported pipeline depth %u (maximum 8)\n", ring_size);
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
   * Use On-Demand Paging for the file-backed MAP_SHARED receive mapping.
   * The RDMA device and driver must support ODP.
   */
  if (file_map != NULL) {
    mr_access |= IBV_ACCESS_ON_DEMAND;
  }

  struct ibv_mr *mr =
      ibv_reg_mr(pd, rdma_buf, rdma_buf_size, mr_access);

  if (!mr) {
    fprintf(stderr,
        "rdrecv: ibv_reg_mr failed for %zu-byte receive region: %s\n",
        rdma_buf_size, strerror(errno));
    return 1;
  }

  TS("ibv_reg_mr done");

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

  struct ibv_qp_attr rts_attr = {
    .qp_state=IBV_QPS_RTS, .timeout=14, .retry_cnt=7, .rnr_retry=7,
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
    last_report_time = transfer_start;
    struct ibv_sge *data_sges = calloc(window, sizeof(*data_sges));
    struct ibv_recv_wr *data_wrs = calloc(window, sizeof(*data_wrs));
    if (!data_sges || !data_wrs) {
      fprintf(stderr, "rdrecv: unable to allocate pipeline receive descriptors\n");
      free(data_sges); free(data_wrs);
      return EXIT_FAILURE;
    }

    for (uint32_t slot = 0; slot < window && next_to_post < data_chunks; ++slot) {
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
        report_progress(verbose, total_bytes, transfer_start,
                        &last_report_time, &last_report_bytes);

        /* Replenish the receive window before acknowledging this chunk. */
        if (next_to_post < data_chunks) {
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
    fprintf(stderr, "Setup time (process start -> transfer start): %.3f s\n",
            transfer_start - _ts0);
    fprintf(stderr, "Transfer time (elapsed): %.3f s\n", xfer_seconds);
    fprintf(stderr, "Transferred: %llu bytes\n", (unsigned long long)total_bytes);
    fprintf(stderr, "Bandwidth %.3f GB/s\n",
            xfer_seconds > 0 ? (total_bytes / xfer_seconds) / 1e9 : 0.0);
    fprintf(stderr, "Finalization time (sentinel -> fdatasync done): %.3f s "
            "(fdatasync %.3f s)\n", persist_end - transfer_end, persist_seconds);
    fprintf(stderr, "Transfer + persistence time: %.3f s; durable bandwidth %.3f GB/s\n",
            durable_seconds,
            durable_seconds > 0 ? (total_bytes / durable_seconds) / 1e9 : 0.0);
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
  if (verbose) fprintf(stderr, "Total command time: %.3f s\n", now_sec() - _ts0);
  return 0;
}
