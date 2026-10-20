/*
 * cc -o rdsend rdsend.c -libverbs
 *
 * usage:
 * rdsend [-v] <server> <port> <key> [filename]
 *
 * Reads data from stdin and RDMA sends it to the given server and port,
 * authenticating with the key.
 *
 * Uses a TCP socket for QP handshake to avoid ibacm dependency.
 *
 * Environment variables (sender decides, receiver follows):
 *   RDMA_BUF_SIZE   - chunk size in MB (default 32)
 *   RDMA_RING_SIZE  - receiver ring depth (default 4)
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
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
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

struct qp_info {
  uint32_t qpn;
  uint32_t psn;
  union ibv_gid gid;
};

/* Sent by rdsend to rdrecv before RDMA setup so rdrecv can allocate correctly */
struct rdma_params {
  uint32_t buf_size;
  uint32_t ring_size;
  uint64_t file_size;  /* 0 = unknown (stdin source) */
};

static double now_sec(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + t.tv_nsec * 1e-9;
}
#define TS(label) do { if (verbose) fprintf(stderr, "TS %.3f %s\n", now_sec() - _ts0, label); } while (0)

/* Emit transfer progress at most once per second when verbose logging is enabled. */
static void report_send_progress(int verbose, uint64_t total_bytes,
                                 double transfer_start, double *last_report_time,
                                 uint64_t *last_report_bytes) {
  if (!verbose) return;
  double now = now_sec();
  double interval = now - *last_report_time;
  if (interval < 1.0) return;
  double interval_mib_s = (double)(total_bytes - *last_report_bytes) /
                          interval / (1024.0 * 1024.0);
  double elapsed = now - transfer_start;
  double average_mib_s = elapsed > 0.0
      ? (double)total_bytes / elapsed / (1024.0 * 1024.0) : 0.0;
  fprintf(stderr,
          "Progress: %.2f GiB sent; elapsed %.1f s; %.1f MiB/s (avg %.1f MiB/s)\n",
          (double)total_bytes / (1024.0 * 1024.0 * 1024.0), elapsed,
          interval_mib_s, average_mib_s);
  *last_report_time = now;
  *last_report_bytes = total_bytes;
}

void usage() {
  fprintf(stderr, "USAGE: rdsend [-v] <server> <port> <key> [filename]\n");
}

// Returns 1 if GID at (devname, port, gidx) is of type "RoCE v2"
static int gid_is_roce_v2(const char *devname, uint8_t port, int gidx) {
  char path[256];
  char typebuf[32] = {};
  snprintf(path, sizeof(path),
           "/sys/class/infiniband/%s/ports/%d/gid_attrs/types/%d",
           devname, port, gidx);
  FILE *f = fopen(path, "r");
  if (!f) return 0;
  if (!fgets(typebuf, sizeof(typebuf), f)) { fclose(f); return 0; }
  fclose(f);
  // strip trailing newline
  typebuf[strcspn(typebuf, "\n")] = '\0';
  return strcmp(typebuf, "RoCE v2") == 0;
}

// Find the ibv_context and port for the device whose RoCEv2 GID matches the
// local IP used to route to target_ip.
struct ibv_context *find_rdma_device(const char *target_ip, uint8_t *port_out,
                                     int *gid_idx_out, union ibv_gid *gid_out, int verbose) {
  // Find which local IP routes to target
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (sock < 0) return NULL;
  struct sockaddr_in remote = {
      .sin_family = AF_INET,
      .sin_port = htons(1),
  };
  inet_aton(target_ip, &remote.sin_addr);
  if (connect(sock, (struct sockaddr *)&remote, sizeof(remote)) < 0) {
    close(sock);
    return NULL;
  }
  struct sockaddr_in local;
  socklen_t slen = sizeof(local);
  getsockname(sock, (struct sockaddr *)&local, &slen);
  close(sock);
  uint32_t local_ip = local.sin_addr.s_addr; // network byte order

  if (verbose) fprintf(stderr, "DEBUG: local_ip = %08x (looking for RoCEv2 GID)\n", local_ip);

  int num_devices;
  struct ibv_device **devlist = ibv_get_device_list(&num_devices);
  if (!devlist) return NULL;

  for (int i = 0; i < num_devices; i++) {
    if (verbose) fprintf(stderr, "DEBUG: checking device %s\n", ibv_get_device_name(devlist[i]));
    struct ibv_context *ctx = ibv_open_device(devlist[i]);
    if (!ctx) continue;

    struct ibv_device_attr attr;
    if (ibv_query_device(ctx, &attr)) {
      ibv_close_device(ctx);
      continue;
    }

    for (uint8_t port = 1; port <= attr.phys_port_cnt; port++) {
      struct ibv_port_attr pattr;
      if (ibv_query_port(ctx, port, &pattr)) continue;
      if (pattr.state != IBV_PORT_ACTIVE) continue;

      for (int gidx = 0; gidx < pattr.gid_tbl_len; gidx++) {
        union ibv_gid gid;
        if (ibv_query_gid(ctx, port, gidx, &gid)) continue;
        // RoCEv2 IPv4-mapped GID: bytes 0-9 are zero, 10-11 are 0xFF,
        // bytes 12-15 are the IPv4 address.
        int is_roce_v2 = gid_is_roce_v2(ibv_get_device_name(devlist[i]), port, gidx);
        if (gid.raw[10] == 0xFF && gid.raw[11] == 0xFF) {
          uint32_t gid_ip;
          memcpy(&gid_ip, &gid.raw[12], 4);
          if (verbose) fprintf(stderr, "DEBUG:  gid[%d]: ip=%08x local=%08x roce_v2=%d\n",
                  gidx, gid_ip, local_ip, is_roce_v2);
          if (gid_ip == local_ip && is_roce_v2) {
            *port_out = port;
            *gid_idx_out = gidx;
            *gid_out = gid;
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

/* Read a regular file in independent, contiguous stripes. Each worker owns
 * one disjoint destination range; pread() avoids shared file-offset races.
 * O_DIRECT reads are used only for complete chunks, with a buffered fallback
 * for short/error cases and for the final potentially unaligned tail. */
#define IO_STRIPES 8

static int pread_full_or_eof(int fd, void *buf, size_t len, off_t offset,
                             size_t *bytes_out) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = pread(fd, (char *)buf + done, len - done,
                      offset + (off_t)done);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) return -1;
    if (n == 0) break;
    done += (size_t)n;
  }
  *bytes_out = done;
  return 0;
}

static int read_file_parallel(int fd, void *buf, uint32_t buf_size,
                              uint64_t offset, int direct_io,
                              uint64_t *bytes_out) {
  if (buf_size % IO_STRIPES != 0) {
    errno = EINVAL;
    return -1;
  }
  const size_t stripe_size = buf_size / IO_STRIPES;
  size_t stripe_bytes[IO_STRIPES] = {0};
  int stripe_errors[IO_STRIPES] = {0};

#pragma omp parallel for schedule(static, 1)
  for (int i = 0; i < IO_STRIPES; ++i) {
    uint64_t stripe_offset = offset + (uint64_t)i * stripe_size;
    if (stripe_offset > (uint64_t)INT64_MAX) {
      stripe_errors[i] = EOVERFLOW;
      continue;
    }
    if (direct_io) {
      ssize_t n;
      do {
        n = pread(fd, (char *)buf + (size_t)i * stripe_size,
                  stripe_size, (off_t)stripe_offset);
      } while (n < 0 && errno == EINTR);
      if (n != (ssize_t)stripe_size) stripe_errors[i] = n < 0 ? errno : EIO;
      else stripe_bytes[i] = (size_t)n;
    } else if (pread_full_or_eof(fd, (char *)buf + (size_t)i * stripe_size,
                                 stripe_size, (off_t)stripe_offset,
                                 &stripe_bytes[i]) != 0) {
      stripe_errors[i] = errno ? errno : EIO;
    }
  }

  uint64_t total = 0;
  for (int i = 0; i < IO_STRIPES; ++i) {
    if (stripe_errors[i]) {
      errno = stripe_errors[i];
      return -1;
    }
    total += stripe_bytes[i];
  }
  *bytes_out = total;
  return 0;
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

int main(int argc, char *argv[]) {
  double _ts0 = now_sec();
  struct ibv_context *ctx = NULL;
  struct ibv_pd *pd = NULL;
  struct ibv_comp_channel *comp_chan = NULL;
  struct ibv_cq *cq = NULL;
  struct ibv_cq *evt_cq = NULL;
  struct ibv_mr *mr = NULL;
  struct ibv_qp *qp = NULL;
  struct ibv_sge sge, rsge;
  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  struct ibv_recv_wr recv_wr = {};
  struct ibv_recv_wr *bad_recv_wr;
  struct ibv_wc wc;
  void *cq_context;

  uint32_t *buf, *buf2, *tmp;

  uint32_t event_count = 0;
  struct timespec now, tmstart;
  double seconds;

  uint64_t total_bytes, buf_read_bytes;
  double transfer_start = 0.0, last_report_time = 0.0;
  uint64_t last_report_bytes = 0;
  int wr_id = 1, more_to_send = 1;
  uint32_t buf_len = 0;

  char *host, *ports;
  int port;
  char *key;
  uint32_t keylen;

  int verbose = 0;
  int argv_idx = 1;

  if (argc < 4) {
    usage();
    return 1;
  }

  if (strcmp(argv[argv_idx], "-v") == 0) {
    verbose++;
    argv_idx++;
  }

  host = argv[argv_idx++];
  ports = argv[argv_idx++];
  char *port_end = NULL;
  errno = 0;
  long parsed_port = strtol(ports, &port_end, 10);
  if (errno || ports[0] == '\0' || *port_end != '\0' ||
      parsed_port < 1 || parsed_port > 65535) {
    fprintf(stderr, "rdsend: invalid TCP port '%s' (expected 1..65535)\n", ports);
    usage();
    return EXIT_FAILURE;
  }
  port = (int)parsed_port;

  key = argv[argv_idx++];
  keylen = strlen(key);

  /* Sender decides buf_size and ring_size; these are sent to rdrecv before
   * RDMA setup so the receiver can allocate accordingly. */
  struct rdma_params params = {
      .buf_size  = 32 * 1024 * 1024,
      .ring_size = 4,
  };
  const char *bs_env = getenv("RDMA_BUF_SIZE");
  if (bs_env) {
    char *end = NULL;
    errno = 0;
    long mb = strtol(bs_env, &end, 10);
    if (errno || bs_env[0] == '\0' || *end != '\0' ||
        mb < 1 || (unsigned long)mb > (UINT32_MAX - 4U) / (2U * 1024U * 1024U)) {
      fprintf(stderr, "rdsend: invalid RDMA_BUF_SIZE='%s' (expected 1..2047 MB)\n", bs_env);
      return EXIT_FAILURE;
    }
    params.buf_size = (uint32_t)mb * 1024U * 1024U;
  }
  const char *rs_env = getenv("RDMA_RING_SIZE");
  if (rs_env) {
    char *end = NULL;
    errno = 0;
    long ring = strtol(rs_env, &end, 10);
    if (errno || rs_env[0] == '\0' || *end != '\0' ||
        ring < 2 || ring > 8) {
      fprintf(stderr, "rdsend: invalid RDMA_RING_SIZE='%s' (expected integer 2..8)\n", rs_env);
      return EXIT_FAILURE;
    }
    params.ring_size = (uint32_t)ring;
  }
  uint32_t buf_size = params.buf_size;

  int fd = STDIN_FILENO;
  int direct_fd = -1;
  if (argv_idx < argc) {
    fd = open(argv[argv_idx], O_RDONLY);
    if (fd < 0) {
      fprintf(stderr, "rdsend: cannot open input file '%s': %s\n",
              argv[argv_idx], strerror(errno));
      return EXIT_FAILURE;
    }
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
      if (st.st_size < 0) {
        fprintf(stderr, "rdsend: input file has a negative size\n");
        close(fd);
        return EXIT_FAILURE;
      }
      params.file_size = (uint64_t)st.st_size;
      if (params.file_size > 0) {
        direct_fd = open(argv[argv_idx], O_RDONLY | O_DIRECT);
        if (direct_fd < 0 && verbose)
          fprintf(stderr, "rdsend: O_DIRECT unavailable (%s); using buffered parallel reads\n",
                  strerror(errno));
      }
    } else if (fstat(fd, &st) < 0) {
      fprintf(stderr, "rdsend: fstat input file '%s' failed: %s\n",
              argv[argv_idx], strerror(errno));
      close(fd);
      return EXIT_FAILURE;
    }
  }

  /* Connect TCP and send params to rdrecv before allocating buffers.
   * This lets rdrecv allocate the right size ring immediately. */
  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
  };
  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    fprintf(stderr, "rdsend: target '%s' is not a numeric IPv4 address; use the receiver's IPv4 address\n", host);
    if (fd != STDIN_FILENO) close(fd);
    return EXIT_FAILURE;
  }

  int tcp_sock = -1;
  int retries = 0;
  int last_connect_errno = 0;
  while (tcp_sock < 0) {
    int candidate = socket(AF_INET, SOCK_STREAM, 0);
    if (candidate < 0) {
      fprintf(stderr, "rdsend: socket creation failed: %s\n", strerror(errno));
      if (fd != STDIN_FILENO) close(fd);
      return EXIT_FAILURE;
    }
    if (connect(candidate, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
      tcp_sock = candidate;
      break;
    }
    last_connect_errno = errno;
    close(candidate);
    retries++;
    if (retries > 300) {
      fprintf(stderr, "rdsend: connection to %s:%d failed after %d attempts: %s\n",
              host, port, retries, strerror(last_connect_errno));
      if (fd != STDIN_FILENO) close(fd);
      return EXIT_FAILURE;
    }
    nanosleep((const struct timespec[]){{0, 10000000L}}, NULL);
  }

  int params_sent = write_full(tcp_sock, &params, sizeof(params));
  if (params_sent != 0) {
    fprintf(stderr, "rdsend: failed to send transfer parameters: %s\n", strerror(errno));
    close(tcp_sock);
    if (fd != STDIN_FILENO) close(fd);
    return EXIT_FAILURE;
  }

  /* Pipelined send slots plus one 4-byte ACK receive area per slot. */
  if (buf_size > (UINT32_MAX - params.ring_size * 4U) / params.ring_size) {
    fprintf(stderr, "rdsend: RDMA buffer ring is too large\n");
    return EXIT_FAILURE;
  }
  buf_len = buf_size * params.ring_size + params.ring_size * 4U;
  buf = mmap(NULL, buf_len, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (buf == MAP_FAILED) { fprintf(stderr, "rdsend: mmap of %u-byte send buffer failed: %s\n", buf_len, strerror(errno)); return 113; }
  madvise(buf, buf_len, MADV_HUGEPAGE);
  memset(buf, 0, buf_len);  /* prefault before ibv_reg_mr */
  buf2 = (uint32_t *)(((char *)buf) + buf_size);

  // Find RDMA device for target
  uint8_t port_num;
  int gid_idx;
  union ibv_gid local_gid;
  TS("RDMA device discovery start");
  ctx = find_rdma_device(host, &port_num, &gid_idx, &local_gid, verbose);
  TS("RDMA device discovery done");
  if (!ctx) {
    fprintf(stderr, "rdsend: no active RoCEv2 GID matches the interface used to reach %s; check link state, IP assignment, and GID table\n", host);
    return 101;
  }

  pd = ibv_alloc_pd(ctx);
  if (!pd) { fprintf(stderr, "rdsend: ibv_alloc_pd failed: %s\n", strerror(errno)); return 109; }

  comp_chan = ibv_create_comp_channel(ctx);
  if (!comp_chan) { fprintf(stderr, "rdsend: ibv_create_comp_channel failed: %s\n", strerror(errno)); return 110; }

  cq = ibv_create_cq(ctx, (int)(2 * params.ring_size + 8), NULL, comp_chan, 0);
  if (!cq) { fprintf(stderr, "rdsend: ibv_create_cq failed: %s\n", strerror(errno)); return 111; }

  if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdsend: ibv_req_notify_cq failed: %s\n", strerror(errno)); return 112; }

  mr = ibv_reg_mr(pd, buf, buf_len, IBV_ACCESS_LOCAL_WRITE);
  if (!mr) { fprintf(stderr, "rdsend: ibv_reg_mr failed for %u-byte buffer: %s; check RLIMIT_MEMLOCK and pinned-memory limits\n", buf_len, strerror(errno)); return 99; }

  struct ibv_qp_init_attr qp_init = {
      .send_cq = cq,
      .recv_cq = cq,
      .qp_type = IBV_QPT_RC,
      .cap = {.max_send_wr = params.ring_size + 2,
              .max_send_sge = 1,
              .max_recv_wr = params.ring_size + 2,
              .max_recv_sge = 1},
  };
  qp = ibv_create_qp(pd, &qp_init);
  if (!qp) { fprintf(stderr, "rdsend: ibv_create_qp failed: %s\n", strerror(errno)); return 114; }

  // Move QP to INIT
  struct ibv_qp_attr init_attr = {
      .qp_state = IBV_QPS_INIT,
      .pkey_index = 0,
      .port_num = port_num,
      .qp_access_flags = 0,
  };
  if (ibv_modify_qp(qp, &init_attr,
                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS)) {
    fprintf(stderr, "rdsend: failed to move QP to INIT: %s\n", strerror(errno));
    return 115;
  }

  /* Exchange QP info over the same TCP connection */
  uint32_t local_psn = (uint32_t)(lrand48() & 0xFFFFFF);
  struct qp_info local_info = {
      .qpn = qp->qp_num,
      .psn = local_psn,
      .gid = local_gid,
  };
  struct qp_info remote_info;

  // rdrecv sends first, then waits for ours
  int n = read_full(tcp_sock, &remote_info, sizeof(remote_info));
  if (n != 0) {
    fprintf(stderr, "rdsend: TCP QP handshake read failed: %s\n", errno ? strerror(errno) : "peer closed connection before sending the full structure");
    return 116;
  }
  n = write_full(tcp_sock, &local_info, sizeof(local_info));
  if (n != 0) {
    fprintf(stderr, "rdsend: TCP QP handshake write failed: %s\n", strerror(errno));
    return 117;
  }
  close(tcp_sock);

  // Move QP to RTR
  struct ibv_qp_attr rtr_attr = {
      .qp_state = IBV_QPS_RTR,
      .path_mtu = IBV_MTU_4096,
      .dest_qp_num = remote_info.qpn,
      .rq_psn = remote_info.psn,
      .max_dest_rd_atomic = 1,
      .min_rnr_timer = 12,
      .ah_attr =
          {
              .is_global = 1,
              .grh =
                  {
                      .dgid = remote_info.gid,
                      .sgid_index = gid_idx,
                      .hop_limit = 64,
                  },
              .sl = 0,
              .port_num = port_num,
          },
  };
  if (ibv_modify_qp(qp, &rtr_attr,
                    IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
    fprintf(stderr, "rdsend: failed to move QP to RTR: %s (check GID, port, and MTU)\n", strerror(errno));
    return 118;
  }

  // Move QP to RTS
  struct ibv_qp_attr rts_attr = {
      .qp_state = IBV_QPS_RTS,
      .timeout = 14,
      .retry_cnt = 7,
      .rnr_retry = 7,
      .sq_psn = local_psn,
      .max_rd_atomic = 1,
  };
  if (ibv_modify_qp(qp, &rts_attr,
                    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                        IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                        IBV_QP_MAX_QP_RD_ATOMIC)) {
    fprintf(stderr, "rdsend: failed to move QP to RTS: %s\n", strerror(errno));
    return 119;
  }

  /* Prepost recv for ACK */
  rsge.addr = (uintptr_t)(((char *)buf) + 2 * buf_size);
  rsge.length = 4;
  rsge.lkey = mr->lkey;

  recv_wr.wr_id = 0;
  recv_wr.sg_list = &rsge;
  recv_wr.num_sge = 1;

  clock_gettime(CLOCK_MONOTONIC, &tmstart);
  TS("transfer start");
  transfer_start = now_sec();
  last_report_time = transfer_start;
  last_report_bytes = 0;
  total_bytes = 0;

  memcpy((void *)buf, key, keylen + 1);
  buf_read_bytes = keylen + 1;
  total_bytes = 0;
  more_to_send = 1;

  if (params.file_size > 0) {
    const uint32_t window = params.ring_size;
    if (verbose) fprintf(stderr, "RDMA pipeline depth: %u chunks (%u MiB each)\n",
                         window, buf_size / (1024U * 1024U));
    char *send_base = (char *)buf;
    char *ack_base = send_base + (size_t)window * buf_size;
    uint64_t chunks = params.file_size / buf_size +
                      ((params.file_size % buf_size) != 0);

    /* Send and acknowledge the authentication key before opening the data window. */
    struct ibv_sge ack_sge = { .addr=(uintptr_t)ack_base, .length=4, .lkey=mr->lkey };
    struct ibv_recv_wr ack_recv = { .wr_id=0, .sg_list=&ack_sge, .num_sge=1 };
    if (ibv_post_recv(qp, &ack_recv, &bad_recv_wr)) {
      fprintf(stderr, "rdsend: failed to post key ACK receive: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    sge.addr = (uintptr_t)send_base;
    sge.length = keylen + 1;
    sge.lkey = mr->lkey;
    send_wr.wr_id = 1;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
      fprintf(stderr, "rdsend: failed to send authentication key: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    int key_send_done = 0, key_ack_done = 0;
    while (!key_send_done || !key_ack_done) {
      if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0) {
        fprintf(stderr, "rdsend: waiting for key completion failed: %s\n", strerror(errno));
        return EXIT_FAILURE;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "rdsend: key completion failed: %s\n", ibv_wc_status_str(wc.status));
        return EXIT_FAILURE;
      }
      if (wc.opcode == IBV_WC_RECV) key_ack_done = 1;
      else if (wc.opcode == IBV_WC_SEND) key_send_done = 1;
    }

    total_bytes = 0;
    uint64_t seq = 0;
    while (seq < chunks) {
      uint32_t batch = (uint32_t)((chunks - seq) < window ? (chunks - seq) : window);
      for (uint32_t j = 0; j < batch; ++j) {
        uint64_t chunk_seq = seq + j;
        uint32_t slot = j;
        char *slot_buf = send_base + (size_t)slot * buf_size;
        uint64_t offset = chunk_seq * (uint64_t)buf_size;
        uint64_t remaining = params.file_size - offset;
        uint64_t bytes = 0;
        if (direct_fd >= 0 && remaining < buf_size) {
          size_t tail_bytes = 0;
          if (pread_full_or_eof(fd, slot_buf, (size_t)remaining,
                                (off_t)offset, &tail_bytes) != 0) {
            fprintf(stderr, "rdsend: final buffered read failed at offset %llu: %s\n",
                    (unsigned long long)offset, strerror(errno));
            return EXIT_FAILURE;
          }
          bytes = tail_bytes;
        } else if (read_file_parallel(direct_fd >= 0 ? direct_fd : fd,
                                      slot_buf, buf_size, offset,
                                      direct_fd >= 0, &bytes) != 0) {
          int direct_error = errno;
          if (direct_fd >= 0 && read_file_parallel(fd, slot_buf, buf_size,
                                                   offset, 0, &bytes) == 0) {
            if (verbose) fprintf(stderr, "rdsend: O_DIRECT chunk read failed (%s); retried buffered\n",
                                 strerror(direct_error));
          } else {
            fprintf(stderr, "rdsend: parallel file read failed at offset %llu: %s\n",
                    (unsigned long long)offset, strerror(errno));
            return EXIT_FAILURE;
          }
        }
        if (bytes == 0 || bytes > buf_size) {
          fprintf(stderr, "rdsend: unexpected read length %llu at offset %llu\n",
                  (unsigned long long)bytes, (unsigned long long)offset);
          return EXIT_FAILURE;
        }
        ack_sge.addr = (uintptr_t)(ack_base + (size_t)slot * 4U);
        ack_sge.length = 4;
        ack_recv.wr_id = chunk_seq;
        if (ibv_post_recv(qp, &ack_recv, &bad_recv_wr)) {
          fprintf(stderr, "rdsend: failed to post ACK receive for chunk %llu: %s\n",
                  (unsigned long long)chunk_seq, strerror(errno));
          return EXIT_FAILURE;
        }
        sge.addr = (uintptr_t)slot_buf;
        sge.length = (uint32_t)bytes;
        send_wr.wr_id = chunk_seq + 2;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
          fprintf(stderr, "rdsend: failed to post data chunk %llu: %s\n",
                  (unsigned long long)chunk_seq, strerror(errno));
          return EXIT_FAILURE;
        }
        total_bytes += bytes;
      }

      uint32_t send_done = 0, ack_done = 0;
      while (send_done < batch || ack_done < batch) {
        if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0) {
          fprintf(stderr, "rdsend: waiting for pipeline completion failed: %s\n", strerror(errno));
          return EXIT_FAILURE;
        }
        if (wc.status != IBV_WC_SUCCESS) {
          fprintf(stderr, "rdsend: pipeline completion failed: %s (vendor_err=%u)\n",
                  ibv_wc_status_str(wc.status), wc.vendor_err);
          return EXIT_FAILURE;
        }
        if (wc.opcode == IBV_WC_RECV) ack_done++;
        else if (wc.opcode == IBV_WC_SEND) send_done++;
      }
      report_send_progress(verbose, total_bytes, transfer_start,
                           &last_report_time, &last_report_bytes);
      seq += batch;
    }

    /* End-of-stream sentinel remains ordered after all data sends. */
    ack_sge.addr = (uintptr_t)ack_base;
    ack_sge.length = 4;
    ack_recv.wr_id = UINT64_MAX;
    if (ibv_post_recv(qp, &ack_recv, &bad_recv_wr)) {
      fprintf(stderr, "rdsend: failed to post sentinel ACK receive: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    sge.addr = (uintptr_t)send_base;
    sge.length = 0;
    send_wr.wr_id = UINT64_MAX;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
      fprintf(stderr, "rdsend: failed to post sentinel: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    int sentinel_send_done = 0, sentinel_ack_done = 0;
    while (!sentinel_send_done || !sentinel_ack_done) {
      if (wait_cq_completion(cq, comp_chan, &wc, &event_count) != 0) {
        fprintf(stderr, "rdsend: waiting for sentinel completion failed: %s\n", strerror(errno));
        return EXIT_FAILURE;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "rdsend: sentinel completion failed: %s\n", ibv_wc_status_str(wc.status));
        return EXIT_FAILURE;
      }
      if (wc.opcode == IBV_WC_RECV) sentinel_ack_done = 1;
      else if (wc.opcode == IBV_WC_SEND) sentinel_send_done = 1;
    }
  } else {
  while (more_to_send) {
    if (buf_read_bytes == 0) {
      more_to_send = 0;
    }

    if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) { fprintf(stderr, "rdsend: ibv_post_recv failed while posting ACK receive: %s\n", strerror(errno)); return 1; }

    sge.addr = (uintptr_t)buf;
    sge.length = buf_read_bytes;
    sge.lkey = mr->lkey;

    send_wr.wr_id = wr_id;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;

    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) { fprintf(stderr, "rdsend: ibv_post_send failed: %s\n", strerror(errno)); return 1; }

    tmp = buf;
    buf = buf2;
    buf2 = tmp;

    if (params.file_size > 0) {
      uint64_t bytes = 0;
      uint64_t remaining = params.file_size - total_bytes;
      if (direct_fd >= 0 && remaining < buf_size) {
        /* O_DIRECT tail lengths may violate device alignment; use buffered I/O. */
        size_t tail_bytes = 0;
        if (pread_full_or_eof(fd, buf, (size_t)remaining,
                              (off_t)total_bytes, &tail_bytes) != 0) {
          fprintf(stderr, "rdsend: final buffered read failed at offset %llu: %s\n",
                  (unsigned long long)total_bytes, strerror(errno));
          return EXIT_FAILURE;
        }
        bytes = tail_bytes;
      } else if (read_file_parallel(direct_fd >= 0 ? direct_fd : fd, buf,
                                    buf_size, total_bytes, direct_fd >= 0,
                                    &bytes) != 0) {
        int direct_error = errno;
        if (direct_fd >= 0 &&
            read_file_parallel(fd, buf, buf_size, total_bytes, 0, &bytes) == 0) {
          if (verbose)
            fprintf(stderr, "rdsend: O_DIRECT chunk read failed (%s); retried buffered\n",
                    strerror(direct_error));
        } else {
          fprintf(stderr, "rdsend: parallel file read failed at offset %llu: %s\n",
                  (unsigned long long)total_bytes, strerror(errno));
          return EXIT_FAILURE;
        }
      }
      buf_read_bytes = bytes;
    } else {
      /* stdin and non-regular sources are streams: preserve sequential reads. */
      ssize_t r;
      do { r = read(fd, buf, buf_size); } while (r < 0 && errno == EINTR);
      if (r < 0) {
        fprintf(stderr, "rdsend: read from stream failed: %s\n", strerror(errno));
        return EXIT_FAILURE;
      }
      buf_read_bytes = (uint64_t)r;
    }
    total_bytes += buf_read_bytes;

    /* Wait for send completion */
    if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context)) { fprintf(stderr, "rdsend: ibv_get_cq_event (send completion) failed: %s\n", strerror(errno)); return 2; }
    if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdsend: ibv_req_notify_cq (send completion) failed: %s\n", strerror(errno)); return 3; }
    int polled = ibv_poll_cq(cq, 1, &wc);
    if (polled != 1) { fprintf(stderr, "rdsend: ibv_poll_cq returned %d waiting for send completion\n", polled); return 4; }
    if (wc.status != IBV_WC_SUCCESS) {
      fprintf(stderr, "send WC error: %s\n", ibv_wc_status_str(wc.status));
      return 5;
    }

    /* Wait for recv completion (ACK from rdrecv) */
    if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context)) { fprintf(stderr, "rdsend: ibv_get_cq_event (receiver ACK) failed: %s\n", strerror(errno)); return 6; }
    if (ibv_req_notify_cq(cq, 0)) { fprintf(stderr, "rdsend: ibv_req_notify_cq (receiver ACK) failed: %s\n", strerror(errno)); return 7; }
    polled = ibv_poll_cq(cq, 1, &wc);
    if (polled != 1) { fprintf(stderr, "rdsend: ibv_poll_cq returned %d waiting for receiver ACK\n", polled); return 8; }
    if (wc.status != IBV_WC_SUCCESS) { fprintf(stderr, "rdsend: ACK completion failed: %s (vendor_err=%u)\n", ibv_wc_status_str(wc.status), wc.vendor_err); return 9; }

    event_count += 2;
    report_send_progress(verbose, total_bytes, transfer_start,
                         &last_report_time, &last_report_bytes);
  }
  } /* params.file_size == 0: legacy stream stop-and-wait path */

  clock_gettime(CLOCK_MONOTONIC, &now);
  seconds = (double)((now.tv_sec + now.tv_nsec * 1e-9) -
                     (double)(tmstart.tv_sec + tmstart.tv_nsec * 1e-9));
  TS("transfer data complete");
  if (verbose > 0) {
    fprintf(stderr, "Transfer time (elapsed): %.3f s\n", seconds);
    fprintf(stderr, "Transferred: %llu bytes\n", (unsigned long long)total_bytes);
    fprintf(stderr, "Bandwidth %.3f GB/s\n", seconds > 0 ? (total_bytes / seconds) / 1e9 : 0.0);
  }
  TS("resource cleanup start");

  ibv_ack_cq_events(cq, event_count);

  ibv_destroy_qp(qp);
  ibv_dereg_mr(mr);
  ibv_destroy_cq(cq);
  ibv_dealloc_pd(pd);
  ibv_destroy_comp_channel(comp_chan);
  ibv_close_device(ctx);
  if (direct_fd >= 0) close(direct_fd);
  if (fd != STDIN_FILENO) close(fd);
  TS("resource cleanup done");
  if (verbose) fprintf(stderr, "Total command time: %.3f s\n", now_sec() - _ts0);

  return 0;
}
