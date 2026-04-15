#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include "monitor_ioctl.h"

/* ===== CONSTANTS ===== */
#define STACK_SIZE       (1024 * 1024)
#define SOCK_PATH        "/tmp/engine.sock"
#define LOG_DIR          "logs"
#define MAX_CONTAINERS   64
#define BUF_CAPACITY     256   /* bounded-buffer slots */
#define MAX_LINE         1024

#define DEFAULT_SOFT_MIB 40
#define DEFAULT_HARD_MIB 64

/* ===== CONTAINER STATE ===== */
typedef enum {
    ST_STARTING = 0,
    ST_RUNNING,
    ST_STOPPED,
    ST_KILLED,
    ST_EXITED,
    ST_HARD_LIMIT_KILLED
} ContainerState;

static const char *state_str(ContainerState s) {
    switch (s) {
        case ST_STARTING:          return "starting";
        case ST_RUNNING:           return "running";
        case ST_STOPPED:           return "stopped";
        case ST_KILLED:            return "killed";
        case ST_EXITED:            return "exited";
        case ST_HARD_LIMIT_KILLED: return "hard_limit_killed";
        default:                   return "unknown";
    }
}

typedef struct {
    char           id[64];
    char           rootfs[256];
    char           cmd[256];
    pid_t          pid;
    ContainerState state;
    time_t         start_time;
    int            soft_mib;
    int            hard_mib;
    int            nice_val;
    char           log_path[256];
    int            exit_code;
    int            exit_signal;
    int            stop_requested;   /* set before sending SIGTERM from 'stop' */

    /* pipe fds in supervisor for logging */
    int            pipe_stdout[2];
    int            pipe_stderr[2];

    /* producer thread for this container */
    pthread_t      producer_tid;
    int            producer_running;
} Container;

/* ===== GLOBAL SUPERVISOR STATE ===== */
static Container   g_containers[MAX_CONTAINERS];
static int         g_num_containers = 0;
static pthread_mutex_t g_containers_lock = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t g_supervisor_running = 1;

/* monitor device fd (opened once in supervisor) */
static int g_monitor_fd = -1;

/* ===== BOUNDED BUFFER ===== */
typedef struct {
    char  data[MAX_LINE];
    char  log_path[256];
} LogEntry;

static LogEntry        g_log_buf[BUF_CAPACITY];
static int             g_buf_head = 0;
static int             g_buf_tail = 0;
static int             g_buf_count = 0;
static pthread_mutex_t g_buf_lock     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_buf_notempty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_buf_notfull  = PTHREAD_COND_INITIALIZER;
static volatile int    g_buf_shutdown = 0;   /* set when supervisor exits */

/* ===== BOUNDED BUFFER OPS ===== */
static void buf_push(const char *line, const char *log_path) {
    pthread_mutex_lock(&g_buf_lock);
    while (g_buf_count == BUF_CAPACITY && !g_buf_shutdown) {
        pthread_cond_wait(&g_buf_notfull, &g_buf_lock);
    }
    if (g_buf_shutdown) {
        pthread_mutex_unlock(&g_buf_lock);
        return;
    }
    strncpy(g_log_buf[g_buf_tail].data,     line,     MAX_LINE - 1);
    strncpy(g_log_buf[g_buf_tail].log_path, log_path, 255);
    g_buf_tail = (g_buf_tail + 1) % BUF_CAPACITY;
    g_buf_count++;
    pthread_cond_signal(&g_buf_notempty);
    pthread_mutex_unlock(&g_buf_lock);
}

/* consumer thread */
static void *consumer_thread(void *arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&g_buf_lock);
        while (g_buf_count == 0 && !g_buf_shutdown) {
            pthread_cond_wait(&g_buf_notempty, &g_buf_lock);
        }
        if (g_buf_count == 0 && g_buf_shutdown) {
            pthread_mutex_unlock(&g_buf_lock);
            break;
        }
        LogEntry entry = g_log_buf[g_buf_head];
        g_buf_head = (g_buf_head + 1) % BUF_CAPACITY;
        g_buf_count--;
        pthread_cond_signal(&g_buf_notfull);
        pthread_mutex_unlock(&g_buf_lock);

        /* write to log file */
        FILE *f = fopen(entry.log_path, "a");
        if (f) {
            fputs(entry.data, f);
            fclose(f);
        }
    }
    /* flush remaining */
    pthread_mutex_lock(&g_buf_lock);
    while (g_buf_count > 0) {
        LogEntry entry = g_log_buf[g_buf_head];
        g_buf_head = (g_buf_head + 1) % BUF_CAPACITY;
        g_buf_count--;
        pthread_mutex_unlock(&g_buf_lock);
        FILE *f = fopen(entry.log_path, "a");
        if (f) { fputs(entry.data, f); fclose(f); }
        pthread_mutex_lock(&g_buf_lock);
    }
    pthread_mutex_unlock(&g_buf_lock);
    return NULL;
}

static pthread_t g_consumer_tid;

/* ===== PRODUCER THREAD (one per container) ===== */
typedef struct {
    int   fd_stdout;
    int   fd_stderr;
    char  log_path[256];
    int  *running_flag;   /* points to container's producer_running */
} ProducerArgs;

static void *producer_thread(void *arg) {
    ProducerArgs *pa = (ProducerArgs *)arg;
    char line[MAX_LINE];

    /* use select to read from both stdout and stderr pipes */
    while (1) {
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;
        if (pa->fd_stdout >= 0) { FD_SET(pa->fd_stdout, &rfds); if (pa->fd_stdout > maxfd) maxfd = pa->fd_stdout; }
        if (pa->fd_stderr >= 0) { FD_SET(pa->fd_stderr, &rfds); if (pa->fd_stderr > maxfd) maxfd = pa->fd_stderr; }
        if (maxfd < 0) break;

        struct timeval tv = {1, 0};
        int ret = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (ret < 0) break;
        if (ret == 0) {
            if (!*(pa->running_flag)) break;
            continue;
        }

        int active = 0;
        if (pa->fd_stdout >= 0 && FD_ISSET(pa->fd_stdout, &rfds)) {
            ssize_t n = read(pa->fd_stdout, line, MAX_LINE - 1);
            if (n > 0) { line[n] = '\0'; buf_push(line, pa->log_path); active = 1; }
            else { close(pa->fd_stdout); pa->fd_stdout = -1; }
        }
        if (pa->fd_stderr >= 0 && FD_ISSET(pa->fd_stderr, &rfds)) {
            ssize_t n = read(pa->fd_stderr, line, MAX_LINE - 1);
            if (n > 0) { line[n] = '\0'; buf_push(line, pa->log_path); active = 1; }
            else { close(pa->fd_stderr); pa->fd_stderr = -1; }
        }
        (void)active;
        if (pa->fd_stdout < 0 && pa->fd_stderr < 0) break;
    }

    *(pa->running_flag) = 0;
    free(pa);
    return NULL;
}

/* ===== CONTAINER PROCESS ENTRY ===== */
typedef struct {
    char id[64];
    char rootfs[256];
    char cmd[256];
    int  nice_val;
    int  pipe_stdout_write;
    int  pipe_stderr_write;
} SpawnArgs;

static int container_entry(void *arg) {
    SpawnArgs *a = (SpawnArgs *)arg;

    /* redirect stdout/stderr to pipes */
    dup2(a->pipe_stdout_write, STDOUT_FILENO);
    dup2(a->pipe_stderr_write, STDERR_FILENO);
    close(a->pipe_stdout_write);
    close(a->pipe_stderr_write);

    /* namespace setup */
    if (chroot(a->rootfs) != 0) { perror("chroot"); return 1; }
    chdir("/");
    mount("proc", "/proc", "proc", 0, NULL);

    /* nice */
    if (a->nice_val != 0) nice(a->nice_val);

    execl("/bin/sh", "sh", "-c", a->cmd, NULL);
    perror("execl");
    return 1;
}

/* ===== SPAWN CONTAINER ===== */
static int do_spawn(Container *c) {
    /* create pipes */
    if (pipe(c->pipe_stdout) < 0 || pipe(c->pipe_stderr) < 0) {
        perror("pipe"); return -1;
    }

    SpawnArgs *a = malloc(sizeof(SpawnArgs));
    strncpy(a->id,     c->id,     63);
    strncpy(a->rootfs, c->rootfs, 255);
    strncpy(a->cmd,    c->cmd,    255);
    a->nice_val           = c->nice_val;
    a->pipe_stdout_write  = c->pipe_stdout[1];
    a->pipe_stderr_write  = c->pipe_stderr[1];

    char *stack = malloc(STACK_SIZE);
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;

    pid_t pid = clone(container_entry, stack + STACK_SIZE, flags, a);
    if (pid < 0) { perror("clone"); free(a); free(stack); return -1; }

    /* close write ends in supervisor */
    close(c->pipe_stdout[1]); c->pipe_stdout[1] = -1;
    close(c->pipe_stderr[1]); c->pipe_stderr[1] = -1;

    c->pid        = pid;
    c->state      = ST_RUNNING;
    c->start_time = time(NULL);
    c->producer_running = 1;

    /* start producer thread */
    ProducerArgs *pa = malloc(sizeof(ProducerArgs));
    pa->fd_stdout    = c->pipe_stdout[0];
    pa->fd_stderr    = c->pipe_stderr[0];
    pa->running_flag = &c->producer_running;
    strncpy(pa->log_path, c->log_path, 255);

    pthread_create(&c->producer_tid, NULL, producer_thread, pa);

    /* register with kernel monitor */
    if (g_monitor_fd >= 0) {
        struct container_info ci;
        ci.pid       = pid;
        ci.soft_mib  = c->soft_mib;
        ci.hard_mib  = c->hard_mib;
        strncpy(ci.id, c->id, sizeof(ci.id) - 1);
        if (ioctl(g_monitor_fd, MONITOR_REGISTER, &ci) < 0)
            fprintf(stderr, "[supervisor] ioctl register failed: %s\n", strerror(errno));
    }

    printf("[supervisor] started %s PID=%d\n", c->id, pid);
    return 0;
}

/* ===== SIGCHLD HANDLER ===== */
static void sigchld_handler(int sig) {
    (void)sig;
    int saved_errno = errno;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_containers_lock);
        for (int i = 0; i < g_num_containers; i++) {
            Container *c = &g_containers[i];
            if (c->pid != pid) continue;

            if (WIFEXITED(status)) {
                c->exit_code   = WEXITSTATUS(status);
                c->exit_signal = 0;
                c->state       = c->stop_requested ? ST_STOPPED : ST_EXITED;
            } else if (WIFSIGNALED(status)) {
                c->exit_signal = WTERMSIG(status);
                c->exit_code   = 128 + c->exit_signal;
                if (c->stop_requested)
                    c->state = ST_STOPPED;
                else if (c->exit_signal == SIGKILL)
                    c->state = ST_HARD_LIMIT_KILLED;
                else
                    c->state = ST_KILLED;
            }
            c->producer_running = 0;
            break;
        }
        pthread_mutex_unlock(&g_containers_lock);
    }
    errno = saved_errno;
}

/* ===== SIGNAL HANDLER FOR SUPERVISOR ===== */
static void sigterm_handler(int sig) {
    (void)sig;
    g_supervisor_running = 0;
}

/* ===== FIND CONTAINER BY ID ===== */
static Container *find_container(const char *id) {
    for (int i = 0; i < g_num_containers; i++)
        if (strcmp(g_containers[i].id, id) == 0)
            return &g_containers[i];
    return NULL;
}

/* ===== COMMAND HANDLERS ===== */

/* returns response string into resp (caller provides buffer) */
static void cmd_start(const char *id, const char *rootfs, const char *cmd,
                      int soft_mib, int hard_mib, int nice_val, char *resp) {
    pthread_mutex_lock(&g_containers_lock);

    if (g_num_containers >= MAX_CONTAINERS) {
        pthread_mutex_unlock(&g_containers_lock);
        strcpy(resp, "ERROR: too many containers\n");
        return;
    }
    if (find_container(id)) {
        pthread_mutex_unlock(&g_containers_lock);
        snprintf(resp, 512, "ERROR: container '%s' already exists\n", id);
        return;
    }

    Container *c = &g_containers[g_num_containers];
    memset(c, 0, sizeof(*c));
    strncpy(c->id,     id,     63);
    strncpy(c->rootfs, rootfs, 255);
    strncpy(c->cmd,    cmd,    255);
    c->soft_mib  = soft_mib;
    c->hard_mib  = hard_mib;
    c->nice_val  = nice_val;
    c->state     = ST_STARTING;
    snprintf(c->log_path, 255, "%s/%s.log", LOG_DIR, id);

    if (do_spawn(c) < 0) {
        pthread_mutex_unlock(&g_containers_lock);
        snprintf(resp, 512, "ERROR: failed to spawn container '%s'\n", id);
        return;
    }

    g_num_containers++;
    pthread_mutex_unlock(&g_containers_lock);
    snprintf(resp, 512, "OK started %s PID=%d\n", id, c->pid);
}

static void cmd_ps(char *resp, int resp_size) {
    int off = 0;
    off += snprintf(resp + off, resp_size - off,
        "%-12s %-8s %-22s %-20s %-6s %-6s %s\n",
        "ID", "PID", "STARTED", "STATE", "SOFT", "HARD", "CMD");

    pthread_mutex_lock(&g_containers_lock);
    for (int i = 0; i < g_num_containers; i++) {
        Container *c = &g_containers[i];
        char tbuf[32];
        struct tm *tm = localtime(&c->start_time);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", tm);

        off += snprintf(resp + off, resp_size - off,
            "%-12s %-8d %-22s %-20s %-6d %-6d %s\n",
            c->id, c->pid, tbuf, state_str(c->state),
            c->soft_mib, c->hard_mib, c->cmd);
    }
    pthread_mutex_unlock(&g_containers_lock);
}

static void cmd_stop(const char *id, char *resp) {
    pthread_mutex_lock(&g_containers_lock);
    Container *c = find_container(id);
    if (!c) {
        pthread_mutex_unlock(&g_containers_lock);
        snprintf(resp, 512, "ERROR: container '%s' not found\n", id);
        return;
    }
    if (c->state != ST_RUNNING) {
        pthread_mutex_unlock(&g_containers_lock);
        snprintf(resp, 512, "ERROR: container '%s' is not running\n", id);
        return;
    }

    c->stop_requested = 1;
    kill(c->pid, SIGTERM);

    /* give it 3s then SIGKILL */
    pthread_mutex_unlock(&g_containers_lock);

    for (int i = 0; i < 30; i++) {
        usleep(100000);
        pthread_mutex_lock(&g_containers_lock);
        if (c->state != ST_RUNNING) {
            pthread_mutex_unlock(&g_containers_lock);
            snprintf(resp, 512, "OK stopped %s\n", id);
            return;
        }
        pthread_mutex_unlock(&g_containers_lock);
    }

    pthread_mutex_lock(&g_containers_lock);
    kill(c->pid, SIGKILL);
    pthread_mutex_unlock(&g_containers_lock);

    snprintf(resp, 512, "OK killed %s\n", id);
}

static void cmd_logs(const char *id, int client_fd) {
    pthread_mutex_lock(&g_containers_lock);
    Container *c = find_container(id);
    char log_path[256] = "";
    if (c) strncpy(log_path, c->log_path, 255);
    pthread_mutex_unlock(&g_containers_lock);

    if (!c) {
        dprintf(client_fd, "ERROR: container '%s' not found\n", id);
        return;
    }

    FILE *f = fopen(log_path, "r");
    if (!f) {
        dprintf(client_fd, "No logs yet for '%s'\n", id);
        return;
    }

    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f))
        dprintf(client_fd, "%s", line);
    fclose(f);
}

/* run: start + wait for exit, return exit_code */
static void cmd_run(const char *id, const char *rootfs, const char *cmd,
                    int soft_mib, int hard_mib, int nice_val, int client_fd) {
    char resp[512];
    cmd_start(id, rootfs, cmd, soft_mib, hard_mib, nice_val, resp);
    dprintf(client_fd, "%s", resp);
    if (strncmp(resp, "ERROR", 5) == 0) return;

    /* wait for container to exit */
    while (1) {
        usleep(200000);
        pthread_mutex_lock(&g_containers_lock);
        Container *c = find_container(id);
        if (!c || c->state != ST_RUNNING) {
            int code = c ? c->exit_code : -1;
            pthread_mutex_unlock(&g_containers_lock);
            dprintf(client_fd, "EXITED exit_code=%d\n", code);
            return;
        }
        pthread_mutex_unlock(&g_containers_lock);
    }
}

/* ===== SUPERVISOR MAIN LOOP ===== */

/* parse optional flags from a token array */
static void parse_opts(char **tokens, int ntok, int *soft_mib, int *hard_mib, int *nice_val) {
    *soft_mib = DEFAULT_SOFT_MIB;
    *hard_mib = DEFAULT_HARD_MIB;
    *nice_val = 0;
    for (int i = 0; i < ntok - 1; i++) {
        if (strcmp(tokens[i], "--soft-mib") == 0) *soft_mib = atoi(tokens[i+1]);
        if (strcmp(tokens[i], "--hard-mib") == 0) *hard_mib = atoi(tokens[i+1]);
        if (strcmp(tokens[i], "--nice")     == 0) *nice_val = atoi(tokens[i+1]);
    }
}

static void handle_client(int client_fd) {
    char buf[2048];
    ssize_t n = recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return;
    buf[n] = '\0';

    /* tokenize */
    char *tokens[32];
    int ntok = 0;
    char *p = strtok(buf, " \t\n");
    while (p && ntok < 31) { tokens[ntok++] = p; p = strtok(NULL, " \t\n"); }
    if (ntok == 0) return;

    char resp[4096] = "";

    if (strcmp(tokens[0], "start") == 0 && ntok >= 4) {
        int soft, hard, nv;
        parse_opts(tokens + 4, ntok - 4, &soft, &hard, &nv);
        cmd_start(tokens[1], tokens[2], tokens[3], soft, hard, nv, resp);
        send(client_fd, resp, strlen(resp), 0);
    }
    else if (strcmp(tokens[0], "run") == 0 && ntok >= 4) {
        int soft, hard, nv;
        parse_opts(tokens + 4, ntok - 4, &soft, &hard, &nv);
        cmd_run(tokens[1], tokens[2], tokens[3], soft, hard, nv, client_fd);
    }
    else if (strcmp(tokens[0], "ps") == 0) {
        cmd_ps(resp, sizeof(resp));
        send(client_fd, resp, strlen(resp), 0);
    }
    else if (strcmp(tokens[0], "stop") == 0 && ntok >= 2) {
        cmd_stop(tokens[1], resp);
        send(client_fd, resp, strlen(resp), 0);
    }
    else if (strcmp(tokens[0], "logs") == 0 && ntok >= 2) {
        cmd_logs(tokens[1], client_fd);
    }
    else {
        snprintf(resp, sizeof(resp), "ERROR: unknown command '%s'\n", tokens[0]);
        send(client_fd, resp, strlen(resp), 0);
    }
}

static void run_supervisor(void) {
    /* signal setup */
    struct sigaction sa_chld = {0};
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    struct sigaction sa_term = {0};
    sa_term.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT,  &sa_term, NULL);

    mkdir(LOG_DIR, 0777);

    /* open kernel monitor (optional — graceful if absent) */
    g_monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (g_monitor_fd < 0)
        fprintf(stderr, "[supervisor] kernel monitor not available (%s)\n", strerror(errno));

    /* start consumer thread */
    pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL);

    /* UNIX domain socket */
    unlink(SOCK_PATH);
    int srv_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
    bind(srv_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(srv_fd, 16);
    chmod(SOCK_PATH, 0666);

    printf("[supervisor] running PID=%d socket=%s\n", getpid(), SOCK_PATH);

    /* make accept non-blocking so we can check g_supervisor_running */
    fcntl(srv_fd, F_SETFL, O_NONBLOCK);

    while (g_supervisor_running) {
        int client_fd = accept(srv_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(50000);
                continue;
            }
            break;
        }
        handle_client(client_fd);
        close(client_fd);
    }

    printf("[supervisor] shutting down...\n");

    /* stop all running containers */
    pthread_mutex_lock(&g_containers_lock);
    for (int i = 0; i < g_num_containers; i++) {
        if (g_containers[i].state == ST_RUNNING) {
            g_containers[i].stop_requested = 1;
            kill(g_containers[i].pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&g_containers_lock);

    /* wait for children */
    for (int i = 0; i < 30; i++) {
        usleep(100000);
        int all_done = 1;
        pthread_mutex_lock(&g_containers_lock);
        for (int j = 0; j < g_num_containers; j++)
            if (g_containers[j].state == ST_RUNNING) { all_done = 0; break; }
        pthread_mutex_unlock(&g_containers_lock);
        if (all_done) break;
    }

    /* join producer threads */
    pthread_mutex_lock(&g_containers_lock);
    for (int i = 0; i < g_num_containers; i++) {
        g_containers[i].producer_running = 0;
    }
    pthread_mutex_unlock(&g_containers_lock);
    for (int i = 0; i < g_num_containers; i++)
        pthread_join(g_containers[i].producer_tid, NULL);

    /* shutdown consumer */
    pthread_mutex_lock(&g_buf_lock);
    g_buf_shutdown = 1;
    pthread_cond_broadcast(&g_buf_notempty);
    pthread_cond_broadcast(&g_buf_notfull);
    pthread_mutex_unlock(&g_buf_lock);
    pthread_join(g_consumer_tid, NULL);

    if (g_monitor_fd >= 0) close(g_monitor_fd);
    close(srv_fd);
    unlink(SOCK_PATH);
    printf("[supervisor] clean exit\n");
}

/* ===== CLI ===== */
static void send_cmd(int argc, char *argv[]) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor (is it running?)\n");
        exit(1);
    }

    /* build message */
    char msg[2048] = "";
    for (int i = 1; i < argc; i++) {
        if (i > 1) strncat(msg, " ", sizeof(msg) - strlen(msg) - 1);
        strncat(msg, argv[i], sizeof(msg) - strlen(msg) - 1);
    }

    send(fd, msg, strlen(msg), 0);

    /* read response */
    char buf[4096];
    ssize_t n;
    while ((n = recv(fd, buf, sizeof(buf) - 1, 0)) > 0) {
        buf[n] = '\0';
        printf("%s", buf);
    }
    close(fd);
}

/* ===== RUN CLIENT (handles SIGINT → stop) ===== */
static char g_run_container_id[64];
static int  g_run_sock_fd = -1;

static void run_sigint_handler(int sig) {
    (void)sig;
    if (g_run_sock_fd >= 0) {
        /* send stop to supervisor */
        int fd2 = socket(AF_UNIX, SOCK_STREAM, 0);
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
        if (connect(fd2, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            char msg[128];
            snprintf(msg, sizeof(msg), "stop %s", g_run_container_id);
            send(fd2, msg, strlen(msg), 0);
            close(fd2);
        }
    }
}

/* ===== MAIN ===== */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
            "Usage:\n"
            "  engine supervisor <base-rootfs>\n"
            "  engine start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  engine run   <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  engine ps\n"
            "  engine stop <id>\n"
            "  engine logs <id>\n");
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        run_supervisor();
        return 0;
    }

    if (strcmp(argv[1], "run") == 0 && argc >= 5) {
        /* special: block until container exits */
        strncpy(g_run_container_id, argv[2], 63);
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        g_run_sock_fd = fd;
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            fprintf(stderr, "Cannot connect to supervisor\n");
            return 1;
        }
        signal(SIGINT,  run_sigint_handler);
        signal(SIGTERM, run_sigint_handler);

        char msg[2048] = "";
        for (int i = 1; i < argc; i++) {
            if (i > 1) strncat(msg, " ", sizeof(msg) - strlen(msg) - 1);
            strncat(msg, argv[i], sizeof(msg) - strlen(msg) - 1);
        }
        send(fd, msg, strlen(msg), 0);

        char buf[4096];
        ssize_t n;
        while ((n = recv(fd, buf, sizeof(buf) - 1, 0)) > 0) {
            buf[n] = '\0';
            printf("%s", buf);
        }
        close(fd);
        return 0;
    }

    send_cmd(argc, argv);
    return 0;
}
