/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum
{
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP,
    CMD_WAIT
} command_kind_t;

typedef enum
{
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct child_config child_config_t;

typedef struct container_record
{
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    void *stack_ptr;
    child_config_t *cfg_ptr;
    struct container_record *next;
} container_record_t;

typedef struct
{
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct
{
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct
{
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct
{
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

struct child_config
{
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
};

typedef struct
{
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0')
    {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20))
    {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2)
    {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc)
        {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0)
        {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0)
        {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0)
        {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19)
            {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes)
    {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state)
    {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* ========================================================================= */
/* CONTAINER METADATA MANAGEMENT                                             */
/* ========================================================================= */

static void add_container(supervisor_ctx_t *ctx, const char *id, pid_t pid, unsigned long soft, unsigned long hard, void *stack, child_config_t *cfg)
{
    container_record_t *new_cont = malloc(sizeof(container_record_t));
    memset(new_cont, 0, sizeof(container_record_t));
    strncpy(new_cont->id, id, CONTAINER_ID_LEN - 1);
    new_cont->host_pid = pid;
    new_cont->state = CONTAINER_STARTING;
    new_cont->started_at = time(NULL);
    new_cont->soft_limit_bytes = soft;
    new_cont->hard_limit_bytes = hard;
    new_cont->stack_ptr = stack;
    new_cont->cfg_ptr = cfg;

    pthread_mutex_lock(&ctx->metadata_lock);
    new_cont->next = ctx->containers;
    ctx->containers = new_cont;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *curr = ctx->containers;
    while (curr != NULL)
    {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

static void update_container_state(supervisor_ctx_t *ctx, const char *id, container_state_t state)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = find_container(ctx, id);
    if (curr != NULL)
        curr->state = state;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void remove_container(supervisor_ctx_t *ctx, const char *id)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = ctx->containers;
    container_record_t *prev = NULL;

    while (curr != NULL)
    {
        if (strcmp(curr->id, id) == 0)
        {
            if (prev == NULL)
                ctx->containers = curr->next;
            else
                prev->next = curr->next;
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0)
    {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0)
    {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
    {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down)
    {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
    {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down)
    {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1)
    {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
        {
            perror("open log file");
            continue;
        }

        if (item.length > 0 && item.length <= LOG_CHUNK_SIZE)
        {
            if (write(fd, item.data, item.length) < 0)
            {
                perror("write log");
            }
        }

        close(fd);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;

    if (setpriority(PRIO_PROCESS, 0, config->nice_value) != 0) {
        perror("warning: setpriority failed");
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("mount private /");
        _exit(1);
    }

    if (dup2(config->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        _exit(1);
    }
    if (dup2(config->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        _exit(1);
    }
    close(config->log_write_fd);

    char hostname[64];
    snprintf(hostname, sizeof(hostname), "container-%s", config->id);
    sethostname(hostname, strlen(hostname));

    fprintf(stderr, "DEBUG rootfs path: [%s]\n", config->rootfs);
    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        _exit(1);
    }
    if (chdir("/") != 0) {
        perror("chdir /");
        _exit(1);
    }
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount proc");
        _exit(1);
    }

    execl("/bin/sh", "sh", "-c", config->command, NULL);
    perror("execve failed");
    _exit(1);
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

typedef struct {
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} pipe_logger_args_t;

static void *pipe_logger_thread(void *arg)
{
    pipe_logger_args_t *args = (pipe_logger_args_t *)arg;
    log_item_t item;
    ssize_t n;
    
    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, args->container_id, sizeof(item.container_id) - 1);
    
    while (1)
    {
        n = read(args->pipe_fd, item.data, sizeof(item.data));
        if (n < 0)
        {
            if (errno == EINTR) continue;
            break;
        }
        if (n == 0) break;
        
        item.length = n;
        if (bounded_buffer_push(args->buffer, &item) != 0) break;
    }
    
    close(args->pipe_fd);
    free(args);
    return NULL;
}

static supervisor_ctx_t *g_ctx = NULL;
static volatile sig_atomic_t child_exit_flag = 0;

static void sigchld_handler(int sig)
{
    (void)sig;
    child_exit_flag = 1;
}

static void sigint_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0)
    {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0)
    {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
    {
        perror("open monitor device");
        return 1;
    }

    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0)
    {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        return 1;
    }

    if (listen(ctx.server_fd, 5) < 0)
    {
        perror("listen");
        return 1;
    }

    mkdir(LOG_DIR, 0755);

    /* Start logging thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0)
    {
        perror("pthread_create");
        return 1;
    }

    printf("Supervisor listening on %s\n", CONTROL_PATH);

    g_ctx = &ctx;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = sigint_handler;
    sigemptyset(&sa_int.sa_mask);
    sa_int.sa_flags = 0; /* No SA_RESTART */
    sigaction(SIGINT, &sa_int, NULL);
    sigaction(SIGTERM, &sa_int, NULL);

    while (!ctx.should_stop)
    {
        if (child_exit_flag) {
            child_exit_flag = 0;
            int status;
            pid_t pid;
            while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                while (curr != NULL) {
                    if (curr->host_pid == pid) {
                        if (WIFEXITED(status)) {
                            curr->state = CONTAINER_EXITED;
                            curr->exit_code = WEXITSTATUS(status);
                        } else if (WIFSIGNALED(status)) {
                            if (curr->stop_requested) {
                                curr->state = CONTAINER_STOPPED;
                            } else if (WTERMSIG(status) == SIGKILL) {
                                curr->state = CONTAINER_KILLED;
                            } else {
                                curr->state = CONTAINER_KILLED;
                            }
                            curr->exit_signal = WTERMSIG(status);
                        }
                        if (curr->stack_ptr) {
                            free(curr->stack_ptr);
                            curr->stack_ptr = NULL;
                        }
                        if (curr->cfg_ptr) {
                            free(curr->cfg_ptr);
                            curr->cfg_ptr = NULL;
                        }
                        break;
                    }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
            }
        }

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0)
        {
            if (errno == EINTR) {
                if (ctx.should_stop) break;
                continue;
            }
            perror("accept");
            continue;
        }

        control_request_t req;
        control_response_t resp;

        if (read(client_fd, &req, sizeof(req)) != sizeof(req))
        {
            perror("read");
            close(client_fd);
            continue;
        }

        memset(&resp, 0, sizeof(resp));

        if (req.kind == CMD_RUN || req.kind == CMD_START)
        {
            int log_pipe[2];
            if (pipe(log_pipe) < 0)
            {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Pipe failed");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            child_config_t *cfg = malloc(sizeof(child_config_t));
            memset(cfg, 0, sizeof(child_config_t));
            strncpy(cfg->id, req.container_id, sizeof(cfg->id) - 1);
            strncpy(cfg->rootfs, req.rootfs, sizeof(cfg->rootfs) - 1);
            strncpy(cfg->command, req.command, sizeof(cfg->command) - 1);
            cfg->nice_value = req.nice_value;
            cfg->log_write_fd = log_pipe[1];

            void *stack = malloc(STACK_SIZE);
            if (!stack)
            {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Stack alloc failed");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                close(log_pipe[0]);
                close(log_pipe[1]);
                free(cfg);
                continue;
            }

            pid_t pid = clone(child_fn, (char *)stack + STACK_SIZE, CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD, cfg);

            if (pid < 0)
            {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Clone failed");
                close(log_pipe[0]);
                close(log_pipe[1]);
                free(stack);
                free(cfg);
            }
            else
            {
                close(log_pipe[1]);

                if (register_with_monitor(ctx.monitor_fd,
                                          req.container_id,
                                          pid,
                                          req.soft_limit_bytes,
                                          req.hard_limit_bytes) < 0)
                {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "Register failed");
                    close(log_pipe[0]);
                    kill(pid, SIGKILL);
                }
                else
                {
                    resp.status = 0;
                    snprintf(resp.message, sizeof(resp.message),
                             "Started container %s (pid=%d)",
                             req.container_id, pid);
                             
                    /* ADD RECORD TO METADATA SYSTEM */
                    add_container(&ctx, req.container_id, pid, req.soft_limit_bytes, req.hard_limit_bytes, stack, cfg);
                    update_container_state(&ctx, req.container_id, CONTAINER_RUNNING);

                    /* Send log item */
                    log_item_t item;
                    memset(&item, 0, sizeof(item));
                    strncpy(item.container_id, req.container_id, CONTAINER_ID_LEN - 1);
                    item.length = snprintf(item.data, sizeof(item.data),
                                           "Started container %s (pid=%d)\n",
                                           req.container_id, pid);
                    bounded_buffer_push(&ctx.log_buffer, &item);

                    /* Launch pipe reader thread */
                    pipe_logger_args_t *args = malloc(sizeof(pipe_logger_args_t));
                    args->pipe_fd = log_pipe[0];
                    args->buffer = &ctx.log_buffer;
                    strncpy(args->container_id, req.container_id, sizeof(args->container_id) - 1);
                    
                    pthread_t t;
                    pthread_create(&t, NULL, pipe_logger_thread, args);
                    pthread_detach(t);
                }
            }
        }
        else if (req.kind == CMD_PS)
        {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *curr = ctx.containers;
            size_t offset = 0;
            resp.message[0] = '\0';
            while (curr != NULL)
            {
                int n = snprintf(resp.message + offset, sizeof(resp.message) - offset,
                                 "%s %d %s\n", curr->id, curr->host_pid, state_to_string(curr->state));
                if (n < 0 || (size_t)n >= sizeof(resp.message) - offset)
                    break;
                offset += n;
                curr = curr->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (offset == 0)
                snprintf(resp.message, sizeof(resp.message), "No containers running");
            resp.status = 0;
        }
        else if (req.kind == CMD_STOP)
        {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *curr = find_container(&ctx, req.container_id);
            if (curr != NULL)
            {
                curr->stop_requested = 1;
                kill(curr->host_pid, SIGTERM);
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message), "Sent SIGTERM to container %s", req.container_id);
            }
            else
            {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Container not found");
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        else if (req.kind == CMD_WAIT)
        {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *curr = find_container(&ctx, req.container_id);
            if (curr != NULL)
            {
                if (curr->state == CONTAINER_EXITED || curr->state == CONTAINER_KILLED || curr->state == CONTAINER_STOPPED) {
                    resp.status = 1; 
                    int exit_val = 0;
                    if (curr->state == CONTAINER_EXITED) {
                        exit_val = curr->exit_code;
                    } else if (curr->state == CONTAINER_KILLED || curr->state == CONTAINER_STOPPED) {
                        exit_val = 128 + curr->exit_signal;
                    }
                    snprintf(resp.message, sizeof(resp.message), "%d", exit_val);
                } else {
                    resp.status = 0; 
                    snprintf(resp.message, sizeof(resp.message), "running");
                }
            }
            else
            {
                resp.status = -1; 
                snprintf(resp.message, sizeof(resp.message), "-1");
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        else if (req.kind == CMD_LOGS)
        {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);
            int fd = open(path, O_RDONLY);
            if (fd >= 0)
            {
                ssize_t bytes = read(fd, resp.message, sizeof(resp.message) - 1);
                if (bytes >= 0)
                {
                    resp.message[bytes] = '\0';
                    resp.status = 0;
                }
                else
                {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "Error reading logs");
                }
                close(fd);
            }
            else
            {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Log file not found");
            }
        }
        else
        {
            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message), "Unsupported command");
        }

        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    close(ctx.monitor_fd);

    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *curr = ctx.containers;
    while (curr) {
        container_record_t *next = curr->next;
        free(curr);
        curr = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int sockfd;
    struct sockaddr_un addr;
    control_response_t resp;

    sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("connect");
        close(sockfd);
        return 1;
    }

    if (write(sockfd, req, sizeof(*req)) != sizeof(*req))
    {
        perror("write");
        close(sockfd);
        return 1;
    }

    if (read(sockfd, &resp, sizeof(resp)) != sizeof(resp))
    {
        perror("read");
        close(sockfd);
        return 1;
    }

    printf("%s\n", resp.message);

    close(sockfd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5)
    {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static volatile sig_atomic_t g_run_sig_caught = 0;
static void run_sig_handler(int sig)
{
    g_run_sig_caught = sig;
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5)
    {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    if (send_control_request(&req) != 0) return 1;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_sig_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    int stopped = 0;
    while (1) {
        if (g_run_sig_caught && !stopped) {
            stopped = 1;
            control_request_t stop_req;
            memset(&stop_req, 0, sizeof(stop_req));
            stop_req.kind = CMD_STOP;
            strncpy(stop_req.container_id, argv[2], sizeof(stop_req.container_id) - 1);
            
            int sock = socket(AF_UNIX, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct sockaddr_un addr;
                memset(&addr, 0, sizeof(addr));
                addr.sun_family = AF_UNIX;
                strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
                if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                    if (write(sock, &stop_req, sizeof(stop_req)) > 0) {
                        /* sent stop successfully */
                    }
                }
                close(sock);
            }
        }

        control_request_t wait_req;
        memset(&wait_req, 0, sizeof(wait_req));
        wait_req.kind = CMD_WAIT;
        strncpy(wait_req.container_id, argv[2], sizeof(wait_req.container_id) - 1);
        
        int sock = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sock >= 0) {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
            if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                if (write(sock, &wait_req, sizeof(wait_req)) > 0) {
                    control_response_t wait_resp;
                    if (read(sock, &wait_resp, sizeof(wait_resp)) == sizeof(wait_resp)) {
                        if (wait_resp.status == 1) { 
                            close(sock);
                            return atoi(wait_resp.message);
                        } else if (wait_resp.status == -1) { 
                            close(sock);
                            return 1;
                        }
                    }
                }
            }
            close(sock);
        }
        usleep(500000); 
    }
    return 0;
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0)
    {
        if (argc < 3)
        {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}