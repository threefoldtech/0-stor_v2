// Struct definition:
// https://github.com/maxux/zdbfs-stats/blob/master/zdbfs-stats.c#L20
#include <stdio.h>

typedef struct stats_t {
	size_t version;

    size_t fuse_reqs;

    size_t cache_hit;
    size_t cache_miss;
    size_t cache_full;
    size_t cache_linear_flush;
    size_t cache_random_flush;
	size_t cache_branches;
	size_t cache_branches_allocated;
	size_t cache_entries;
	size_t cache_blocks;
	size_t cache_blocksize;

    size_t syscall_getattr;
    size_t syscall_setattr;
    size_t syscall_create;
    size_t syscall_readdir;
    size_t syscall_open;
    size_t syscall_read;
    size_t syscall_write;
    size_t syscall_mkdir;
    size_t syscall_unlink;
    size_t syscall_rmdir;
    size_t syscall_rename;
    size_t syscall_link;
    size_t syscall_symlink;
    size_t syscall_statsfs;
    size_t syscall_ioctl;

    size_t read_bytes;
    size_t write_bytes;

    size_t errors;

} fs_stats_t;
