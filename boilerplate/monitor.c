#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/timer.h>
#include <linux/jiffies.h>
#include <linux/sched.h>
#include <linux/mm.h>
#include <linux/signal.h>
#include <linux/pid.h>

/* pull in struct container_info + ioctl numbers */
#include "monitor_ioctl.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Student");
MODULE_DESCRIPTION("Container memory monitor LKM");

#define DEVICE_NAME  "container_monitor"
#define CLASS_NAME   "container_mon"
#define CHECK_INTERVAL_MS 2000   /* RSS check every 2 seconds */

/* ===== PER-CONTAINER ENTRY ===== */
struct mon_entry {
    struct list_head list;
    pid_t            pid;
    int              soft_mib;
    int              hard_mib;
    char             id[64];
    int              soft_warned;   /* have we already warned soft limit? */
};

static LIST_HEAD(g_entries);
static DEFINE_MUTEX(g_list_lock);

/* ===== DEVICE ===== */
static int           g_major;
static struct class  *g_class  = NULL;
static struct device *g_device = NULL;
static struct cdev    g_cdev;
static dev_t          g_devno;

/* ===== TIMER ===== */
static struct timer_list g_timer;

/* ===== RSS HELPER ===== */
static long get_rss_mib(pid_t pid) {
    struct task_struct *task;
    struct mm_struct   *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) { rcu_read_unlock(); return -1; }

    mm = get_task_mm(task);
    rcu_read_unlock();

    if (!mm) return 0;

    rss_pages = get_mm_rss(mm);
    mmput(mm);

    return (rss_pages * PAGE_SIZE) >> 20;  /* bytes → MiB */
}

/* ===== TIMER CALLBACK — periodic RSS check ===== */
static void check_memory(struct timer_list *t) {
    struct mon_entry *entry, *tmp;

    mutex_lock(&g_list_lock);
    list_for_each_entry_safe(entry, tmp, &g_entries, list) {
        long rss = get_rss_mib(entry->pid);

        if (rss < 0) {
            /* process gone — remove stale entry */
            pr_info("container_monitor: pid %d gone, removing\n", entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if (rss >= entry->hard_mib) {
            pr_warn("container_monitor: [%s] pid=%d RSS=%ldMiB >= hard=%dMiB — KILLING\n",
                    entry->id, entry->pid, rss, entry->hard_mib);
            kill_pid(find_vpid(entry->pid), SIGKILL, 1);
            /* entry will be removed on next tick once pid is gone */
        } else if (rss >= entry->soft_mib && !entry->soft_warned) {
            pr_warn("container_monitor: [%s] pid=%d RSS=%ldMiB >= soft=%dMiB — WARNING\n",
                    entry->id, entry->pid, rss, entry->soft_mib);
            entry->soft_warned = 1;
        }
    }
    mutex_unlock(&g_list_lock);

    /* re-arm */
    mod_timer(&g_timer, jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));
}

/* ===== IOCTL ===== */
static long mon_ioctl(struct file *file, unsigned int cmd, unsigned long arg) {
    switch (cmd) {

    case MONITOR_REGISTER: {
        struct container_info info;
        if (copy_from_user(&info, (void __user *)arg, sizeof(info)))
            return -EFAULT;

        struct mon_entry *entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) return -ENOMEM;

        entry->pid         = info.pid;
        entry->soft_mib    = info.soft_mib;
        entry->hard_mib    = info.hard_mib;
        entry->soft_warned = 0;
        strncpy(entry->id, info.id, sizeof(entry->id) - 1);
        entry->id[sizeof(entry->id) - 1] = '\0';

        mutex_lock(&g_list_lock);
        list_add(&entry->list, &g_entries);
        mutex_unlock(&g_list_lock);

        pr_info("container_monitor: registered [%s] pid=%d soft=%dMiB hard=%dMiB\n",
                entry->id, entry->pid, entry->soft_mib, entry->hard_mib);
        return 0;
    }

    case MONITOR_UNREGISTER: {
        int pid;
        if (copy_from_user(&pid, (void __user *)arg, sizeof(pid)))
            return -EFAULT;

        mutex_lock(&g_list_lock);
        struct mon_entry *entry, *tmp;
        list_for_each_entry_safe(entry, tmp, &g_entries, list) {
            if (entry->pid == pid) {
                list_del(&entry->list);
                kfree(entry);
                pr_info("container_monitor: unregistered pid=%d\n", pid);
                break;
            }
        }
        mutex_unlock(&g_list_lock);
        return 0;
    }

    default:
        return -EINVAL;
    }
}

/* ===== FILE OPS ===== */
static int mon_open(struct inode *inode, struct file *file) { return 0; }
static int mon_release(struct inode *inode, struct file *file) { return 0; }

static const struct file_operations g_fops = {
    .owner          = THIS_MODULE,
    .open           = mon_open,
    .release        = mon_release,
    .unlocked_ioctl = mon_ioctl,
};

/* ===== MODULE INIT / EXIT ===== */
static int __init mon_init(void) {
    int ret;

    ret = alloc_chrdev_region(&g_devno, 0, 1, DEVICE_NAME);
    if (ret < 0) { pr_err("container_monitor: alloc_chrdev_region failed\n"); return ret; }
    g_major = MAJOR(g_devno);

    cdev_init(&g_cdev, &g_fops);
    ret = cdev_add(&g_cdev, g_devno, 1);
    if (ret < 0) { unregister_chrdev_region(g_devno, 1); return ret; }

    g_class = class_create(CLASS_NAME);
    if (IS_ERR(g_class)) {
        cdev_del(&g_cdev); unregister_chrdev_region(g_devno, 1);
        return PTR_ERR(g_class);
    }

    g_device = device_create(g_class, NULL, g_devno, NULL, DEVICE_NAME);
    if (IS_ERR(g_device)) {
        class_destroy(g_class); cdev_del(&g_cdev); unregister_chrdev_region(g_devno, 1);
        return PTR_ERR(g_device);
    }

    /* start periodic timer */
    timer_setup(&g_timer, check_memory, 0);
    mod_timer(&g_timer, jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));

    pr_info("container_monitor: loaded, device /dev/%s major=%d\n", DEVICE_NAME, g_major);
    return 0;
}

static void __exit mon_exit(void) {
    del_timer_sync(&g_timer);

    /* free all list entries */
    mutex_lock(&g_list_lock);
    struct mon_entry *entry, *tmp;
    list_for_each_entry_safe(entry, tmp, &g_entries, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&g_list_lock);

    device_destroy(g_class, g_devno);
    class_destroy(g_class);
    cdev_del(&g_cdev);
    unregister_chrdev_region(g_devno, 1);

    pr_info("container_monitor: unloaded\n");
}

module_init(mon_init);
module_exit(mon_exit);
