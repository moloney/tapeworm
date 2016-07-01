from __future__ import print_function
import sys, os, argparse, logging, traceback, imp, time, re, fnmatch
from os import path
from . import util, tapemgr
from .spool import Spool
from .jobs import (QueueEntry,
                   Archive,
                   BackupJob,
                   BackupRun,
                   RestoreRun,
                   CopyRun,
                   JobManager,
                   DATE_FMT,
                   path_to_ordinal)


logger = logging.getLogger('tapeworm.tapeworm_cli')


def run_backups(arg_parser, args, conf, job_mgr):
    if args.all or args.jobs:
        # Queue backup runs
        if args.all and args.jobs:
            arg_parser.error("The options --all and --jobs are mutually "
                             "exclusive")
        with conf.db.transaction():
            for job in BackupJob.select():
                if not args.all and not job.name in args.jobs:
                    continue
                job_mgr.queue_backup(job,
                                     conf.root_dirs[job.name],
                                     priority=args.priority)
    else:
        arg_parser.error("You must specify jobs or use the --all option")


def list_files(arg_parser, args, conf, job_mgr):
    if args.regex:
        re_list = [args.regex[0]]
    else:
        re_list = []
    if args.glob:
        re_list.append(fnmatch.translate(args.glob[0]))
    sub_ord = None
    if args.sub_dir:
        norm_path = path.normpath(args.sub_dir[0])
        if norm_path[0] == os.sep:
            norm_path = norm_path[1:]
        sub_ord = path_to_ordinal(norm_path)
        last_seen_depth = None

    for job_name in args.jobs:
        job = BackupJob.select().where(BackupJob.name == job_name).get()
        try:
            last_run = BackupRun.select().\
                where((BackupRun.job == job) & (BackupRun.successful == True)).\
                order_by(BackupRun.run_date.desc()).get()
        except BackupRun.DoesNotExist:
            arg_parser.error("No successful runs for job %s" % job_name)

        for bu_dt, seen_dt, arc_name, bu_path in job.gen_state(job_mgr.index_dir):
            path_ord = path_to_ordinal(bu_path)
            if sub_ord:
                if path_ord[0] < sub_ord[0]:
                    continue
                elif path_ord[0] > sub_ord[0]:
                    if (last_seen_depth is None or
                        (path_ord[0] > last_seen_depth + 1)
                       ):
                        break
                path_sub_ord = path_ord[:len(sub_ord)]
                if path_sub_ord[1:] != sub_ord[1:]:
                    continue
                else:
                    last_seen_depth = path_ord[0]
            if seen_dt < last_run.run_date and not args.deleted:
                continue
            if not all(re.match(regex, bu_path) for regex in re_list):
                continue
            print(job.encode_state(bu_dt, seen_dt, arc_name, bu_path), end='')


def dump_files(arg_parser, args, conf, job_mgr):
    if args.regex:
        re_list = [args.regex[0]]
    else:
        re_list = []
    if args.glob:
        re_list.append(fnmatch.translate(args.glob[0]))
    if args.date_range:
        dates = date_range.split(',')
        if len(dates) == 1:
            start_date, end_date = (dates[0], None)
        else:
            start_date, end_date = dates
    sub_ord = None
    if args.sub_dir:
        norm_path = path.normpath(args.sub_dir[0])
        if norm_path[0] == os.sep:
            norm_path = norm_path[1:]
        sub_ord = path_to_ordinal(norm_path)
        last_seen_depth = None

    for job_name in args.jobs:
        job = BackupJob.select().where(BackupJob.name == job_name).get()
        runs = BackupRun.select().\
                where((BackupRun.job == job) & (BackupRun.successful == True))
        if args.date_range:
            runs = runs.where(BackupRun.run_date > start_date)
            if end_date is not None:
                runs = runs.where(BackupRun.run_date < end_date)
        runs = runs.order_by(BackupRun.run_date.desc())
        for run in runs:
            for archive in list(Archive.select().\
                                where(Archive.run == run).\
                                order_by(Archive.run_index)):
                if sub_ord:
                    last_ord = path_to_ordinal(archive.last_path)
                    if last_ord < sub_ord:
                        continue
                    first_ord = path_to_ordinal(archive.first_path)
                    if (first_ord[0] > sub_ord[0] and
                        (last_seen_depth is None or
                         (first_ord[0] > last_seen_depth + 1))
                       ):
                        break
                for size, m_time, pth in archive.contents(job_mgr.index_dir):
                    if sub_ord:
                        path_ord = path_to_ordinal(pth)
                        if path_ord[1:len(sub_ord)] != sub_ord[1:]:
                            continue
                        last_seen_depth = path_ord[0]
                    #if not all(re.match(regex, pth) for regex in re_list):
                    #    continue
                    print(archive.encode_index(size, m_time, pth), end='')


def restore_files(arg_parser, args, conf, job_mgr):
    if not path.exists(args.dest_dir[0]):
        arg_parser.error("The given 'dest_dir' does not exist")
    with open(args.file_list[0]) as content_f:
        job_mgr.queue_restore(content_f,
                              args.dest_dir[0],
                              strip_archive=args.strip_archive,
                              min_free_space=args.min_free,
                              priority=args.priority)


def copy_archives(arg_parser, args, conf, job_mgr):
    if not path.exists(args.dest_dir[0]):
        arg_parser.error("The given 'dest_dir' does not exist")
    arcs = []
    with open(args.archive_list[0]) as arc_f:
        for line in arc_f:
            line = line.strip()
            if line == '':
                continue
            arcs.append(line)
    job_mgr.queue_copy(arcs,
                       args.dest_dir[0],
                       with_pararchives=args.pararchives,
                       min_free_space=args.min_free,
                       priority=args.priority)


def show_mod_queue(arg_parser, args, conf, job_mgr):
    entries = list(QueueEntry.select().\
                    order_by(QueueEntry.priority.desc(),
                             QueueEntry.queued_date)
                  )
    if len(entries) == 0:
        print("No entries in queue")
        return
    if args.cancel:
        for entry in entries:
            dt_str = entry.queued_date.strftime('%Y%m%d_%H%M%S')
            if dt_str == args.cancel:
                job_mgr.cancel_entry(entry)
                break
        else:
            print("No matching queue entry")
    else:
        hdr_line = 'Type\t\tPrior\t\tAvail\t\tDateTime'
        print(hdr_line)
        print('-' * len(hdr_line.expandtabs()))
        for entry in entries:
            run = job_mgr.get_run(entry)
            if isinstance(run, BackupRun):
                type_str = 'BACKUP'
            elif isinstance(run, RestoreRun):
                type_str = 'RESTORE'
            else:
                type_str = 'COPY'
            dt_str = entry.queued_date.strftime('%Y%m%d_%H%M%S')
            print('%s\t\t%4.2f\t\t%s\t\t%s' %
                  (type_str, entry.priority, entry.available, dt_str))


def run_daemon(arg_parser, args, conf, job_mgr):
    max_jobs = args.max_jobs
    with job_mgr.processing_lock():
        while True:
            n_proc = job_mgr.process_work_queue(max_jobs=args.max_jobs)
            if hasattr(conf, 'buffered_handlers'):
                for hndlr in conf.buffered_handlers:
                    hndlr.flush()
            if max_jobs is not None:
                max_jobs -= n_proc
                if max_jobs < 1:
                    break
            time.sleep(args.poll_sec)


def build_arg_parser():
    # Setup top level arg parser
    arg_parser = argparse.ArgumentParser(description="Manage tape backups")
    sub_parsers = arg_parser.add_subparsers(title="Subcommands")

    # Backup Command
    backup_help = ("Queue a backup run for one or more jobs.")
    backup_parser = sub_parsers.add_parser('backup', help=backup_help)
    backup_parser.add_argument('jobs', nargs='*',
                               help='The jobs to run')
    backup_parser.add_argument('-a', '--all', action='store_true',
                               help='Run all jobs')
    backup_parser.set_defaults(func=run_backups)

    # List Command
    list_help = ("List files that existed during the most recent successful "
                 "backup for one or more jobs. Files that were recently "
                 "deleted can optionally be included.")
    list_parser = sub_parsers.add_parser('list', help=list_help)
    list_parser.add_argument('jobs', nargs='+')
    list_parser.add_argument('-d', '--deleted', action='store_true',
                             help='Include jobs that have been recently '
                             'deleted.')
    list_parser.add_argument('-s', '--sub-dir', nargs=1,
                             help='Restrict results to the given directory')
    list_parser.add_argument('-g', '--glob', nargs=1,
                             help='Restrict results using the given file '
                             'globbing expression')
    list_parser.add_argument('-r', '--regex', nargs=1,
                             help='Restrict results using the given '
                             'regular expression')
    list_parser.set_defaults(func=list_files)

    #Dump command
    dump_help = ("Dump the indices of the archives created for one or more "
                 "backup jobs.")
    dump_parser = sub_parsers.add_parser('dump', help=dump_help)
    dump_parser.add_argument('jobs', nargs='+')
    dump_parser.add_argument('--date-range', nargs=1,
                             help=("Restrict results to the given date range")
                            )
    dump_parser.add_argument('-s', '--sub-dir', nargs=1,
                             help='Restrict results to the given directory')
    dump_parser.add_argument('-g', '--glob', nargs=1,
                             help='Restrict results using the given file '
                             'globbing expression')
    dump_parser.add_argument('-r', '--regex', nargs=1,
                             help='Restrict results using the given '
                             'regular expression')
    dump_parser.set_defaults(func=dump_files)

    # Restore Command
    restore_help = ("Queue the restoration of a list of files (as generated "
                    "by the 'list' or 'dump' commands) to a destination "
                    "directory.")
    restore_parser = sub_parsers.add_parser('restore', help=restore_help)
    restore_parser.add_argument('file_list', nargs=1)
    restore_parser.add_argument('dest_dir', nargs=1)
    restore_parser.add_argument('-s', '--strip-archive', action='store_true',
                                help='Extract files directly into the '
                                'dest_dir instead of storing in seperate sub '
                                'directores.')
    restore_parser.add_argument('--min-free', default=0, type=int,
                                help="Minimum amount of free space in the "
                                "dest_dir, as determined by the 'df' "
                                "command. When the limit is reached the "
                                "restoration will be paused.")
    restore_parser.set_defaults(func=restore_files)

    # Copy command
    copy_help = ("Queue one or more archives to be copied off tape to a "
                 "destination directory")
    copy_parser = sub_parsers.add_parser('copy', help=copy_help)
    copy_parser.add_argument('archive_list', nargs=1)
    copy_parser.add_argument('dest_dir', nargs=1)
    copy_parser.add_argument('-p', '--pararchives', action='store_true',
                             help='Get any associated pararchives as well')
    copy_parser.add_argument('--min-free', default=0, type=int,
                             help="Minimum amount of free space in the "
                             "dest_dir, as determined by the 'df' "
                             "command. When the limit is reached the "
                             "copy will be paused.")
    copy_parser.set_defaults(func=copy_archives)

    # Queue Command
    queue_help = "Display the work queue. Options can be used to modify it"
    queue_parser = sub_parsers.add_parser('queue', help=queue_help)
    queue_parser.add_argument('-c', '--cancel',
                              help=("Cancel the job with the given DateTime"))
    queue_parser.set_defaults(func=show_mod_queue)

    # Daemon Command
    daemon_help = ("Wait for jobs to be queued and process them. Only one "
                   "such process can be runnning at a time")
    daemon_parser = sub_parsers.add_parser('daemon', help=daemon_help)
    daemon_parser.add_argument('--max-jobs', default=None, type=int,
                               help='Max number of jobs to process before '
                               'exiting, otherwise it will run indefinately')
    daemon_parser.add_argument('--poll-sec', default=60,
                               help="The number of seconds to sleep between "
                               "polling the queue")
    daemon_parser.set_defaults(func=run_daemon)

    # General oprions
    gen_opt = arg_parser.add_argument_group('General Options')
    gen_opt.add_argument('-p', '--priority', default=0.5,
                         help='Set the priority for the backup or restore '
                         'jobs being queued')
    gen_opt.add_argument('-i', '--init-db', action='store_true',
                         help='Initialize the database. Must be done once '
                         'before running any jobs.')
    gen_opt.add_argument('-c', '--conf-path', help='Specify the '
                         'path to the configuration file. Defaults to the '
                         'environment variable TAPEWORM_CONF')
    return arg_parser


def get_job_mgr(conf, args):
    # Create a tape manager, spool and job manager
    tape_mgr = tapemgr.TapeManager(conf.db,
                                   conf.changer,
                                   conf.storage_choosers,
                                   conf.cleaning_chooser,
                                   init_db=args.init_db)
    spool = Spool(tape_mgr,
                  conf.spool_path,
                  conf.spool_size,
                  conf.block_size)
    job_mgr = JobManager(spool,
                         conf.job_specs,
                         conf.db,
                         conf.index_dir,
                         args.init_db,
                         conf.max_archive_size)
    return job_mgr

def main(argv=sys.argv):
    arg_parser = build_arg_parser()
    args = arg_parser.parse_args(argv[1:])

    # Import the configuration
    conf_path = os.environ.get('TAPEWORM_CONF')
    if args.conf_path is not None:
        conf_path = args.conf_path
    if conf_path is None:
        arg_parser.error("No configuration file defined")
    conf = imp.load_source('tapeworm_conf', conf_path)

    # Initialize the database proxy
    conf.db.connect()
    util.database_proxy.initialize(conf.db)

    # Create the job manager
    job_mgr = get_job_mgr(conf, args)

    # Handle any subcommands, make sure any buffered log handlers are flushed
    try:
        args.func(arg_parser, args, conf, job_mgr)
    finally:
        if hasattr(conf, 'buffered_handlers'):
            for hndlr in conf.buffered_handlers:
                hndlr.flush()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception, e:
        logger.error("Unhandled exception: %s", traceback.format_exc())
        raise
