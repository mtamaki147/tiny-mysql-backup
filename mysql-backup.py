#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, with_statement


class CmdError(Exception):
    def __init__(self, msg=''):
        _msg = msg

    def __str__(self):
        return _msg


def _execute_cmd(*cmd):
    print ' '.join(cmd)

    import subprocess
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while None == popen.poll():
        popen.stdout.softspace = True
        print popen.stdout.readline(),
    if 0 != popen.returncode:
        raise CmdError(str(popen.returncode))


def _path_join(*leaf):
    from os.path import abspath, join
    return abspath(join(*leaf))


def _backup(options):
    print 'start backup: %s' % _current_time()

    try:
        # バックアップファイルパスを作成
        extensions = {
            'lvm': 'tar.gz',
            'dump': 'sql'
        }
        from time import localtime
        file_name_parts = [options.title, options.backup_by] + list(localtime()[:3]) + [extensions[options.backup_by]]
        backup_file_name = '%s_backup_by_%s_%04d%02d%02d.%s' % tuple(file_name_parts)
        backup_file_path = _path_join('/tmp', backup_file_name)

        # バックアップファイルを作成
        if not options.is_skip_backup:
            _create_backup(options, backup_file_path)

        # s3にUP
        _upload_to_s3(options, backup_file_path)

    finally:
        print 'backup complete: %s' % _current_time()

    # 正常終了したことを表示する。
    import os
    print 'http://%s.s3.amazonaws.com/%s' % (options.aws_bucket_name, os.path.join(options.aws_upload_dir, backup_file_name))


def _current_time():
    import datetime

    class JST(datetime.tzinfo):
        def utcoffset(self, dt):
            return datetime.timedelta(hours=9)

        def dst(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return 'JST'
    return datetime.datetime.now(JST()).isoformat()


def _execute_query(cursor, query):
    print query
    cursor.execute(query)


def _path_get_children(path):
    import os
    return [os.path.join(path, name) for name in os.listdir(path)]


def _backup_by_lvm(options, cursor, backup_file_path):
    # DBをロック
    _execute_query(cursor, 'FLUSH TABLES WITH READ LOCK;')
    try:
        try:
            # スナップショットを作成
            _execute_cmd('sync')
            _execute_cmd('/usr/sbin/lvcreate', '-L%s' % options.snapshot_size, '-s', '-n', 'backup', '/dev/%s/%s' % (options.vg_name, options.lv_name))
        finally:
            # DBのロックを解放
            _execute_query(cursor, 'UNLOCK TABLES;')

        # スナップショットをマウント
        _execute_cmd('mount', '/dev/%s/backup' % options.vg_name, '/mnt')
        try:
            # tarで圧縮
            import os
            if os.path.exists(backup_file_path):
                print 'remove old backup'
                os.remove(backup_file_path)

            backup_targets = _path_get_children(options.target_mysql_dir)
            parent_dir = os.path.split(options.target_mysql_dir)[1]

            def _gen_targets():
                for target in backup_targets:
                    target = os.path.split(target)[1]
                    if not options.exclude_targets or target not in options.exclude_targets:
                            yield os.path.join(parent_dir, target)
            backup_targets = list(_gen_targets())
            backup_targets.append('/etc/my.cnf')

            _execute_cmd('tar', '-zcvf', backup_file_path, '-C', '/mnt%s' % os.path.dirname(options.target_mysql_dir), *backup_targets)

        finally:
            # マウントを解除
            _execute_cmd('umount', '/mnt')

    finally:
        # スナップショットを解放
        _execute_cmd('/usr/sbin/lvremove', '-f', '/dev/%s/backup' % options.vg_name)


def _enable_path(path):
    try:
        import os
        os.makedirs(os.path.dirname(path))
    except OSError:
        pass


def _backup_by_dump(cursor, dump_file_path):
    # DBをロック
    _execute_query(cursor, 'FLUSH TABLES WITH READ LOCK;')

    try:
        # DBのダンプを作成
        print 'mysqldump'
        cmd = ('mysqldump', '--default-character-set=binary', '-u', options.mysql_user, '--password=%s' % options.mysql_passwd, options.mysql_db_name)

        import subprocess
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        print 'save %s' % dump_file_path
        _enable_path(dump_file_path)
        print dump_file_path
        with open(dump_file_path, 'wb') as writable:
            writable.write(popen.stdout.read())

        print popen.stderr.read()

        returncode = popen.wait()
        if 0 != returncode:
            raise CmdError(str(returncode))

    finally:
        # DBのロックを解放
        _execute_query(cursor, 'UNLOCK TABLES;')


def _create_backup(options, backup_file_path):
    # DBを用意
    print 'connect mysql'
    from MySQLdb import connect
    connection = connect(user=options.mysql_user, passwd=options.mysql_passwd)
    cursor = connection.cursor()

    # バックアップ実行
    if 'dump' == options.backup_by:
        _backup_by_dump(cursor, backup_file_path)
    if 'lvm' == options.backup_by:
        _backup_by_lvm(options, cursor, backup_file_path)

from datetime import tzinfo
class TZ(tzinfo):
    def __init__(self, name='', hours=0, minutes=0):
        self._name = name
        self._hours = hours
        self._minutes = minutes

    def tzname(self, dt):
        return self._name

    def utcoffset(self, dt):
        from datetime import timedelta
        return timedelta(hours=self._hours, minutes=self._minutes)

    def dst(self, dt):
        # 夏時間は対応しない
        from datetime import timedelta
        return timedelta(0)

# strftime だと+09:00に対応できないため、自前正規表現
# http://www.python.jp/doc/2.5/lib/strftime-behavior.html
def datetime_from_w3cdtf(w3cdtf_str):
    # '2001-08-02T10:45:23.01235+09:00'
    from re import findall
    re_templates = {'number4': '([0-9]{4})', 'number2': '([0-9]{2})'}
    re_timezone = r'(\+|-)%(number2)s:%(number2)s' % re_templates
    re_templates['timezone'] = '(%s|Z)' % re_timezone
    re_w3cdtf5 = '%(number4)s-%(number2)s-%(number2)sT%(number2)s:%(number2)s:%(number2)s.([0-9]+)%(timezone)s' % re_templates
    parts = findall(re_w3cdtf5, w3cdtf_str)[0]

    # [('2001', '08', '02', '10', '45', '23', '012345', '+09:00', '+', '09', '00')]
    keys = ('year', 'month', 'day', 'hour', 'minute', 'second', 'decimal_second', 'timezone', 'plus_or_minus', 'tz_hours', 'tz_minutes')
    parts = dict(zip(keys, parts))

    if 'Z' == parts['timezone']:
        parts['tz_hours'] = 0
        parts['tz_minutes'] = 0
    tz_hours = int(parts['tz_hours'])
    if '-' == parts['plus_or_minus']:
        tz_hours *= -1
    tz = TZ('Autogen', tz_hours, parts['tz_minutes'])

    from datetime import datetime
    return datetime(
        int(parts['year']),
        int(parts['month']),
        int(parts['day']),
        int(parts['hour']),
        int(parts['minute']),
        int(parts['second']),
        int(1000000 * float('0.%s' % parts['decimal_second'])),
        tz
    )


def _upload_to_s3(options, local_file_path):
    # S3に接続
    print 'connect s3'
    from boto.s3.connection import S3Connection
    connection = S3Connection(options.aws_access_key, options.aws_secret_key)

    # バックアップ用のバケットを取得
    print 'get_bucket'
    from boto.exception import S3ResponseError
    try:
        bucket = connection.get_bucket(options.aws_bucket_name)
    except S3ResponseError:
        print 'bucket not exist.'
        print 'prease create bucket: %s' % options.aws_bucket_name
        from sys import exit
        exit()

    # バックアップをS3に保存
    if not options.is_skip_upload:
        print 'save to s3'
        from boto.s3.key import Key
        # tar.gz
        key = Key(bucket)
        import os
        key.key = os.path.join(options.aws_upload_dir, os.path.split(local_file_path)[1])
        print 'backup to %s' % key.key

        def _cb(progress, max):
            print "\r.%s" % ('.' * int(79 * ((0.0 + progress) / max))),
            import sys
            sys.stdout.flush()
        key.set_contents_from_filename(local_file_path, cb=_cb, replace=True)
        print
        key.set_acl('private')

    # ローカルのバックアップを削除
    if not options.is_skip_remove_local:
        print 'remove local backup'
        os.remove(local_file_path)

    if not options.is_skip_remove_s3:
        print 'remove remote backup'

        # 古いS3のバックアップを削除く
        # 今月はすべて残す
        # 先月は毎週一つづつ残す
        from datetime import datetime, timedelta
        now = datetime.now(tz=TZ())
        print 'now: ', now
        last_one_month = now - timedelta(days=30)
        print 'last_one_month:', last_one_month
        last_two_month = now - timedelta(days=30 * 2)
        print 'last_two_month:', last_two_month

        for key in bucket.list():
            key_date = datetime_from_w3cdtf(key.last_modified)
            # 古いバックアップほど梳h
            if last_two_month < key_date < last_one_month:
                print 'target?(1-2month): ', key_date, key_date.day
                if key_date.day not in (1, 8, 15, 22, 29):
                    print 'delete: %s' % key.key
                    key.delete()

            # それ以前は毎月一つだけ残す
            if key_date <= last_two_month:
                print 'target?(2month after): ', key_date, key_date.day
                if 1 != key_date.day:
                    print 'delete: %s' % key.key
                    key.delete()

if __name__ == '__main__':
    from optparse import OptionParser

    usage = ['']
    usage.append('\tbackup by lvm: sudo ionice -c3 nice -n 19 ./mysql-backup.py --backup-by-lvm --title=TITLE --mysql-user=MYSQL_USER --mysql-passwd=MYSQL_PASSWD --aws-access-key=AWS_ACCESS_KEY --aws-secret-key=AWS_SECRET_KEY --aws-bucket-name=AWS_BUCKET_NAME --aws-upload-dir=AWS_UPLOAD_DIR --vg-name=VG_NAME --lv-name=LV_NAME [--snapshot-size=SNAPSHOT_SIZE] --target-mysql-dir=TARGET_DIR [--exclude-targets=EXCLUDE] [options]')
    usage.append('\tbackup by dump: sudo ionice -c3 nice -n 19 ./mysql-backup.py --backup-by-dump --title=TITLE --mysql-user=MYSQL_USER --mysql-passwd=MYSQL_PASSWD --mysql-db-name=MYSQL_DB_NAME --aws-access-key=AWS_ACCESS_KEY --aws-secret-key=AWS_SECRET_KEY --aws-bucket-name=AWS_BUCKET_NAME --aws-upload-dir=AWS_UPLOAD_DIR [options]')

    parser = OptionParser('\n'.join(usage))

    # backup_by
    parser.add_option('--backup-by-lvm', dest='backup_by', action='store_const',
        const='lvm', help='specify backup method (required): lvm')
    parser.add_option('--backup-by-dump', dest='backup_by', action='store_const',
        const='dump', help='specify backup method (required): dump')

    # common options
    parser.add_option('--title', dest='title', action='store',
        help='common option: backupfile title (required)')

    parser.add_option('--mysql-user', dest='mysql_user', action='store',
        help='common option (required)')
    parser.add_option('--mysql-passwd', dest='mysql_passwd', action='store',
        help='common option (option)')

    parser.add_option('--aws-access-key', dest='aws_access_key', action='store',
        help='common option (required)')
    parser.add_option('--aws-secret-key', dest='aws_secret_key', action='store',
        help='common option (required)')
    parser.add_option('--aws-bucket-name', dest='aws_bucket_name', action='store',
        help='common option (required)')
    parser.add_option('--aws-upload-dir', dest='aws_upload_dir', action='store',
        help='common option (required)')

    # lvm options
    parser.add_option('--vg-name', dest='vg_name', action='store',
        help='lvm option (required): target vg name')
    parser.add_option('--lv-name', dest='lv_name', action='store',
        help='lvm option (required): target lv name')
    parser.add_option('--snapshot-size', dest='snapshot_size', action='store',
        default='5G', help='lvm option (option): default 5G')
    parser.add_option('--target-mysql-dir', dest='target_mysql_dir', action='store',
        help='lvm option (required): target mysql dir')
    parser.add_option('--exclude-targets', dest='exclude_targets', action='append',
        help='lvm option (option): exclude directory names in target mysql directory. Plurals can be specified.')

    # dump options
    parser.add_option('--mysql-db-name', dest='mysql_db_name', action='store',
        help='dump option (required)')

    # options
    parser.add_option('--skip-backup', dest='is_skip_backup', action='store_true',
        default=False, help='for debug: skip create backup')
    parser.add_option('--skip-remove-local', dest='is_skip_remove_local', action='store_true',
        default=False, help='for debug: skip remove local backup')
    parser.add_option('--skip-remove-s3', dest='is_skip_remove_s3', action='store_true',
        default=False, help='for debug: skip remove s3 backup')
    parser.add_option('--skip-upload', dest='is_skip_upload', action='store_true',
        default=False, help='for debug: skip upload')

    options = parser.parse_args()[0]

    requires = ('title', 'backup_by', 'mysql_user', 'aws_access_key', 'aws_secret_key', 'aws_bucket_name', 'aws_upload_dir')
    try:
        for require in requires:
            if not hasattr(options, require) or not getattr(options, require):
                print
                print 'required argument: --%s' % require.replace('_', '-')
                print
                raise StandardError
        if 'lvm' == options.backup_by:
            if not options.vg_name or not options.lv_name or not options.target_mysql_dir:
                print
                print 'when backup by lvm, require argument: --vg-name, --lv-name, --target-mysql-dir'
                print
                raise StandardError
        if 'dump' == options.backup_by:
            if not options.mysql_db_name:
                print
                print 'when backup by dump, require argument: --mysql-db-name'
                print
                raise StandardError
    except StandardError:
        parser.print_help()
        from sys import exit
        exit()

    _backup(options)
