#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, with_statement

__all__ = ['']


def _path_enable(path):
    try:
        from os import makedirs
        from os.path import dirname
        makedirs(dirname(path))
    except OSError:
        pass

def _get_keys(bucket):
    keys = ((key.last_modified, key) for key in bucket.list())
    keys = sorted(keys, cmp=lambda x, y: cmp(y, x))
    return [item[1] for item in keys]

def main(options, args):
    # S3に接続
    from boto.s3.connection import S3Connection
    connection = S3Connection(
        options.AWS_ACCESS_KEY, options.AWS_SECRET_KEY)

    # バックアップ用のバケットを取得
    bucket = connection.get_bucket(options.bucket)

    if not args:
        # バックアップをリストアップ
        print 'order by newer:'
        keys = _get_keys(bucket)
        for index, key in enumerate(keys):
            print index, key.key
    else:
        # バックアップの取得
        keys = []
        for arg in args:
            # キーが数値なら、対応するバックアップファイル名に変換
            try:
                key_index = int(arg)
                if not keys:
                    keys = _get_keys(bucket)
                key = keys[key_index]
                key = keys[key_index]
            except ValueError:
                # 数値でないなら普通に取得
                key = bucket.get_key(arg)

            # 保存先を作成
            path = os.path.join(options.dest_dir, key.key)
            print 'download: %s to %s' % (key.key, path)
            # 保存
            _path_enable(path)
            def _cb(current, max):
                print "\r.%s" % ('.' * int(79 * ((0.0 + current) / max))),
                from sys import stdout
                stdout.flush()
            key.get_contents_to_filename(path, cb=_cb)

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-b', '--bucket', dest='bucket', action='store',
        help='set target bucket')
    import os
    parser.add_option(
        '-o', '--dest-dir', dest='dest_dir', action='store',
        default=os.getcwd(), help='specify store directory path for download file')
    parser.add_option(
        '-a', '--aws-access-key', dest='AWS_ACCESS_KEY', action='store')
    parser.add_option(
        '-s', '--aws-secret-key', dest='AWS_SECRET_KEY', action='store')

    options, args = parser.parse_args()
    requires = ('bucket', 'AWS_ACCESS_KEY', 'AWS_SECRET_KEY')
    for require in requires:
        if not hasattr(options, require) or not getattr(options, require):
            print 'require: --%s' % require.replace('_', '-').lower()
            from sys import exit
            exit()
    main(options, args)
