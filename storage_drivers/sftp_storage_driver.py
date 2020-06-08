"""
.. module:: storage_drivers.sftp_storage_driver
    :synopsis: SFTP implementation of the base StorageDriver

.. moduleauthor:: Hasan Jawad <hasan_sg@hotmail.com>
"""
import os
import codecs
import pysftp

from .storage_driver import StorageDriver, StorageDriverError


class SFTPStorageDriver(StorageDriver):
    """
    Read and write to SFTP.
    """

    def __init__(self, host, port, username, password, sftp_root_dir):
        """
        Set up the credentials and remote root dir.

        :param str host: SFTP host.
        :param int port: SFTP port.
        :param str username: SFTP username.
        :param str password: SFTP password credentials.
        :param str sftp_root_dir: The remote SFTP root dir to use.
        """
        super(SFTPStorageDriver, self).__init__()

        self.sftp_root_dir = sftp_root_dir

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(
            host=host,
            username=username,
            password=password,
            port=port,
            cnopts=cnopts)

        self.sftp.makedirs(self.sftp_root_dir)

    def get_filename(self, dag_id, task_id, execution_date):
        return os.path.join(
            self.sftp_root_dir,
            dag_id,
            task_id,
            self.execution_date_string(execution_date)
        )

    def get_path(self, dag_id, task_id):
        return os.path.join(self.sftp_root_dir, dag_id, task_id)

    def read(self, dag_id, task_id, execution_date, encoding='utf-8'):
        filename = self.get_filename(dag_id, task_id, execution_date)

        with self.sftp.open(filename, 'rb') as f:
            data = f.read()

        return data

    def get_read_stream(self, dag_id, task_id, execution_date):
        filename = self.get_filename(dag_id, task_id, execution_date)

        f = self.sftp.open(filename, 'rb')
        return f

    def write(self, dag_id, task_id, execution_date, data, *args, **kwargs):
        filename = self.get_filename(dag_id, task_id, execution_date)

        self.check_or_create_dir(os.path.dirname(filename))

        with self.sftp.open(filename, 'w') as f:
            f.write(data)

    def write_from_stream(self, dag_id, task_id, execution_date, stream, *args, **kwargs):
        self.write(dag_id, task_id, execution_date, data=stream.read())

    def list_filenames_in_path(self, path):
        return self.sftp.listdir(path)

    def check_or_create_dir(self, dir):
        """
        Make sure our storage location exists.

        :param str dir: The directory name to look for and create if it
            doesn't exist..
        :return:
        """
        self.sftp.makedirs(dir)
