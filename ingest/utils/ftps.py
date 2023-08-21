import logging
from ftplib import FTP_TLS, FTP


class MyFTP_TLS(FTP_TLS):
    """Explicit FTPS, with shared TLS session"""
    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # this is the fix
        return conn, size


def ftps_list_dirs(host: str, username: str, password: str, remote_dir: str):
    """List files in remote folder"""
    with MyFTP_TLS(host) as ftps:
        # ftps = MyFTP_TLS(host)
        # login after securing control channel
        ftps.login(username, password)           
        # switch to secure data connection.. 
        # IMPORTANT! Otherwise, only the user and password is encrypted and
        # not all the file data.
        ftps.prot_p()    

        ftps.cwd(remote_dir)

        ftps.retrlines('LIST')

        # filename = 'remote_filename.bin'
        # print('Opening local file ' + filename)
        # with open(filename, 'wb') as myfile:
        #     ftps.retrbinary('RETR %s' % filename, myfile.write)

        ftps.close()


def ftps_download_file(host: str, username: str, password: str, remote_dir: str, filename: str, local_dir: str):
    """
    Download file from remote FTP host using username and password
    @param remote_dir remote directory containing the file
    @param filename name of file in remote directory
    @param local_dir local directory to download file into
    """
    with MyFTP_TLS(host) as ftps:

        ftps = MyFTP_TLS(host)

        ftps.login(username, password)           
        # switch to secure data connection.. 
        # IMPORTANT! Otherwise, only the user and password is encrypted and
        # not all the file data.
        ftps.prot_p() 

        ftps.cwd(remote_dir)

        with open(f'{local_dir}/{filename}', "wb") as file:
            logging.info('Opening local file ' + filename)

            # use FTP's RETR command to download the file
            ftps.retrbinary(f"RETR {filename}", file.write)

        ftps.close()