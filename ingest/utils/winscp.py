import logging
import pysftp
from pathlib import Path


def verify_known_host(host, username, private_key) -> pysftp.CnOpts:
    """
    verify host by loading host information if exists and downloading host public key into /.ssh/known_hosts if not
    @param host url endpoint of the host
    @param username 
    @param private_key path to private key file (.pem format)
    """

    # Loads .ssh/known_hosts  
    logging.info("Loading known hosts")  
    cnopts = pysftp.CnOpts()
    hostkeys = None

    path = Path.joinpath(Path.home(), '.ssh')
    logging.info("Ensuring %s exists", path)  
    path.mkdir(parents=True, exist_ok=True)

    if cnopts.hostkeys.lookup(host) == None:
        logging.info("New host - will accept any host key")
        # Backup loaded .ssh/known_hosts file
        hostkeys = cnopts.hostkeys
        # And do not verify host key of the new host
        cnopts.hostkeys = None

    with pysftp.Connection(host, username=username, private_key=private_key, cnopts=cnopts) as sftp:
        if hostkeys != None:
            logging.info("Connected to new host, caching its hostkey")
            hostkeys.add(
                host, sftp.remote_server_key.get_name(), sftp.remote_server_key)
            hostkeys.save(pysftp.helpers.known_hosts())

    return cnopts


def ls_remote_directory(remote_dir, host: str, username: str, private_key: str, cnopts: pysftp.CnOpts) -> str:
    """list files in a remote directory"""
    with pysftp.Connection(host, username=username, private_key=private_key, cnopts=cnopts) as sftp:

        # Switch to a remote directory
        sftp.cwd(remote_dir)

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

        # Print data
        for attr in directory_structure:
            logging.info(f'{attr.filename}, {attr}')


def download_remote_file(remote_file, local_file, host: str, username: str, private_key: str, cnopts: pysftp.CnOpts) -> str:
    """download file from remote directory"""

    with pysftp.Connection(host, username=username, private_key=private_key, cnopts=cnopts) as sftp:
        
        logging.info("Connection succesfully stablished ... ")
        logging.info("Downloading file from %s", remote_file)
        sftp.get(remote_file, local_file)



