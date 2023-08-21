from requests import get


def verify_public_ip():
    ip = get('https://api.ipify.org').content.decode('utf8')
    return 'My public IP address is: {}'.format(ip)