import socket

from loguru import logger
from sshtunnel import SSHTunnelForwarder

LOCALHOST = '127.0.0.1'


def _get_random_port() -> int:
    s = socket.socket()
    s.bind((LOCALHOST, 0))
    return s.getsockname()[1]


def start_tunnel(host: str, port: int, user: str, key: str, target_host: str, target_port: int, local_port: int = None) -> int:
    local_port = local_port or _get_random_port()

    tunnel = SSHTunnelForwarder(
        (host, port),
        ssh_username=user,
        ssh_private_key=key,
        remote_bind_address=(target_host, target_port),
        local_bind_address=(LOCALHOST, local_port),
    )

    tunnel.start()

    return (tunnel, LOCALHOST, local_port)


def establish_tunnel(conf: dict, local_port: int = None) -> tuple:
    using_tunnel = bool(conf.get('tunnel_host'))
    local_host = LOCALHOST if using_tunnel else conf['host']

    z = start_tunnel(conf['tunnel_host'], conf['tunnel_port'], conf['tunnel_username'], conf['tunnel_key'], conf['host'], conf['port'], local_port=local_port)\
        if using_tunnel\
        else (None, local_host, conf['port'])

    if using_tunnel:
        logger.debug(f'🛣️  Tunnel established: {conf["host"]}:{conf["port"]} --> {conf["tunnel_username"]}@{conf["tunnel_host"]} --> {z[1]}:{z[2]}')

    return z
