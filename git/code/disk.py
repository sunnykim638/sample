#!/usr/bin/env python3
import argparse, json, threading, socket, sys, time, uuid
from typing import Dict, Any, Tuple


SUCCESS = 0
FAILURE = 1
FREE = "Free"
INDSS = "InDSS"
BUF_SIZE = 65535
REASONS = {
    "DUPLICATE_NAME": "duplicate-name",
    "DSS_EXITST": "dss-exist",
    "DISK_IN_DSS": "disk-in-dss",
    "USERNAME_NOT_FOUND": "username-not-found",
    "DISKNAME_NOT_FOUND": "diskname-not-found",
    "INSUFFICIENT_DISKS": "insufficient-disks",
    "INVALID_PARAMS": "invalid-params",
    "PORT_CONFLICT": "port-conflict",
}

def log(role, name, event, **kwargs):
    # [2025-09-2 12:34:56] [MANAGER] [RECV] txid = , cmd=register-user, from=UserA@1.2.3.4:1111
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    base = f"[{ts}] [{role}] [{event}]"
    if name:
        base += f" [{name}]"
    if kwargs:
        details = " ".join(f"{k} = {v}" for k, v in kwargs.items())
        base += details
    print(base, flush=True)

def new_id() -> str:
    return str(uuid.uuid4())

def power_two(n: int) -> bool:
    return n > 0 and (n & (n-1)) == 0

def udp_socket(ip: str, port: int) -> socket.socket:
    sck = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sck.bind((ip, port))
    return sck

def send_json(sck: socket.socket, data: bytes, address: Tuple[str, int], role:str, name:str, event:str, **kwarg):
    sck.sendto(data, address)
    log(role, name, event, kwarg)

def request(command:str, tid: str, sender: Dict[str, Any], payload: Dict[str, Any]) -> bytes:
    obj = {
        "cmd": command,
        "txid": tid,
        "from": sender,
        "payload": payload,
    }
    return json.dumps(obj).encode("utf-8")

def parse_msg(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))

def response(tid: str, ret: int, reason: str = "", data: Dict[str, Any] = None) -> bytes:
    obj = {
        "txid": tid,
        "ret": ret,
    }
    if reason: obj["reason"] = reason
    if data: obj["data"] = data
    return json.dumps(obj).encode("utf-8")


def listener(sock: socket.socket, role: str, name: str):
    while True:
        data, addr = sock.recvfrom(BUF_SIZE)
        try:
            msg = parse_msg(data)
            log(role, name, "RECV", from_addr=addr, msg=msg)
        except Exception as e:
            log(role, name, "BADMSG", err=str(e), from_addr=addr)

def register(manager_ip: str, manager_port: int, disk_name: str, my_ip: str, m_port: int, c_port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    txid = new_id()
    req = request(
        "register-disk",
        txid,
        {"name": disk_name, "ip": my_ip, "m_port": m_port, "c_port": c_port},
        {"disk_name": disk_name, "ip": my_ip, "m_port": m_port, "c_port": c_port}
    )
    s.sendto(req, (manager_ip, manager_port))
    log("DISK", disk_name, "SEND", txid=txid, command="register-disk", to=f"{manager_ip}:{manager_port}")
    data, _ = s.recvfrom(BUF_SIZE)
    resp = parse_msg(data)
    log("DISK", disk_name, "RESP", txid=txid, ret=resp.get("ret"), reason=resp.get("reason", ""))

def dereg(manager_ip: str, manager_port: int, disk_name: str, my_ip: str, m_port: int, c_port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    txid = new_id()
    req = request(
        "deregister-disk",
        txid,
        {"name": disk_name, "ip": my_ip, "m_port": m_port, "c_port": c_port},
        {"disk_name": disk_name}
    )
    s.sendto(req, (manager_ip, manager_port))
    log("DISK", disk_name, "SEND", txid=txid, command="deregister-disk", to=f"{manager_ip}:{manager_port}")
    data, _ = s.recvfrom(BUF_SIZE)
    resp = parse_msg(data)
    log("DISK", disk_name, "RESP", txid=txid, ret=resp.get("ret"), reason=resp.get("reason", ""))

def main():
    ap = argparse.ArgumentParser(description="DSS Disk (UDP, multi-threaded skeleton)")
    ap.add_argument("--name", required=True) # disk name 
    ap.add_argument("--manager-ip", required=True)
    ap.add_argument("--manager-port", type=int, required=True)
    ap.add_argument("--m-port", type=int, required=True) # management port - used for communication between the manager and peers
    ap.add_argument("--c-port", type=int, required=True) # command port - used for communcation between peers
    args = ap.parse_args()

    my_ip = "127.0.0.1"
    m_sock = udp_socket(my_ip, args.m_port)
    c_sock = udp_socket(my_ip, args.c_port)

    threading.Thread(target=listener, args=(m_sock,"DISK",args.name), daemon=True).start()
    threading.Thread(target=listener, args=(c_sock,"DISK",args.name), daemon=True).start()

    log("DISK", args.name, "START", my_ip=my_ip, m_port=args.m_port, c_port=args.c_port)
    register(args.manager_ip, args.manager_port, args.name, my_ip, args.m_port, args.c_port)

    log("DISK", args.name, "READY", note="Press Ctrl+C to deregister and exit.")
    try:
        while True:
            input()
    except KeyboardInterrupt:
        pass

    dereg(args.manager_ip, args.manager_port, args.name, my_ip, args.m_port, args.c_port)
    log("DISK", args.name, "STOP")

if __name__ == "__main__":
    main()