#!/usr/bin/env python3
import argparse, json, random, socket, sys, time, uuid
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



class Manager:
    def __init__(self):
        self.dss: Dict[str, Dict[str, Any]] = {}
        self.users: Dict[str, Dict[str, Any]] = {}
        self.disks: Dict[str, Dict[str, Any]] = {}
        self.using_ports = set()

    def _port_key(self, ip: str, mport: int, cport: int):
        return (ip, mport), (ip, cport)

    def _reserve(self, ip: str, mport: int, cport: int):
        a, b = self._port_key(ip, mport, cport)
        self.using_ports.add(a)
        self.using_ports.add(b)

    def _release(self, ip: str, mport: int, cport: int):
        a, b = self._port_key(ip, mport, cport)
        self.using_ports.discard(a)
        self.using_ports.discard(b)

    def _check_conflict(self, ip: str, mport: int, cport: int) -> bool:
        a, b = self._port_key(ip, mport, cport)
        return (a in self.using_ports) or (b in self.using_ports)
    

def register_user(m: Manager, payload: Dict[str, Any]) -> Tuple[int, str]:
    user_name = payload.get("user_name", "")
    ip = payload.get("ip", "")
    mport = payload.get("mport", -1)
    cport = payload.get("cport", -1)
    print(user_name)
    print(ip)
    print(mport)
    print(cport)

    if not user_name or not ip or not (2500 <= mport <= 2999) or not (2500 <= cport <= 2999):
        return FAILURE, REASONS["INVALID_PARAMS"]
    
    if user_name in m.users:
        return FAILURE, REASONS["DUPLICATE_NAME"]
    
    if m._check_conflict(ip, mport, cport):
        return FAILURE, REASONS["PORT_CONFLICT"]
    
    m.users[user_name] = {"ip": ip, "mport": mport, "cport": cport}
    m._reserve(ip, mport, cport)
    return SUCCESS, ""

def deregister_user(m: Manager, payload: Dict[str, Any]) -> Tuple[int, str]:
    user_name = payload.get("user_name", "")
    if user_name in m.users:
        return FAILURE, REASONS["USERNAME_NOT_FOUND"]
    user = m.users.pop(user_name)
    m._release(user["ip"], user["mport"], user["cport"])
    return SUCCESS, ""



def register_disk(m: Manager, payload: Dict[str, Any]) -> Tuple[int, str]:
    disk_name = payload.get("disk_name", "")
    ip = payload.get("ip", "")
    mport = payload.get("mport", -1)
    cport = payload.get("cport", -1)

    if not disk_name or not ip or not (2500 <= mport <= 2999) or not (2500 <= cport <= 2999):
        return FAILURE, REASONS["INVALID_PARAMS"]
    
    if disk_name in m.disks:
        return FAILURE, REASONS["DUPLICATE_NAME"]
    
    if m._check_conflict(ip, mport, cport):
        return FAILURE, REASONS["PORT_CONFLICT"]
    
    m.disks[disk_name] = {"ip": ip, "mport": mport, "cport": cport, "state": FREE, "dss": None, "striping_unit": None}
    m._reserve(ip, mport, cport)
    return SUCCESS, ""

def deregister_disk(m: Manager, payload: Dict[str, Any]) -> Tuple[int, str]:
    disk_name = payload.get("user_name", "")
    if disk_name in m.disks:
        return FAILURE, REASONS["DISKNAME_NOT_FOUND"]
    info = m.disks[disk_name]
    if info["state"] != FREE:
        return FAILURE, REASONS["DISK_IN_DSS"]
    
    disk = m.disks.pop(disk_name)
    m._release(disk["ip"], disk["mport"], disk["cport"])
    return SUCCESS, ""


def configure_dss(m: Manager, payload: Dict[str, Any]) -> Tuple[int, str, Dict[str, Any]]:
    dss_name = payload.get("dss_name", "") # alphabetic string at most 15 characters
    n = int(payload.get("n", 0)) # n >= 3 number of disks in the disk array
    b = int(payload.get("striping_unit", 0)) # block size in bytes, power of two

    if not dss_name or n < 3 or not (128 <= b <= 1000000) or not power_two(b):
        return FAILURE, REASONS["INVALID_PARAMS"], {}
    
    if dss_name in m.dss:
        return FAILURE, REASONS["DSS_EXITST"], {}
    
    free_disk = [d for d, info in m.disks.items() if info["state"] == FREE]
    print(len(free_disk))
    print()
    if len(free_disk) < n:
        return FAILURE, REASONS["INSUFFICIENT_DISKS"], {}
    
    c = random.sample(free_disk, n)
    m.dss[dss_name] = {"n": n, "striping_unit": b, "disks": c}

    for d in c:
        m.disks[d]["state"] = INDSS
        m.disks[d]["dss"] = dss_name
        m.disks[d]["striping_unit"] = b
    
    response = {
        "dss_name": dss_name,
        "n": n,
        "striping_unit": b,
        "disks": [{
            "disk_name": d,
            "ip": m.disks[d]["ip"],
            "cport": m.disks[d]["cport"]
        } for d in c] 
    }
    return SUCCESS, "", response


def main():
    ap = argparse.ArgumentParser(description="DSS Manager")
    ap.add_argument("--mport", type=int, required=True, help="Manager listen port (from 2500 to 2999 )")
    args = ap.parse_args()
    host = "127.0.0.1"
    st = Manager()
    sock = udp_socket(host, args.mport)
    log("MANAGER", "manager", "START", host=host, mport=args.mport)

    while True:
        data, address = sock.recvfrom(BUF_SIZE)
        try:
            message = parse_msg(data)
        except Exception as e:
            log("MANAGER", "manager", "BADMSG", err=str(e), from_addr=address)
            continue

        txid = message.get("txid", "-")
        command  = message.get("cmd", "")
        src  = message.get("from", {})
        payload = message.get("payload", {})
        log("MANAGER", "manager", "RECV", txid=txid, command=command, src=src, from_addr=address)

        ret = FAILURE
        reason = ""
        data_out = {}

        try:
            if command == "register-user":
                ret, reason = register_user(st, payload)
            elif command == "register-disk":
                ret, reason = register_disk(st, payload)
            elif command == "configure-dss":
                ret, reason, data_out = configure_dss(st, payload)
            elif command == "deregister-user":
                ret, reason = deregister_user(st, payload)
            elif command == "deregister-disk":
                ret, reason = deregister_disk(st, payload)
            else:
                reason = REASONS["INVALID_PARAMS"]
        except Exception as e:
            ret, reason = FAILURE, f"exception:{e}"

        resp = response(txid, ret, reason, data_out if data_out else None)
        sock.sendto(resp, address)
        log("MANAGER", "manager", "SEND", txid=txid, ret=ret, reason=reason, to_addr=address)




if __name__ == "__main__":
    main()
