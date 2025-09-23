import pandas as pd
import paramiko, socket, select, threading, pymysql
import requests
import datetime
headers = {
    'Content-Type': 'application/json;charset=UTF-8'
}
url = 'https://a.engageminds.ai/s2s/es'
# 提现数据 - mysql中获取
def start_ssh_local_forward(ssh_host, ssh_port, ssh_user, key_path, key_passphrase,
                            remote_host, remote_port, local_host="127.0.0.1", local_port=0):
    """
    建立一个 SSH 本地转发，返回 (closer, bound_port)。
    closer() 用于关闭转发及 SSH 连接。
    """
    # 1) 建 SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=ssh_host,
        port=ssh_port,
        username=ssh_user,
        key_filename=key_path,
        passphrase=key_passphrase,   # 无口令可传 None 或省略该参数
        allow_agent=True,
        look_for_keys=False,
    )
    transport = ssh.get_transport()

    # 2) 开本地监听（端口 0 表示自动分配）
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((local_host, local_port))
    listen_sock.listen(64)
    bound_port = listen_sock.getsockname()[1]

    stop_evt = threading.Event()

    def _pipe(client_sock):
        try:
            # 通过 SSH 打开到 MySQL 的直连通道
            chan = transport.open_channel(
                kind='direct-tcpip',
                dest_addr=(remote_host, remote_port),
                src_addr=client_sock.getsockname(),
            )
            while True:
                rlist, _, _ = select.select([client_sock, chan], [], [], 60)
                if client_sock in rlist:
                    data = client_sock.recv(16384)
                    if not data:
                        break
                    chan.sendall(data)
                if chan in rlist:
                    data = chan.recv(16384)
                    if not data:
                        break
                    client_sock.sendall(data)
        finally:
            try: chan.close()
            except: pass
            try: client_sock.close()
            except: pass

    def _accept_loop():
        while not stop_evt.is_set():
            r, _, _ = select.select([listen_sock], [], [], 0.5)
            if listen_sock in r:
                try:
                    s, _ = listen_sock.accept()
                except OSError:
                    break
                threading.Thread(target=_pipe, args=(s,), daemon=True).start()
        try: listen_sock.close()
        except: pass

    t = threading.Thread(target=_accept_loop, daemon=True)
    t.start()

    def closer():
        stop_evt.set()
        try: listen_sock.close()
        except: pass
        try: ssh.close()
        except: pass

    return closer, bound_port


# ==== 使用示例 ====
SSH_HOST = "118.194.235.114"
SSH_PORT = 22
SSH_USER = "root"
SSH_KEY = "D:\Desktop\mania777-mysql\mania777-mysql\id_rsa"   # 或 id_rsa
SSH_PASSPHRASE = None                   # 私钥口令；无口令就 None

MYSQL_RHOST = "127.0.0.1"  # MySQL 在 SSH 机器上的地址
MYSQL_RPORT = 3306
MYSQL_USER = "rlj"
MYSQL_PASS = "hdETSg#hfa.!a"
MYSQL_DB   = "mania777"

closer, local_port = start_ssh_local_forward(
    SSH_HOST, SSH_PORT, SSH_USER, SSH_KEY, SSH_PASSPHRASE,
    MYSQL_RHOST, MYSQL_RPORT,
)
conn = pymysql.connect(
    host="127.0.0.1",
    port=local_port,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB,
    charset="utf8mb4",
    autocommit=False,  # 显式禁用自动提交
    connect_timeout=10, read_timeout=30, write_timeout=30,
    # cursorclass=pymysql.cursors.DictCursor
)
# 获取当日数据
start= (datetime.date.today()).strftime('%Y-%m-%d')
end = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
sql ="""
SELECT user_id,amount,created_at,order_no
FROM prod_user_withdraw_history
where created_at between '{0}' and '{1}'
and mark = 'ok'
and status =1
"""
df = pd.read_sql(sql.format(start,end), conn)
# 关闭连接
conn.close()
df['sts'] = pd.to_datetime(df['created_at']).astype('int64')// 10**6

# 组装数据
def data_to_em(df):
    body_list =[]
    if df.shape[0]>0:
        for index,row in df.iterrows():
            dic = {
            "ts": row["sts"],
            "appk":'UVxs42Ho3FDdQ1wLQxIkk1J4r00HUwwX',
            "uid": row['user_id'],
            "os":1,
            "country":'',
            "events": [
              {
                "ts": row['sts'],
                "cdid": row['user_id'],
                "eid": "withdraw",
                "props": {
                    'withdraw_amount':row['amount'],
                    'data_type':'补偿上报',
                    'date_diff':'1小时'
                }}
            ]
            }
            body_list.append(dic)
        response=requests.post(url, headers=headers, json=body_list)
    else:
        pass


# 读取本地存储，判断是否已经上报
def get_data(df):
    df2 = pd.read_excel('mania提现补偿上报.xlsx')
    if df2.shape[0] == 0:
        # 还未存入数据,直接覆盖
        df.to_excel('mania提现补偿上报.xlsx')
        data_to_em(df)

    else:
        # 判断订单号是否已经上报过
        order_no = df2['order_no'].to_list()
        res = df[~df['order_no'].isin(order_no)]
        data_to_em(res)
        # 覆盖本地文件
        df.to_excel('mania提现补偿上报.xlsx')
if __name__ == '__main__':
    get_data(df)
    pass

