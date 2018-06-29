from flask import Flask,request,redirect, url_for
import requests
import threading
import time
import pymysql
asdadasdasdasdadasdadasdadsdasdasdadadasdadasda
app = Flask(__name__)

LEADER = 0
CANDIDATE = 1
FOLLOWER = 2

A = {'type' : 'A', 'term':1, 'leader':1, 'data':''}#心跳
B = {'type' : 'B', 'term':1, 'candidate':1}#发起投票
C = {'type' : 'C', 'term':1, 'vote':1, 'my_id':1}#投票

serverListA  = {1:'http://127.0.0.1:1001/listen_A',
               2:'http://127.0.0.1:1002/listen_A',
               3:'http://127.0.0.1:1003/listen_A',
               4:'http://127.0.0.1:1004/listen_A',
               5:'http://127.0.0.1:1005/listen_A'}
serverListB  = {1:'http://127.0.0.1:1001/listen_B',
               2:'http://127.0.0.1:1002/listen_B',
               3:'http://127.0.0.1:1003/listen_B',
               4:'http://127.0.0.1:1004/listen_B',
               5:'http://127.0.0.1:1005/listen_B'}
serverListC  = {1:'http://127.0.0.1:1001/listen_C',
               2:'http://127.0.0.1:1002/listen_C',
               3:'http://127.0.0.1:1003/listen_C',
               4:'http://127.0.0.1:1004/listen_C',
               5:'http://127.0.0.1:1005/listen_C'}
portList = {1:1001,
            2:1002,
            3:1003,
            4:1004,
            5:1005}
DBList = {  'id':0,
            'term':1,
            'leader':2,
            'state':3,
            'my_vote':4,
            'vote_flag':5
            }

#leader = 1
my_server = 1
timeouts = 5#如果Timeouts时间内没有收到leader的心跳，则升级为candidate，并发送投票请求
waitingTimeouts = 5#发送投票请求后，如果waitingTimeouts时间内没有收到回复，或者票数不够成为leader，则再次发送投票请求
#my_term = 1
#my_state = FOLLOWER
#my_vote = 0
#vote_flag = False

def get_InfoFromDatabase(name):
    conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='root',
        db='serverInfo',
        charset='utf8'
    )
    # 获取游标
    cursor = conn.cursor()
    #获取数据
    sql_select = "SELECT *FROM server WHERE id=" + str(my_server)
    # cursor执行sql语句
    cursor.execute(sql_select)
    # 使用fetchall方法
    serverInfo = cursor.fetchall()  # 将所有的结果放入rr中
    #处理数据
    raw = serverInfo[0]
    tmpInfo = raw[DBList[name]]
    # 关闭数据库
    cursor.close()
    conn.close()

    return tmpInfo

def set_InfoFromDatabase(name,value):
    conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='root',
        db='serverInfo',
        charset='utf8'
    )
    # 获取游标
    cursor = conn.cursor()
    # 修改数据
    sql_select = "UPDATE server SET " + str(name) + "=" + str(value) + " WHERE id=" + str(my_server)
    #print(sql_select)
    # cursor执行sql语句
    cursor.execute(sql_select)
    # 事务提交，否则数据库得不到更新
    cursor.close()
    conn.commit()
    conn.close()

def entry_post(url,data,timeout):
    try:
        requests.post(url, data=data, timeout=timeout)  # 获取服务器响应
    except BaseException:
        print('post error:' + url + ', ')
    else:
        print('post OK')

def broadcast(type, data):
    # entry_post('http://127.0.0.1:1001/listen_' + type, data=data,timeout=1)  # 获取服务器响应
    entry_post('http://127.0.0.1:1002/listen_' + type, data=data,timeout=1)  # 获取服务器响应
    entry_post('http://127.0.0.1:1003/listen_' + type, data=data,timeout=1)  # 获取服务器响应
    entry_post('http://127.0.0.1:1004/listen_' + type, data=data,timeout=1)  # 获取服务器响应
    entry_post('http://127.0.0.1:1005/listen_' + type, data=data,timeout=1)  # 获取服务器响应

def candidate_startVoting():
    global B
    global my_waitingTimer
    my_term = get_InfoFromDatabase('term')
    my_state = get_InfoFromDatabase('state')
    my_vote = get_InfoFromDatabase('my_vote')
    vote_flag = get_InfoFromDatabase('vote_flag')
    if vote_flag == 0:
        my_state = CANDIDATE
        my_term = my_term + 1
        my_vote = my_vote +1

        print("candidate_startVoting term:" + str(my_term))
        set_InfoFromDatabase('term', my_term)
        set_InfoFromDatabase('state', my_state)
        set_InfoFromDatabase('my_vote', my_vote)
        B['term'] = my_term
        B['candidate'] = my_server
        print('我是1，我时间到了，让大家投票吧')
        print(B)
        # 重置定时器
        my_waitingTimer.cancel()
        my_waitingTimer = threading.Timer(waitingTimeouts, sleepBeforeNextVoting)
        my_waitingTimer.start()

        broadcast('B',B)
        print('--我说完了')

def sleepBeforeNextVoting():
    global my_timer
    #将状态置为FOLLOWER，防止出现多个CANDIDATE互锁
    set_InfoFromDatabase('state', FOLLOWER)
    my_timer = threading.Timer(timeouts, candidate_startVoting)
    my_timer.start()

def candidate_becomeLeader():
   # global A
    #A['term'] = 3
    #A['leader'] = my_server
    global my_waitingTimer
    set_InfoFromDatabase('leader', my_server)
    set_InfoFromDatabase('state', LEADER)
    print(str(my_server) + ' become leader!')
    #entry_post('http://127.0.0.1:1002/listen_A', data=A,timeout=1)  # 获取服务器响应
    my_waitingTimer.cancel()
    leader_sendCycle()

def leader_sendCycle():
    global A
    my_term = get_InfoFromDatabase('term')
    leader = get_InfoFromDatabase('leader')
    A['term'] = my_term
    A['leader'] = my_server

    broadcast('A',A)
    print('leader_sendCycle...')

    cycleTimer = threading.Timer(1, leader_sendCycle)
    cycleTimer.start()

    return ''

my_timer = threading.Timer(timeouts, candidate_startVoting)
my_waitingTimer = threading.Timer(waitingTimeouts, candidate_startVoting)

def init():
    #global my_timer
    set_InfoFromDatabase('term', 1)
    set_InfoFromDatabase('leader', 0)
    set_InfoFromDatabase('state', 2)
    set_InfoFromDatabase('my_vote', 0)
    set_InfoFromDatabase('vote_flag', 0)
    my_timer.start()

@app.route('/')
def index():
    return redirect(url_for('listen'), code=302)  # URL跳转，默认代码是302，可以省略

@app.route('/listen_A',methods=['POST'])
def listen_A():
    my_state = get_InfoFromDatabase('state')
    vote_flag = get_InfoFromDatabase('vote_flag')
    my_term = get_InfoFromDatabase('term')
    global C
    global serverList
    global my_server
    global my_timer

    method = request.method
    type = request.form['type']
    term = int(request.form['term'])
    leader = int(request.form['leader'])
    data = request.form['data']
    if method == 'POST' and type == 'A' and term >= my_term:
        my_state = FOLLOWER
        vote_flag = 0
        set_InfoFromDatabase('state', my_state)
        set_InfoFromDatabase('vote_flag', vote_flag)
        set_InfoFromDatabase('term', term)
        set_InfoFromDatabase('leader', leader)

        my_timer.cancel()
        my_timer = threading.Timer(timeouts, candidate_startVoting)
        my_timer.start()

        print('我收到来自leader的心跳，bonbonbon.....')
    else:
        print('Not Post or not type A!!!!')
    return 'over'

@app.route('/listen_B',methods=['POST'])
def listen_B():
    global C
    global serverList
    global my_server
    global my_timer
    global my_waitingTimer

    method = request.method
    type = request.form['type']
    term = int(request.form['term'])
    if method == 'POST' and type == 'B':
        my_timer.cancel()
        my_waitingTimer.cancel()
        tmp_candidate = int(request.form['candidate'])
        print(str(tmp_candidate) + ' server发过投票请求过来，我是' +  str(my_server))
        my_term = get_InfoFromDatabase('term')
        vote_flag = get_InfoFromDatabase('vote_flag')
        my_state = get_InfoFromDatabase('state')
        if term > my_term and my_state == FOLLOWER and vote_flag == 0:  # 用term检查本机是否已经投过票了
            my_term = my_term + 1
            set_InfoFromDatabase('term', my_term)
            #print("follower_typeHandling_B term:" + str(my_term))
            C['vote'] = tmp_candidate
            C['term'] = my_term
            C['my_id'] = my_server
            tmp_host = serverListC[tmp_candidate]
            entry_post(tmp_host, data=C,timeout=1)  # 获取服务器响应
            vote_flag = 1
            set_InfoFromDatabase('vote_flag', vote_flag)
            print('---我投了票')
            print(C)
        else:
            print('---我没理他')
            print(term)
            print(my_term)
            print(my_state)
            print(vote_flag)
    else:
        print('Not Post or not type B!!!!')
    return 'over'

@app.route('/listen_C',methods=['POST'])
def listen_C():
    global C
    global serverList
    global my_server
    global my_waitingTimer

    method = request.method
    type = request.form['type']
    term = int(request.form['term'])
    my_state = get_InfoFromDatabase('state')
    if method == 'POST' and type == 'C' and my_state == CANDIDATE:
        tmp_vote = int(request.form['vote'])
        voter_id = int(request.form['my_id'])
        #time.sleep(2)
        print(str(voter_id) + ' server投票，我是' +  str(my_server))
        my_vote = get_InfoFromDatabase('my_vote')
        my_term = get_InfoFromDatabase('term')
        if tmp_vote == my_server:  # 检查是不是投给本机的票，任期号是否一致
            my_vote = my_vote + 1
            print(my_vote)
            set_InfoFromDatabase('my_vote', my_vote)
            #重置定时器
            my_waitingTimer.cancel()
            my_waitingTimer = threading.Timer(waitingTimeouts, candidate_startVoting)
            my_waitingTimer.start()
        else:
            print(tmp_vote)
            print(my_server)
            print(term)
            print(my_term)
        if my_vote > 2:
            candidate_becomeLeader()
            my_vote = 0
            set_InfoFromDatabase('my_vote', my_vote)
        else:
            print('投票数不够，目前有' + str(my_vote) + '票')
        # print('my vote is ' + str(my_vote))
        # print('my term is ' + str(my_term))
    else:
        print('Not Post or not type C!!!!')
    return 'over'

if __name__ == '__main__':
    print("OK !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    init()
    app.run(host='localhost',port=portList[my_server],debug=True,use_reloader=False)
    #while True:
        #r = entry_post("http://127.0.0.1:1002/listen", data=A)
        #time.sleep(1)
    # get_InfoFromDatabase('term')
    # get_InfoFromDatabase('leader')
    # get_InfoFromDatabase('state')
    # get_InfoFromDatabase('my_vote')
    # get_InfoFromDatabase('vote_flag')
    #
    # set_InfoFromDatabase('term',1)
    # set_InfoFromDatabase('leader',0)
    # set_InfoFromDatabase('state',2)
    # set_InfoFromDatabase('my_vote',0)
    # set_InfoFromDatabase('vote_flag',0)
    #
    # get_InfoFromDatabase('term')
    # get_InfoFromDatabase('leader')
    # get_InfoFromDatabase('state')
    # get_InfoFromDatabase('my_vote')
    # get_InfoFromDatabase('vote_flag')

































