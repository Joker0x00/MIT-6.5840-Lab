Lab 地址

> [https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv.html](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv.html)

该lab主要内容是实现一个具有linearizable的kv单Server系统，实现`Get`, `Put`和`Append`方法。

需要注意以下问题：

- 多个Client同时发起请求，Server需要在共享数据加锁。
- Client的RPC调用可能会由于网络问题失败，需要重复向Server发起请求。
- Server需要过滤掉重复的请求，并返回缓存的结果。

可能发生的情况如下图所示：

![](https://secure2.wostatic.cn/static/kpMWt67K4QSRF8ceQvQWXm/image.png?auth_key=1722782126-7LKCC1w4hA84wyMEyNNmiG-0-cfd30b75c6a647fd5a890f0e7a74b197)

为了确保Server能够过滤重复请求，客户端在发起请求时，携带ID（随机数）和Req（请求序号）。服务端在内存中定义Map数据结构哦，保存请求客户端的ID和下一个Req。当请求来临时，Server通过检查自己保存的Req是否等于请求的Req，来判断是否重复请求。如果保存的Req和Client的请求Req相等时则是新的请求，执行指令；否则是重复请求，返回缓存的数据。

再执行完Put和Append请求后，都在Map中缓存返回的结果，以便重复请求来临时直接返回。

## Linearization

在单服务系统中，很容易实现线性一致性。线性一致性是根据客户端的输入输出历史记录来定义的。

有两个限制条件：

1. 如果一个操作在另一个操作开始前就结束了，那么这个操作必须在执行历史中出现在另一个操作前面。
2. 执行历史中，读操作，必须在相应的key的写操作之后。

如果能将历史记录构造成一个没有循环的序列，且满足上述要求，则说明该系统具备Linearization。

详细解释如下：

> [https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-07-raft2/7.6-qiang-yi-zhi-linearizability](https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-07-raft2/7.6-qiang-yi-zhi-linearizability)

## 更多思路

> [https://blog.csdn.net/hzf0701/article/details/138904641](https://blog.csdn.net/hzf0701/article/details/138904641)

该文中客户端成功接收到服务端的响应后，立即向服务端发起RPC调用，告诉服务端可以将缓存数据删除，节省更多内存空间。利用map存储每一次任务的执行结果，便于检查是否重复，简洁明了。

## 代码实现

### client.go

```Go
package kvsrv

import (
  "crypto/rand"
  "log"
  "math/big"
  "sync"
  "time"

  "6.5840/labrpc"
)

type Clerk struct {
  server *labrpc.ClientEnd
  // You will have to modify this struct.
  id             int64
  req            int
  retryTime      time.Duration
  requestTimeout time.Duration
  mtx            sync.Mutex
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
  ck := new(Clerk)
  ck.server = server
  // You'll have to add code here.
  ck.id = nrand()
  ck.req = 0
  ck.retryTime = 50 * time.Millisecond
  ck.requestTimeout = 500 * time.Millisecond
  ck.mtx = sync.Mutex{}
  return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  args := GetArgs{
    Key: key,
    Id:  ck.id,
    Req: ck.req,
  }
  reply := GetReply{}

  for {
    select {
    case <-time.After(ck.requestTimeout):
      {
        log.Fatalf("get rpc timeout")
        return ""
      }
    default:
      // log.Printf("client %v get, req: %v", ck.id, ck.req)
      ok := ck.server.Call("KVServer.Get", &args, &reply)
      if ok {
        return reply.Value
      }
      // log.Printf("client %v failed, retry...", ck.id)
      time.Sleep(ck.retryTime)
    }
  }
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
  // You will have to modify this function.
  args := PutAppendArgs{
    Key:   key,
    Value: value,
    Id:    ck.id,
    Req:   ck.req,
  }
  reply := PutAppendReply{}

  for {
    select {
    case <-time.After(ck.requestTimeout):
      {
        log.Fatalf("putAppend rpc timeout")
        return ""
      }
    default:
      // log.Printf("client %v %v, req: %v, k: %v, v: %v", ck.id, op, ck.req, key, value)
      ok := ck.server.Call("KVServer."+op, &args, &reply)
      if ok {
        ck.req++
        // ck.Ack(ck.req)
        return reply.Value
      }
      // log.Printf("client %v failed, retry...", ck.id)
      time.Sleep(ck.retryTime)
    }
  }

}

func (ck *Clerk) Put(key string, value string) {
  ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
  return ck.PutAppend(key, value, "Append")
}
```



### server.go

```Go
package kvsrv

import (
  "log"
  "sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug {
    log.Printf(format, a...)
  }
  return
}

type Client struct {
  req int
  res string
}

type KVServer struct {
  mu sync.Mutex

  // Your definitions here.
  data   map[string]string
  client map[int64]Client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  client := kv.client[args.Id]
  // log.Printf("server: c %v, put req: %v, nowReq: %v, append, k: %v, v: %v", args.Id, args.Req, client.req, args.Key, args.Value)
  if args.Req >= client.req {
    client.req = args.Req + 1
    // log.Printf("update req for %v: %v", args.Id, client.req)
    delete(kv.data, args.Key)
    kv.data[args.Key] = args.Value
    kv.client[args.Id] = client
  }
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  client := kv.client[args.Id]
  // log.Printf("server: c %v, append req: %v, nowReq: %v, append, k: %v, v: %v", args.Id, args.Req, client.req, args.Key, args.Value)
  if args.Req >= client.req {
    client.req = args.Req + 1
    // log.Printf("update req for %v: %v", args.Id, client.req)
    client.res = kv.data[args.Key]
    kv.data[args.Key] += args.Value
    kv.client[args.Id] = client
  } else {
    // log.Printf("retry request: %v", args.Id)
  }
  reply.Value = client.res
}

func StartKVServer() *KVServer {
  kv := new(KVServer)
  // You may need initialization code here.
  kv.data = make(map[string]string)
  kv.client = make(map[int64]Client)
  kv.mu = sync.Mutex{}
  return kv
}

```



## common.go

```Go
package kvsrv

// Put or Append
type PutAppendArgs struct {
  Key   string
  Value string
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Id  int64
  Req int
}

type PutAppendReply struct {
  Value string
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Id  int64
  Req int
}

type GetReply struct {
  Value string
}

```



## 测试

```Bash
go test

```

![](https://secure2.wostatic.cn/static/gqiUoKPf9aLx4iTwcNgDJ4/image.png?auth_key=1722782843-31ENBW6G1PSvk8YMuTu6q4-0-aec1c0b9fd042ca04e3e166c20782a9b)

MIT6.5840 课程Lab完整项目

> [https://github.com/Joker0x00/MIT-6.5840-Lab/](https://github.com/Joker0x00/MIT-6.5840-Lab/)

