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
