package triblab

import (
	"trib"
	"net/rpc"
	"time"
	"fmt"
)

type client struct {
	// server address
	addr string
}

// implement KeyString interface
func (self *client) Get(key string, value *string) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.Get", &key, value)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.Get latency = ", elapsed)


	// close connection
	return conn.Close()
}

func (self *client) Set(kv *trib.KeyValue, succ *bool) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.Set", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.Set latency = ", elapsed)

	// close connection
	return conn.Close()
}

func (self *client) Keys(p *trib.Pattern, list *trib.List) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.Keys", p, list)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.Keys latency = ", elapsed)

	if list.L == nil {
		list.L = []string{}
	}

	// close connection
	return conn.Close()
}

// implement KeyList interface 
func (self *client) ListGet(key string, list *trib.List) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.ListGet", &key, list)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.ListGet latency = ", elapsed)

	if list.L == nil {
		list.L = []string{}
	}

	// close connection
	return conn.Close()
}

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.ListAppend", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.ListAppend latency = ", elapsed)

	// close connection
	return conn.Close()
}

func (self *client) ListRemove(kv *trib.KeyValue, n *int) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.ListRemove", kv, n)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.ListRemove latency = ", elapsed)

	// close connection
	return conn.Close()
}

func (self *client) ListKeys(p *trib.Pattern, list *trib.List) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.ListKeys", p, list)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.ListKeys latency = ", elapsed)

	if list.L == nil {
		list.L = []string{}
	}

	// close connection
	return conn.Close()
}

// implement clock
func (self *client) Clock(atLeast uint64, ret *uint64) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	tstart := time.Now()
	e = conn.Call("Storage.Clock", &atLeast, ret)
	if e != nil {
		conn.Close()
		return e
	}
	elapsed := time.Since(tstart)
	fmt.Println("Storage.Clock latency = ", elapsed)

	// close connection
	return conn.Close()
}

// test creation
var _ trib.Storage = new(client)

