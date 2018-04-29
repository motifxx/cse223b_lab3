package triblab

import (
	"trib"
	"net"
	"net/rpc"
	"net/http"
	"fmt"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
	return &client{addr: addr}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	srv := rpc.NewServer()
	e := srv.RegisterName("Storage", b.Store)
	if e != nil {
		if b.Ready != nil {
			b.Ready <- false
		}
		return e
	}

	l, e := net.Listen("tcp", b.Addr)
	fmt.Println("srv2: ", b.Addr)
	if e != nil {
		if b.Ready != nil {
			b.Ready <- false
		}
		return e
	}


	if b.Ready != nil {
		b.Ready <- true
	}

	return http.Serve(l, srv)
}
