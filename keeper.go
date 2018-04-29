package triblab

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"trib"
)

// KeeperClient
type KeeperClient struct {
	addr string
}

func (self *KeeperClient) GetBacks(stub string, backs *[]string) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	e = conn.Call("Keeper.GetBacks", stub, backs)
	if e != nil {
		conn.Close()
		return e
	}

	return conn.Close()
}

func (self *KeeperClient) GetId(stub string, myId *int64) error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	e = conn.Call("Keeper.GetId", stub, myId)
	if e != nil {
		conn.Close()
		return e
	}

	return conn.Close()
}


func NewKeeperClient(addr string) *KeeperClient {
	return &KeeperClient{addr: addr}
}


// Keeper with proper RPC interface
type Keeper struct {
	kconfig *trib.KeeperConfig
	// GetBacks
	// GetAddr
	// GetId
}

func (self *Keeper) GetBacks(stub string, backs *[]string) error {
	if self.kconfig == nil {
		return fmt.Errorf("Keeper not configured.")
	}

	*backs = self.kconfig.Backs
	return nil
}

/*
func (self *Keeper) GetAddr(stub string, myaddr *string) error {
	if self.kconfig == nil {
		return fmt.Errorf("Keeper not configured.")
	}

	*myaddr = self.kconfig.Addr()
	return nil
}
*/

func (self *Keeper) GetId(stub string, myId *int64) error {
	if self.kconfig == nil {
		return fmt.Errorf("Keeper not configured.")
	}

	*myId = self.kconfig.Id
	return nil
}


// repeating every 1s forever 
func bclk_sync(all_stores []trib.Storage) {
	var curr_max uint64
	curr_max = 0

	ticker := time.NewTicker(time.Second)
	clkChan := make(chan uint64)

	go func(tick <-chan time.Time){
		for {
			_ = <-tick

			for _, store := range all_stores {
				go func(s trib.Storage) {
					var ret uint64
					_ = s.Clock(curr_max, &ret)
					clkChan <- ret
				}(store)
			}

			// update max
			var max uint64
			max = 0
			for _ = range all_stores {
				clki := <-clkChan
				if clki > max {
					max = clki
				}
			}

			if max > curr_max {
				curr_max = max
			} else {
				curr_max = curr_max + 1
			}
		}
	}(ticker.C)
}

func ServeKeeper(kc *trib.KeeperConfig) error {
	if kc == nil {
		return fmt.Errorf("Invalid Keeper Config.")
	}

	// Check if any addresses are invalid
	for _, b := range kc.Backs {
		if b == "" {
			if kc.Ready != nil {
				kc.Ready <- false
			}
			return fmt.Errorf("Invalid back-ends address for Keeper config.")
		}
	}

	for _, k := range kc.Addrs {
		if k == "" {
			if kc.Ready != nil {
				kc.Ready <- false
			}
			return fmt.Errorf("Invalid Keeper address.")
		}
	}

	if kc.This >= len(kc.Addrs) {
		if kc.Ready != nil {
			kc.Ready <- false
		}
		return fmt.Errorf("Invalid Keeper this pointer.")
	}


	// Server Establishment
	var serverUp = make(chan bool, 1)
	var serverErr = make(chan error, 1)
	go func(kc *trib.KeeperConfig, ready chan bool, errs chan error) error {
		k := &Keeper{kconfig: kc}

		kserver := rpc.NewServer()
		err := kserver.RegisterName("Keeper", k)
		if err != nil {
			fmt.Println("Could not register keeper server")
			if ready != nil {
				ready <- false
			}
			if errs != nil {
				errs <- err
			}
			return err
		}

		l, e := net.Listen("tcp", kc.Addr())
		if e != nil {
			fmt.Println("Could not open keeper address %q for listen.", kc.Addr())
			if ready != nil {
				ready <- false
			}
			if errs != nil {
				errs <- e
			}
			return e
		}

		if ready != nil {
			ready <- true
		}
		if errs != nil {
			errs <- nil
		}

		return http.Serve(l, kserver)
	}(kc, serverUp, serverErr)

	serverReady := <-serverUp
	errS := <-serverErr

	if !serverReady {
		if kc.Ready != nil {
			kc.Ready <- false
		}
		return errS
	}

	// sync clocks of backends every 1 sec.
	go func(kc *trib.KeeperConfig) {
		// retrieve all respective backends which should have been created already before
		// keeper establishment.
		var all_stores = make([]trib.Storage, 0, len(kc.Backs))
		for _, baddr := range kc.Backs {
			all_stores = append(all_stores, NewClient(baddr))
		}

		// lab2: assume current keeper is always primary.
		// ticker repeat every 1s.
		bclk_sync(all_stores)
	}(kc)

	if kc.Ready != nil {
		kc.Ready <- true
	}

	return nil
}


