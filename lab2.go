package triblab

import (
	"trib"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

const (
	USER_BIN = "_USERLIST_"
)

type BinI struct {
	trib.Storage

	bname string
	pstore trib.Storage
}

type VStorage struct {
	trib.BinStorage
	// bin_hash

	baddrs []string     // backend addresses
	binmap map[string]*BinI
}

type ServerI struct {
	trib.Server

	vstore trib.BinStorage
	users []string
}

// BinI 
func (self *BinI) Get(key string, value *string) error {
	return self.pstore.Get(self.bname+"::"+key, value)
}

func (self *BinI) Set(kv *trib.KeyValue, succ *bool) error {
	var kvb *trib.KeyValue

	if kv != nil {
		kvb = &trib.KeyValue{Key: self.bname+"::"+kv.Key, Value: kv.Value}
	} else {
		kvb = nil
	}

	return self.pstore.Set(kvb, succ)
}

func (self *BinI) rmPrefix(slist []string) []string {
	rlist := make([]string, 0)
	for _, s := range slist {
		rlist = append(rlist, strings.SplitN(s, "::", 2)[1])
	}
	return rlist
}

func (self *BinI) Keys(p *trib.Pattern, list *trib.List) error {
	var pb *trib.Pattern

	if p != nil {
		pb = &trib.Pattern{Prefix: self.bname+"::"+p.Prefix, Suffix: p.Suffix}
	} else {
		pb = nil
	}

	err := self.pstore.Keys(pb, list)
	if err != nil {
		return err
	}

	list.L = self.rmPrefix(list.L)
	return nil
}

func (self *BinI) ListGet(key string, list *trib.List) error {
	return self.pstore.ListGet(self.bname+"::"+key, list)
}

func (self *BinI) ListAppend(kv *trib.KeyValue, succ *bool) error {
	var kvb *trib.KeyValue

	if kv != nil {
		kvb = &trib.KeyValue{Key: self.bname+"::"+kv.Key, Value: kv.Value}
	} else {
		kvb = nil
	}

	return self.pstore.ListAppend(kvb, succ)
}

func (self *BinI) ListRemove(kv *trib.KeyValue, n* int) error {
	var kvb *trib.KeyValue

	if kv != nil {
		kvb = &trib.KeyValue{Key: self.bname+"::"+kv.Key, Value: kv.Value}
	} else {
		kvb = nil
	}

	return self.pstore.ListRemove(kvb, n)
}

func (self *BinI) ListKeys(p *trib.Pattern, list *trib.List) error {
	var pb *trib.Pattern

	if p != nil {
		pb = &trib.Pattern{Prefix: self.bname+"::"+p.Prefix, Suffix: p.Suffix}
	} else {
		pb = nil
	}

	err := self.pstore.ListKeys(pb, list)
	if err != nil {
		return err
	}

	list.L = self.rmPrefix(list.L)
	return nil
}

func (self *BinI) Clock(atLeast uint64, ret *uint64) error {
	return self.pstore.Clock(atLeast, ret)
}

var _ trib.Storage = new(BinI)


// VStorage
func (self *VStorage) bin_hash(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
        return uint32(h.Sum32()) % uint32(len(self.baddrs))
}

func (self *VStorage) Bin(name string) trib.Storage {
	if len(name)==0 {
		return nil
	}

	b, e := self.binmap[name]
	if e == true {
		return b
	}

	h := self.bin_hash(name)
	newbin := &BinI{bname: name, pstore: NewClient(self.baddrs[h])}
	self.binmap[name] = newbin
	return newbin
}

var _ trib.BinStorage = new(VStorage)


// Trib Sort Utility
type Less_Fn func(t1, t2 *trib.Trib) bool

type tribSort struct {
	tribs []*trib.Trib
	lessfns []Less_Fn
}

func (self *tribSort) Less(i, j int) bool {
	var k int
	for k = 0; k < len(self.lessfns)-1; k++ {
		if self.lessfns[k](self.tribs[i], self.tribs[j]) {
			return true
		}

		if self.lessfns[k](self.tribs[j], self.tribs[i]) {
			return false
		}
	}

	return self.lessfns[k](self.tribs[i], self.tribs[j])
}

func (self *tribSort) Sort(tribs []*trib.Trib) {
	self.tribs = tribs
	sort.Sort(self)
}

func (self *tribSort) Len() int {
	return len(self.tribs)
}

func (self *tribSort) Swap(i, j int) {
	self.tribs[i], self.tribs[j] = self.tribs[j], self.tribs[i]
}

func OrderedBy(lessfns ...Less_Fn) *tribSort {
	return &tribSort{lessfns: lessfns}
}

func less_clock(t1, t2 *trib.Trib) bool {
	return t1.Clock < t2.Clock
}

func less_time(t1, t2 *trib.Trib) bool {
	return t1.Time.Before(t2.Time)
}

func less_user(t1, t2 *trib.Trib) bool {
	return t1.User < t2.User
}

func less_message(t1, t2 *trib.Trib) bool {
	return t1.Message < t2.Message
}

func printTribs(tribs []*trib.Trib) {
	for _, t := range tribs {
		fmt.Println(t.Clock, t.Time, t.User, t.Message)
	}
}


// ServerI
func (self *ServerI) hasUser(user string) (bool, error) {
	var exist_flag string

	userDB := self.vstore.Bin(USER_BIN)
	err := userDB.Get(user, &exist_flag)
	if err != nil {
		return false, err
	}
	return exist_flag=="true", nil
}

func (self *ServerI) SignUp(user string) error {
	if !trib.IsValidUsername(user) {
		return fmt.Errorf("Invalid user name %q", user)
	}

	exist, err := self.hasUser(user)
	if err != nil {
		return err
	}

	if exist == true {
		return fmt.Errorf("User %q exists already.", user)
	}

	userDB := self.vstore.Bin(USER_BIN)
	var succ bool 
	err = userDB.Set(&trib.KeyValue{Key: user, Value: "true"}, &succ)
	if err != nil {
		return err
	}

	if succ != true {
		return fmt.Errorf("Error in registering user %q", user)
	}

	var clk uint64
	err = userDB.Clock(0, &clk)
	if err != nil {
		return err
	}

	return nil
}


func (self *ServerI) ListUsers() ([]string, error) {
	if len(self.users) >= trib.MinListUser {
		return self.users, nil
	}

	userDB := self.vstore.Bin(USER_BIN)
	var userlist trib.List
	err := userDB.Keys(&trib.Pattern{"",""}, &userlist)
	if err != nil {
		return nil, err
	}

	if len(userlist.L) < trib.MinListUser {
		self.users = userlist.L
	} else {
		self.users = userlist.L[0:trib.MinListUser]
	}

	sort.Strings(self.users)
	return self.users, nil
}


func (self *ServerI) getTribs(user string) ([]*trib.Trib, error) {
	var list trib.List
	bin := self.vstore.Bin(user)
	err := bin.ListGet("posts", &list)
	if err != nil {
		return nil, err
	}

	tribs := make([]*trib.Trib, 0)
	for i:=0; i<len(list.L); i++ {
		post := list.L[i]
		var t trib.Trib
		json.Unmarshal([]byte(post), &t)

		tribs = append(tribs, &t)
	}

	return tribs, nil
}


func (self *ServerI) expirePosts(user string) {
	tribs, err := self.getTribs(user)
	if err != nil {
		fmt.Errorf("Error: getTribs failure.")
		return
	}

	if len(tribs) <= trib.MaxTribFetch {
		return
	}

	OrderedBy(less_clock, less_time, less_user, less_message).Sort(tribs)

	bin := self.vstore.Bin(user)

	for i:=0; i<len(tribs)-trib.MaxTribFetch; i++ {
		post, _ := json.Marshal(*tribs[i])

		var n int
		err = bin.ListRemove(&trib.KeyValue{Key: "posts", Value: string(post)}, &n)
		if err != nil {
			fmt.Errorf("Error: expirePosts failure.")
		}

		var clk uint64
		err = bin.Clock(0, &clk)
		if err != nil {
			fmt.Errorf("Error: expirePosts clock update failure.")
		}
	}
}


func (self *ServerI) Post(who, post string, clock uint64) error {
	exist, err := self.hasUser(who)
	if err != nil {
		return err
	}

	if exist != true {
		return fmt.Errorf("user %q not found.", who)
	}

	if len(post) == 0 {
		return fmt.Errorf("Error: empty post.")
	}

	if len(post) > trib.MaxTribLen {
		return fmt.Errorf("Error: trib too long.")
	}

	
	bin := self.vstore.Bin(who)

	// sync clock
	var newclk uint64
	err = bin.Clock(clock, &newclk)
	if err != nil {
		return err
	}

	tb := trib.Trib{who, post, time.Now(), newclk}
	tb_json, errj := json.Marshal(tb)
	if errj != nil {
		return errj
	}

	var succ bool
	err = bin.ListAppend(&trib.KeyValue{"posts", string(tb_json)}, &succ)
	if err != nil  {
		return err
	}
	if succ != true {
		return fmt.Errorf("Error: post append failure.")
	}
	
	self.expirePosts(who)
	return nil
}


func (self *ServerI) Tribs(user string) ([]*trib.Trib, error) {
	exist, err := self.hasUser(user)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, fmt.Errorf("User %q not found.", user)
	}


	tribs, err2 := self.getTribs(user)
	if err2 != nil {
		return nil, err2
	}

	OrderedBy(less_clock, less_time, less_user, less_message).Sort(tribs)

	var tlist []*trib.Trib

	if len(tribs) <= trib.MaxTribFetch {
		tlist = tribs
	} else {
		tlist = tribs[len(tribs)-trib.MaxTribFetch:]
	}

	printTribs(tlist)
	return tlist, nil
}


func removeDup(s []string) []string {
	var ms map[string]int

	ms = make(map[string]int)

	for _, item := range s {
		ms[item] = 0
	}

	result := []string{}
	
	for key, _ := range ms {
		result = append(result, key)
	}

	return result
}


func (self *ServerI) Following(who string) ([]string, error) {
	exist, err := self.hasUser(who)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("User %q not found", who)
	}

	bin := self.vstore.Bin(who)
	var list trib.List
	err = bin.ListGet("follows", &list)
	if err != nil {
		return nil, err
	}

	return removeDup(list.L), nil
}


func (self *ServerI) IsFollowing(who, whom string) (bool, error) {
	exist, err := self.hasUser(who)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, fmt.Errorf("User %q not found", who)
	}

	exist, err = self.hasUser(whom)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, fmt.Errorf("User %q not found", whom)
	}

	if who == whom {
		return true, fmt.Errorf("Following oneself is prohibited.")
	}

	
	flist, err2 := self.Following(who)
	if err2 != nil {
		return false, err2
	}

	for _, w := range flist {
		if whom == w {
			return true, nil
		}
	}

	return false, nil
}



func (self *ServerI) Follow(who, whom string) error {
	exist, err := self.hasUser(who)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("User %q not found", who)
	}

	exist, err = self.hasUser(whom)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("User %q not found", whom)
	}

	if who == whom {
		return fmt.Errorf("Following oneself is prohibited.")
	}


	// is already following?
	isFollowing, err2 := self.IsFollowing(who, whom)
	if err2 != nil {
		return err2
	}

	if isFollowing {
		return fmt.Errorf("already following user %q.", whom)
	}


	flist, err3 := self.Following(who)
	if err3 != nil {
		return err3
	}

	if len(flist) >= trib.MaxFollowing {
		return fmt.Errorf("Reached max. limit of ", trib.MaxFollowing, " followees.")
	}

	bin := self.vstore.Bin(who)
	var b bool
	err = bin.ListAppend(&trib.KeyValue{"follows", whom}, &b)
	if err != nil {
		return err
	}
	if !b {
		return fmt.Errorf("Follow list append failed.")
	}

	var clk uint64
	err = bin.Clock(0, &clk)
	if err != nil {
		return err
	}

	return nil
}


func (self *ServerI) Unfollow(who, whom string) error {
	exist, err := self.hasUser(who)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("User %q not found.", who)
	}

	exist, err = self.hasUser(whom)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("User %q not found.", whom)
	}

	if who == whom {
		return fmt.Errorf("cannot unfollow yourself.")
	}


	is_following, err2 := self.IsFollowing(who, whom)
	if err2 != nil {
		return err2
	}

	if !is_following {
		return fmt.Errorf("User %q is not following %q.", who, whom)
	}

	bin := self.vstore.Bin(who)
	var n int
	err = bin.ListRemove(&trib.KeyValue{"follows", whom}, &n)
	if err != nil {
		return err
	}

	var clk uint64
	err = bin.Clock(0, &clk)
	if err != nil {
		return err
	}

	return nil
}


func (self *ServerI) Home(user string) ([]*trib.Trib, error) {
	exist, err := self.hasUser(user)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("User %q not found.", user)
	}

	flist, err2 := self.Following(user)
	if err2 != nil {
		return nil, err2
	}

	flist = append(flist, user)
	tribs := make([]*trib.Trib, 0)

	tribs_chan := make(chan []*trib.Trib, len(flist))
	ready_chan := make(chan bool, len(flist))

	// create multiple retrieval routines for each followee.
	for _, u := range flist {
		go func(user string) {
			tl := make([]*trib.Trib, 0)
			u_tribs, u_err := self.Tribs(user)
			if u_err != nil {
				fmt.Errorf("Error retrieving tribs for user %q.", user)
			}

			for _, trib := range u_tribs {
				tl = append(tl, trib)
			}

			tribs_chan <- tl
			ready_chan <- true
		}(u)
	}

	for i:=0; i<len(flist); i++ {
		<-ready_chan
		var tl = <-tribs_chan
		for _, tb := range tl {
			tribs = append(tribs, tb)
		}
	}

	OrderedBy(less_clock, less_time, less_user, less_message).Sort(tribs)

	if len(tribs) > trib.MaxTribFetch {
		tribs = tribs[len(tribs)-trib.MaxTribFetch:]
	}

	return tribs, nil
}


//
func NewBinClient(backs []string) trib.BinStorage {
	return &VStorage{baddrs: backs, binmap: make(map[string]*BinI)}
}

// defined in keeper.go
/*  
func ServeKeeper(kc *trib.KeeperConfig) error {
	panic("todo")
}
*/

func NewFront(s trib.BinStorage) trib.Server {
	return &ServerI{vstore: s, users: make([]string, 0)}
}
