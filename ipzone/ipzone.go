package ipzone

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dale-di/ipdispatch/rbtree"
)

// Zone 是IP段与地域运营商的对应关系
type Zone struct {
	ipmin uint32
	ipmax uint32
	name  string
	id    int
}

// Node 节点信息
type Node struct {
	servers         []*Server
	serverID        map[string]int
	curserver       *Server
	servercount     int
	balance         byte
	bw              int
	maxbw           int
	freebw          int
	name            string
	id              int
	vhost           string
	status          int
	overflow2node   string
	overflow2nodeid int
	swtree          *rbtree.Tree
	sw              []int
	reqlastmin      uint64 //上一分钟请求数
	reqmin          uint64
	reqcount        uint64 //分配到此节点的请求计数
}

//Server 服务器信息
type Server struct {
	next      *Server
	ip        string
	weight    int
	weightstr string
	id        int
	status    int
}

//ServerWeight 服务器权重信息
type ServerWeight struct {
	server *Server
	keymin uint32
	keymax uint32
	key    uint32
}

//Vhost 虚拟主机的配置信息
type Vhost struct {
	id          int
	name        string
	zone2node   [1000]int
	nodes       []*Node
	nodeID      map[string]int
	defaultNode int
	reqcount    uint64
}

//IPDisp IP调度配置入口
type IPDisp struct {
	zoneID     map[string]int
	zoneMax    int
	vhosts     map[string]*Vhost
	rbtree     *rbtree.Tree
	mutex      sync.Mutex
	reqcount   uint64
	othercount uint64
}

const (
	//swMAX 设置权重最大值
	swMAX = 10000
)

var serverstat = map[string]int{"up": 0, "down": 2, "backup": 4}

//New 初始化IPDisp
func New() *IPDisp {
	ipdisp := &IPDisp{}
	ipdisp.zoneID = make(map[string]int)
	ipdisp.vhosts = make(map[string]*Vhost)
	ipdisp.reqcount = 0
	ipdisp.othercount = 0
	return ipdisp
}

//Name 返回zone的名称
func (zone *Zone) Name() (name string) {
	return zone.name
}

//GetCount 获取统计信息
func (ipdisp *IPDisp) GetCount(host string, node string, last string) (count uint64) {
	count = 0
	//fmt.Printf("IPDispF: %v\n", *ipdisp)
	switch node {
	case "none":
		vhost, ok := ipdisp.vhosts[host]
		if ok == true {
			count = vhost.reqcount
		}
	case "all":
		count = ipdisp.reqcount
	case "other":
		count = ipdisp.othercount
	default:
		vhost, ok := ipdisp.vhosts[host]
		if ok == true {
			nid, ok1 := vhost.nodeID[node]
			if ok1 == true {
				//fmt.Printf("%v\n", vhost.nodes[nid].swtree.String())
				if last != "" {
					count = vhost.nodes[nid].reqlastmin
				} else {
					count = vhost.nodes[nid].reqcount
				}
			}
		}
	}
	return
}

//Set 动态变更节点带宽，节点状态，服务器权重，服务器状态
func (ipdisp *IPDisp) Set(host string, object string, values []string) (err error) {
	ipdisp.mutex.Lock()
	defer ipdisp.mutex.Unlock()
	vhost, ok := ipdisp.vhosts[host]
	if ok != true {
		err = errors.New("Not found " + host)
		return
	}
	err = errors.New("Value is invalid")
	switch object {
	case "node":
		for _, v := range values {
			items := strings.Split(v, ":")
			if len(items) != 3 {
				return
			}
			// node key value
			nid := vhost.nodeID[items[0]]
			node := vhost.nodes[nid]
			switch items[1] {
			case "bw":
				bw, ok := strconv.Atoi(items[2])
				if ok != nil {
					return
				}
				node.bw = bw
			case "status":
				status, ok := serverstat[items[2]]
				if ok == false {
					return
				}
				node.status = status
			}
		}
	case "server":
		for _, v := range values {
			items := strings.Split(v, ":")
			if len(items) != 4 {
				return
			}
			nid := vhost.nodeID[items[0]]
			node := vhost.nodes[nid]
			sid := node.serverID[items[1]]
			svr := node.servers[sid]
			switch items[2] {
			case "weight":
				svr.weightstr = items[3]
			case "status":
				status, ok := serverstat[items[3]]
				if ok == false {
					return
				}
				svr.status = status
			}
		}
	}

	return nil
}

//InetNetwork 将字符串形式的IP转换为无符号整数
func InetNetwork(ipstr string) (ipuint uint32) {
	ipuint = 0
	ip := net.ParseIP(ipstr)
	if ip == nil {
		return
	}
	ipuint += uint32(ip[12]) << 24
	ipuint += uint32(ip[13]) << 16
	ipuint += uint32(ip[14]) << 8
	ipuint += uint32(ip[15])
	return
}

//Comparator 在红黑树中，用于选择节点的比较方法
func Comparator(a, b interface{}) int {
	switch b.(type) {
	case Zone:
		bZone := b.(Zone)
		switch a.(type) {
		case uint32:
			aIP := a.(uint32)
			switch {
			case aIP < bZone.ipmin:
				return -1
			case aIP > bZone.ipmax:
				return 1
			default:
				return 0
			}
		case Zone:
			aZone := a.(Zone)
			switch {
			case aZone.ipmin > bZone.ipmin:
				return 1
			case aZone.ipmin < bZone.ipmin:
				return -1
			default:
				return 0
			}
		default:
			return 0
		}
	case ServerWeight:
		bb := b.(ServerWeight)
		switch a.(type) {
		case uint32:
			aa := a.(uint32)
			switch {
			case aa < bb.keymin:
				return -1
			case aa > bb.keymax:
				return 1
			default:
				return 0
			}
		case ServerWeight:
			aa := a.(ServerWeight)
			switch {
			case aa.keymin > bb.keymin:
				return 1
			case aa.keymin < bb.keymin:
				return -1
			default:
				return 0
			}
		default:
			return 0
		}
	default:
		return 0
	}
}

//file2string 将文件读取到字符串数组
func file2string(fname string) (flines []string, err error) {
	cf, err := os.Open(fname)
	if err != nil {
		return
	}
	defer cf.Close()
	r := bufio.NewReader(cf)
	for {
		buf, err := r.ReadSlice('\n')
		if err == io.EOF {
			err = nil
			break
		}
		flines = append(flines, string(buf[0:len(buf)-1]))
	}
	return
}

//LoadNode 加载每个host的node配置
func (ipdisp *IPDisp) LoadNode(conf string, vhostname string) (err error) {
	var flines []string
	flines, err = file2string(conf)
	if err != nil {
		return
	}

	ipdisp.vhosts[vhostname] = &Vhost{}
	vhost := ipdisp.vhosts[vhostname]
	vhost.nodeID = make(map[string]int)
	vhost.defaultNode = 0
	vhost.reqcount = 0
	nodeid := -1
	var cnode *Node
	for _, fline := range flines {
		flen := len(fline)
		if flen < 3 || fline[0] == '#' {
			continue
		}
		if fline[0] == '[' && fline[flen-1] == ']' {
			//获取节点名称，初始化节点配置
			//if nodeID > -1 {
			//	cnode.servers[cnode.servercount-1].next = cnode.servers[0]
			//}
			nodename := string(fline[1 : flen-1])
			cnode = &Node{}
			cnode.name = nodename
			cnode.status = 0
			cnode.servercount = 0
			cnode.balance = 'r'
			cnode.id = nodeid + 1
			cnode.reqmin = 0
			cnode.reqlastmin = 0
			cnode.reqcount = 0
			cnode.freebw = 20
			cnode.sw = make([]int, swMAX)
			cnode.serverID = make(map[string]int)
			cnode.swtree = rbtree.NewWith(Comparator)
			vhost.nodes = append(vhost.nodes, cnode)
			nodeid++
			vhost.nodeID[nodename] = nodeid
		} else {
			if nodeid == -1 {
				continue
			}
			cfline := string(fline)
			cf := strings.Split(cfline, "=")
			if len(cf) != 2 {
				continue
			}
			switch cf[0] {
			case "server":
				server := &Server{}
				server.status = 0
				server.weight = 0
				server.id = 0
				sinfo := strings.Split(cf[1], " ")
				server.ip = sinfo[0]
				sinfolen := len(sinfo)
				switch sinfolen {
				case 2:
					server.id, err = strconv.Atoi(sinfo[1])
				case 3:
					server.id, err = strconv.Atoi(sinfo[1])
					server.weightstr = sinfo[2]
				case 4:
					server.id, err = strconv.Atoi(sinfo[1])
					server.weightstr = sinfo[2]
					server.status = serverstat[sinfo[3]]
				}
				/*
					swcount = swcount + server.weight
				*/
				cnode.servers = append(cnode.servers, server)
				if cnode.servercount == 0 {
					cnode.curserver = server
				} else {
					cnode.servers[cnode.servercount-1].next = server
				}
				cnode.serverID[server.ip] = server.id
				cnode.servercount++
			case "bw":
				cnode.bw, err = strconv.Atoi(cf[1])
			case "maxbw":
				cnode.maxbw, err = strconv.Atoi(cf[1])
			case "freebw":
				cnode.freebw, err = strconv.Atoi(cf[1])
			case "overflow2node":
				cnode.overflow2node = cf[1]
			case "status":
				cnode.status = serverstat[cf[1]]
			case "default":
				vhost.defaultNode = nodeid
			case "balance":
				switch cf[1][0] {
				case 'h':
					cnode.balance = cf[1][0]
				case 'r':
					cnode.balance = cf[1][0]
				case 'w':
					cnode.balance = cf[1][0]
				case 'A':
					cnode.balance = cf[1][0]
				case 'a':
					cnode.balance = cf[1][0]
				default:
					err = errors.New(cnode.name + ": balance config is invalid")
					return
				}
			}
		}
	}
	for _, node := range vhost.nodes {
		var ok bool
		node.overflow2nodeid, ok = vhost.nodeID[node.overflow2node]
		if ok == false {
			node.overflow2nodeid = -1
		}
		//使server数组中，server.next首尾相接
		node.servers[node.servercount-1].next = node.servers[0]
		//添加特定均衡方式：o(only one)，代表只有一个server
		if len(node.servers) == 1 {
			node.balance = 'o'
		}
		err = node.initbalance()
	}

	return
}

//initbalance 根据服务器配置信息，计算权重分配方式
func (node *Node) initbalance() (err error) {
	switch node.balance {
	case 'h':
		k := 0
		swcount := 0
		swarray := make([]float64, swMAX)
		swmap := make(map[uint32]int)
		for id, svr := range node.servers {
			for _, swrange := range strings.Split(svr.weightstr, ",") {
				swr := strings.Split(swrange, "-")
				swrLen := len(swr)
				swmin := k + 1
				swmax, _ := strconv.Atoi(swr[0])
				swmax = swmax*100 + k
				if swrLen == 2 {
					swmin = swmax
					swmax, _ = strconv.Atoi(swr[1])
					swmax = swmax * 100
				}
				for i := swmin; i <= swmax; i++ {
					hash := Chash(uint32((id+1)*256*32 + (i+1)*563217))
					if swcount > swMAX {
						fmt.Fprintf(os.Stderr, node.name+": the total weight too max.")
						break
					}
					svr.weight++
					swarray[i-1] = float64(hash)
					swmap[hash] = id
					swcount++
				}
				k = swmax
			}
		}
		if swcount != swMAX {
			err = errors.New("Total weight is not swmax: " + string(swcount))
			return
		}
		sort.Float64s(swarray)
		var kk uint32
		kk = 0
		for _, v := range swarray {
			fv := uint32(v)
			swnode := ServerWeight{}
			swnode.keymin = kk
			swnode.keymax = fv
			sid := swmap[fv]
			swnode.server = node.servers[sid]
			//fmt.Printf("Hash: %v %v %v\n", kk, fv, swnode.server.ip)
			node.swtree.Put(swnode)
			kk = fv + 1
		}

	case 'A':

		for id, svr := range node.servers {
			w, _ := strconv.Atoi(svr.weightstr)
			svr.weight = w * 100
			for i := 0; i < svr.weight; i++ {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				rn := r.Intn(swMAX)
				for node.sw[rn] > 0 {
					rn = r.Intn(swMAX)
				}
				node.sw[rn] = id
			}
		}
	case 'a':
		swnum := make([]int, node.servercount)
		for i := 0; i < node.servercount; i++ {
			swnum[i] = 0
		}
		tt := int(swMAX / node.servercount)
		gg := int(tt / 3)
		for i := 0; i < swMAX; i++ {
			sgn := int(i / (node.servercount * 3))
			sg := i % (node.servercount * 3)
			sid := int(sg / 3)
			ssw := node.servers[sid].weight
			if ssw <= swnum[sid] || swnum[sid] >= tt {
				continue
			}
			swnum[sid]++
			node.sw[i] = sid
			if ssw < tt {
				cc := tt - ssw
				switch {
				case (gg-cc < sgn || gg-cc < 0) && sg%3 == 1:
					if swnum[sid] > ssw {
						swnum[sid]--
					}
				case (gg < cc && cc <= gg*2) && gg*2-cc < sgn && sg%3 == 2:
					if swnum[sid] > ssw {
						swnum[sid]--
					}
				case cc > gg*2 && (tt-cc) < sgn && sg%3 == 0:
					if swnum[sid] > ssw {
						swnum[sid]--
					}
				}
			}
		}
		for i := 0; i < swMAX; i++ {
			sg := i % (node.servercount * 3)
			sgn := int(i / (node.servercount * 3))
			sid := int(sg / 3)
			ssw := node.servers[sid].weight
			if ssw < tt {
				cc := tt - ssw

				switch {
				case (gg-cc <= sgn || gg-cc < 0) && sg%3 == 1:
					goto BLSW
				case (gg < cc && cc <= gg*2) && gg*2-cc <= sgn && sg%3 == 2:
					goto BLSW
				case cc > gg*2 && (tt-cc) <= sg && sg%3 == 0:
					goto BLSW
				default:
					continue
				}
			BLSW:
				for {
					if node.curserver.weight > tt && node.curserver.weight > swnum[node.curserver.id] {
						sid = node.curserver.id
						node.curserver = node.curserver.next
						break
					}
					node.curserver = node.curserver.next
				}
				node.sw[i] = sid
				swnum[sid]++
			}
		}
	}
	return nil
}

//LoadView 加载每个host的view配置
func (ipdisp *IPDisp) LoadView(conf string, vhostname string) (err error) {
	var flines []string
	flines, err = file2string(conf)
	if err != nil {
		return
	}
	vhost := ipdisp.vhosts[vhostname]
	for _, fline := range flines {
		sline := strings.Split(fline, ";")
		if len(sline) != 2 {
			continue
		}
		v1, ok1 := ipdisp.zoneID[sline[0]]
		v2, ok2 := vhost.nodeID[sline[1]]
		if ok1 {
			if ok2 {
				vhost.zone2node[v1] = v2
			} else {
				err = errors.New(conf + " not valid node: " + sline[1])
			}
		} else {
			err = errors.New(conf + " not valid zone: " + sline[0])
		}
	}
	return
}

//LoadZone 读取ip地址段，并保存到rbtree中
func (ipdisp *IPDisp) LoadZone(conf string) (err error) {
	ipdisp.rbtree = rbtree.NewWith(Comparator)
	var flines []string
	flines, err = file2string(conf)
	if err != nil {
		return
	}
	zoneids := ipdisp.zoneID
	ipdisp.zoneMax = 1
	for _, fline := range flines {
		zone := Zone{}
		ipinfo := strings.Split(fline, ";")
		if len(ipinfo) != 2 {
			continue
		}
		zone.name = ipinfo[1]
		ips := strings.Split(ipinfo[0], "/")
		if len(ips) != 2 {
			continue
		}
		m, err := strconv.Atoi(ips[1])
		if err != nil {
			continue
		}
		zone.ipmin = InetNetwork(ips[0])
		mask := (1 << (32 - uint32(m))) - 1
		zone.ipmax = zone.ipmin | uint32(mask)
		if v, ok := zoneids[zone.name]; ok == true {
			zone.id = v
		} else {
			zoneids[zone.name] = ipdisp.zoneMax
			zone.id = ipdisp.zoneMax
			ipdisp.zoneMax++
		}
		ipdisp.rbtree.Put(zone)
		//fmt.Printf("%v  %v  %v\n",ipzone.ipmin,ipzone.ipmax,ipzone.Zone)
	}
	return
}

//Init 读取配置文件，并加载到IPDisp
func (ipdisp *IPDisp) Init(cfpath string) (err error) {
	err = ipdisp.LoadZone(cfpath + "/ipz")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	var dirs []os.FileInfo
	dirs, err = ioutil.ReadDir(cfpath)
	if err != nil {
		return
	}
	for _, dir := range dirs {
		if dir.IsDir() {
			//fmt.Printf("%s/%s\n", cfpath, dir.Name())
			err = ipdisp.LoadNode(cfpath+"/"+dir.Name()+"/node.conf", dir.Name())
			if err != nil {
				return err
			}
			err = ipdisp.LoadView(cfpath+"/"+dir.Name()+"/view.conf", dir.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//Chash 一致性哈希算法
func Chash(a uint32) uint32 {
	a = (a + 0x7ed55d16) + (a << 12)
	a = (a ^ 0xc761c23c) ^ (a >> 19)
	a = (a + 0x165667b5) + (a << 5)
	a = (a + 0xd3a2646c) ^ (a << 9)
	a = (a + 0xfd7046c5) + (a << 3)
	a = (a ^ 0xb55a4f09) ^ (a >> 16)
	return a * 3221225474
}

//HashStr 将字符串转换为无符号整数
func HashStr(hashstr string) uint32 {
	strlen := len(hashstr)
	var hash uint32
	for i := 0; i < strlen; i++ {
		hash = uint32(hashstr[i]) + (hash << 6) + (hash << 16) - hash
	}
	return Chash(hash)
}

//QueryZone 查找IP所在的区域
func (ipdisp *IPDisp) QueryZone(clip string) string {
	ip := InetNetwork(clip)
	rbnode, ok := ipdisp.rbtree.Get(ip)
	if ok == true {
		ipz := rbnode.(Zone)
		return ipz.name
	}
	return ""
}

//Query 根据客户端IP，host，调度字符串（通常可以用url）计算调度目标
func (ipdisp *IPDisp) Query(clip string, host string, hashstr string) (string, string, error) {
	//fmt.Printf("IPDisp: %v\n", *ipdisp)
	ipdisp.reqcount++
	var err error
	vhost, ok := ipdisp.vhosts[host]
	if ok != true {
		ipdisp.othercount++
		err = errors.New("Not found " + host)
		return "", "", err
	}
	vhost.reqcount++
	var node *Node
	zonename := "None"
	nodeid := vhost.defaultNode
	ip := InetNetwork(clip)
	if ip == 0 {
		err = errors.New("Not valid ip: " + clip)
		return "", "", err
	}
	node = vhost.nodes[nodeid]
	//查找IP所属区域
	rbnode, ok := ipdisp.rbtree.Get(ip)
	if ok == true {
		ipz := rbnode.(Zone)
		zonename = ipz.name
		nodeid = vhost.zone2node[ipz.id]
		node = vhost.nodes[nodeid]
	}
	//如果节点的状态为down，以overflow节点替换，如果没有overflow节点，查找其他可用节点替换
	if node.status != 0 {
		if node.overflow2nodeid == -1 {
			for node.status != 0 {
				nid := node.id + 1
				node = vhost.nodes[nid]
			}
		} else {
			node = vhost.nodes[node.overflow2nodeid]
		}
	}
	//判断节点带宽使用，超过阈值，向overflow2node切流量
	if node.maxbw-node.bw >= node.freebw && node.reqmin > node.reqlastmin && node.overflow2nodeid >= 0 {
		node = vhost.nodes[node.overflow2nodeid]
	}
	curtime := time.Now().Unix()
	//unixtime := curtime.Unix()
	if curtime%60 == 0 {
		node.reqlastmin = node.reqmin
		node.reqmin = 0
	}
	node.reqcount++
	node.reqmin++
	var curserver *Server
	//根据节点负载均衡的方式，选择server。
	switch node.balance {
	case 'o':
		return node.curserver.ip, zonename, nil
	case 'a':
		sid := int(HashStr(hashstr)) % (swMAX - 1)
		curserver = node.servers[node.sw[sid]]
	case 'A':
		sid := int(HashStr(hashstr)) % (swMAX - 1)
		curserver = node.servers[node.sw[sid]]
	case 'h':
		//fmt.Printf("Hash: %v\n", HashStr(hashstr))
		rbnode, ok := node.swtree.Get(HashStr(hashstr))
		if ok == true {
			swnode := rbnode.(ServerWeight)
			curserver = swnode.server
		} else {
			curserver = getnextsvr(node)
		}
	case 'r':
		curserver = getnextsvr(node)
	}
	if curserver.status != 0 {
		curserver = getnextsvr(node)
	}
	return curserver.ip, zonename, nil
}

func getnextsvr(node *Node) *Server {
	curserver := node.curserver
	for curserver.status != 0 {
		curserver = curserver.next
	}
	node.curserver = curserver.next
	return curserver
}
