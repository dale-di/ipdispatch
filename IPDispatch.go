package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dale-di/ipdispatch/ipzone"

	"github.com/facebookgo/grace/gracehttp"
)

type ipdAction struct {
	action string
	param  map[string]string
	result interface{}
}

var ipdisp *ipzone.IPDisp
var ipdActionCH = make(chan ipdAction, 1)
var ipdResultCH = make(chan ipdAction, 1)
var ipdCH = make(chan *ipzone.IPDisp, 1)

var actionLock sync.Mutex

const (
	//Version 版本号
	Version = "1.0"
	//SVer 版本信息
	SVer = "LPD/" + Version
)

var (
	conf     = flag.String("c", "no", "configure dir")
	pidfile  = flag.String("p", "/tmp/IPDispatch.pid", "the pidfile's path")
	username = flag.String("u", "root", "assume identity of <username>")
	ncpu     = flag.Int("n", 0, "number cpus")
	lport    = flag.String("l", ":8080", "Listen addr")
)

func main() {
	flag.Parse()
	var err error
	//fmt.Printf("start: %v\n", os.Getpid())
	if *ncpu != 0 {
		runtime.GOMAXPROCS(*ncpu)
	}
	if *conf == "no" {
		fmt.Printf("No configure dir")
		os.Exit(1)
	}
	var nullFile *os.File
	var userinfo *user.User
	var credential *syscall.Credential
	if nullFile, err = os.Open(os.DevNull); err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	if os.Getuid() == 0 {
		if userinfo, err = user.Lookup(*username); err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}

		credential = new(syscall.Credential)
		var i int
		i, _ = strconv.Atoi(userinfo.Uid)
		credential.Uid = uint32(i)
		i, _ = strconv.Atoi(userinfo.Gid)
		credential.Gid = uint32(i)
	}
	if err = Daemon(
		pidfile,
		[]*os.File{nullFile, os.Stdin, os.Stderr},
		credential,
	); err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	go func(ipdispch chan *ipzone.IPDisp, action chan ipdAction, result chan ipdAction) {
		var ipdispIns = ipzone.New()
		err = ipdispIns.Init(*conf)
		if err != nil {
			fmt.Printf("Init false: %v\n", err)
			os.Remove(*pidfile)
			os.Exit(1)
		}
		ipdispch <- ipdispIns
		for {
			select {
			case doAction := <-action:
				switch {
				case doAction.action == "get":
					pm := doAction.param
					doAction.result = ipdispIns.GetCount(pm["host"], pm["node"], pm["last"])
				case doAction.action == "query":
					pm := doAction.param
					ip, zone, _ := ipdispIns.Query(pm["clip"], pm["host"], pm["path"])
					toip := make(map[string]string)
					toip["ip"] = ip
					toip["zonename"] = zone
					doAction.result = toip
				case doAction.action == "set":
					pm := doAction.param
					vv := doAction.result.([]string)
					err := ipdispIns.Set(pm["host"], pm["object"], vv)
					doAction.result = false
					if err == nil {
						doAction.result = true
					}
				}
				result <- doAction
			}
		}
	}(ipdCH, ipdActionCH, ipdResultCH)
	select {
	case ipdisp = <-ipdCH:
		break
	case <-time.After(time.Duration(3) * time.Second):
		fmt.Printf("Init false.\n")
	}
	gracehttp.Serve(&http.Server{Addr: *lport,
		Handler:        ipDisp(),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 2048})

}

func ipDisp() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		actionLock.Lock()
		defer actionLock.Unlock()
		w.Header().Set("Server", SVer)
		clip := r.RemoteAddr
		speAddr := r.Header.Get("X-Addr")
		if speAddr != "" {
			clip = speAddr
		}
		qzone := r.Header.Get("X-Query-Zone")
		if qzone == "yes" {
			zonename := ipdisp.QueryZone(clip)
			w.Write([]byte(zonename))
		} else {
			//fmt.Printf("clip: %s\n", r.Host)
			ipdaction := ipdAction{}
			ipdaction.action = "query"
			p := make(map[string]string)
			p["clip"] = clip
			p["host"] = r.Host
			p["path"] = r.URL.Path
			ipdaction.param = p
			//ip, _, _ := ipdisp.Query(clip, r.Host, r.URL.Path)
			ipdActionCH <- ipdaction
			var ip string
			//var zonename string
			select {
			case ipdaction = <-ipdResultCH:
				result := ipdaction.result.(map[string]string)
				ip = result["ip"]
			}
			w.Header().Set("Location", "http://"+ip+r.URL.Path)
			w.WriteHeader(http.StatusFound)
		}
	})
	mux.HandleFunc("/ipdadmin/set", func(w http.ResponseWriter, r *http.Request) {
		actionLock.Lock()
		defer actionLock.Unlock()
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		queryparam := r.PostForm
		w.Header().Set("Server", SVer)
		ipdaction := ipdAction{}
		ipdaction.param = map[string]string{"host": "", "object": "", "value": ""}
		for k := range ipdaction.param {
			//fmt.Printf("pp: %s %s\n", k, q)
			v, ok := queryparam[k]
			if ok {
				if k == "value" {
					ipdaction.result = v
					continue
				}
				ipdaction.param[k] = v[0]
			}
		}
		ipdaction.action = "set"
		ipdActionCH <- ipdaction
		var rst bool
		select {
		case ipdaction = <-ipdResultCH:
			rst = ipdaction.result.(bool)
		}

		if rst == true {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/ipdadmin/get", func(w http.ResponseWriter, r *http.Request) {
		actionLock.Lock()
		defer actionLock.Unlock()
		w.Header().Set("Server", SVer)
		queryparam := r.URL.Query()
		ipdaction := ipdAction{}
		ipdaction.param = map[string]string{"host": "", "node": "", "last": ""}
		for k := range ipdaction.param {
			//fmt.Printf("pp: %s %s\n", k, q)
			v, ok := queryparam[k]
			if ok {
				ipdaction.param[k] = v[0]
			}
		}
		ipdaction.action = "get"
		ipdActionCH <- ipdaction
		var count uint64
		select {
		case ipdaction = <-ipdResultCH:
			count = ipdaction.result.(uint64)
		}
		w.Write([]byte(strconv.FormatUint(count, 32)))
	})
	return mux
}
