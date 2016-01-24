package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const (
	//DAEMONKEY 环境变量
	DAEMONKEY = "IPDispatch"
	// DAEMONVALUE 环境变量-值
	DAEMONVALUE = "1"
)

// Daemon 启动后台进程
func Daemon(pidfile *string, ifile []*os.File, credential *syscall.Credential) (err error) {
	if os.Getenv("LISTEN_FDS") != "" {
		pidFile(pidfile)
		return
	}
	if os.Getenv(DAEMONKEY) == DAEMONVALUE {
		pidFile(pidfile)
		return
	}
	var originalWD, _ = os.Getwd()
	attr := &os.ProcAttr{
		Dir:   originalWD,
		Env:   []string{DAEMONKEY + "=" + DAEMONVALUE},
		Files: ifile,
		Sys: &syscall.SysProcAttr{
			//Chroot:     d.Chroot,
			Credential: credential,
			//Setsid:     true,
		},
	}
	var abspath string
	var child *os.Process
	if abspath, err = filepath.Abs(os.Args[0]); err != nil {
		return
	}
	if child, err = os.StartProcess(abspath, os.Args, attr); err != nil {
		return
	}
	// ppid := os.Getppid()
	// if os.Getenv("LISTEN_FDS") != "" {
	// 	if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
	// 		return fmt.Errorf("failed to close parent: %s", err)
	// 	}
	// }
	if child != nil && os.Getenv("LISTEN_FDS") == "" {
		os.Exit(0)
	}
	return
}

func pidFile(pidfile *string) (err error) {
	if err = os.MkdirAll(filepath.Dir(*pidfile), os.FileMode(0755)); err != nil {
		return
	}
	var f *os.File
	if f, err = os.OpenFile(*pidfile, os.O_RDWR|os.O_CREATE, 0600); err != nil {
		return
	}
	_, err = fmt.Fprintf(f, "%d", os.Getpid()) //os.Getpid()
	if err != nil {
		return
	}
	err = f.Close()
	if err != nil {
		return
	}
	return
}
