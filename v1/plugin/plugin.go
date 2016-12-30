/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin/rpc"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
)

var (
	pchan chan []byte
	app   *cli.App
)

func init() {
	app = cli.NewApp()
	app.Flags = []cli.Flag{
		cli.UintFlag{
			Name:  "port",
			Value: 8183,
		},
		cli.BoolFlag{
			Name: "service",
		},
	}
}

// Plugin is the base plugin type. All plugins must implement GetConfigPolicy.
type Plugin interface {
	GetConfigPolicy() (ConfigPolicy, error)
}

// Collector is a plugin which is the source of new data in the Snap pipeline.
type Collector interface {
	Plugin

	GetMetricTypes(Config) ([]Metric, error)
	CollectMetrics([]Metric) ([]Metric, error)
}

// Processor is a plugin which filters, agregates, or decorates data in the
// Snap pipeline.
type Processor interface {
	Plugin

	Process([]Metric, Config) ([]Metric, error)
}

// Publisher is a sink in the Snap pipeline.  It publishes data into another
// System, completing a Workflow path.
type Publisher interface {
	Plugin

	Publish([]Metric, Config) error
}

// startCollectoService
func startCollectorService(plugin Collector, name string, version int, port uint) int {
	exitCode := make(chan int)
	go func() {
		exitCode <- startCollector(plugin, name, version, Exclusive(true))
	}()
	pchan = make(chan []byte)
	preamble := <-pchan
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(preamble))
	})
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic("Unable to get open port")
	}
	defer listener.Close()
	stoppableListener, err := newListener(listener)
	http.Serve(stoppableListener, nil)
	return <-exitCode
}

type stoppableListener struct {
	*net.TCPListener
	haltChan chan struct{}
}

func newListener(l net.Listener) (*stoppableListener, error) {
	listener, ok := l.(*net.TCPListener)
	if !ok {
		return nil, errors.New("Error wrapping listener")
	}

	return &stoppableListener{
		TCPListener: listener,
		haltChan:    HaltChan,
	}, nil
}

func (sl *stoppableListener) Accept() (net.Conn, error) {
	for {
		// wait for up to a second for a new connection and then check
		// the killchan
		sl.SetDeadline(time.Now().Add(time.Second))
		newConn, err := sl.TCPListener.Accept()

		select {
		case <-sl.haltChan:
			return nil, errors.New("Halt detected")
		default:
			// loop until halt is detected
		}

		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		return newConn, err
	}
}

// StartCollector is given a Collector implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartCollector(plugin Collector, name string, version int, opts ...MetaOpt) int {
	result := 0
	app.Action = func(c *cli.Context) error {
		args = c.Args()
		if c.Bool("service") {
			fmt.Printf("start service on port %d", c.Uint("port"))
			result = startCollectorService(plugin,
				name,
				version,
				c.Uint("port"))
		} else {
			result = startCollector(plugin, name, version, opts...)
		}
		return nil
	}
	app.Run(os.Args)
	return result
}

func startCollector(plugin Collector, name string, version int, opts ...MetaOpt) int {
	getArgs()
	m := newMeta(collectorType, name, version, opts...)
	server := grpc.NewServer()
	// TODO(danielscottt) SSL
	proxy := &collectorProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterCollectorServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

// StartProcessor is given a Processor implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartProcessor(plugin Processor, name string, version int, opts ...MetaOpt) int {
	getArgs()
	m := newMeta(processorType, name, version, opts...)
	server := grpc.NewServer()
	// TODO(danielscottt) SSL
	proxy := &processorProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterProcessorServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

// StartPublisher is given a Publisher implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartPublisher(plugin Publisher, name string, version int, opts ...MetaOpt) int {
	getArgs()
	m := newMeta(publisherType, name, version, opts...)
	server := grpc.NewServer()
	// TODO(danielscottt) SSL
	proxy := &publisherProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterPublisherServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

type server interface {
	Serve(net.Listener) error
}

type preamble struct {
	Meta          meta
	ListenAddress string
	PprofAddress  string
	Type          pluginType
	State         int
	ErrorMessage  string
}

func startPlugin(srv server, m meta, p *pluginProxy) int {
	l, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		panic("Unable to get open port")
	}
	l.Close()

	addr := fmt.Sprintf("127.0.0.1:%v", l.Addr().(*net.TCPAddr).Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		// TODO(danielscottt): logging
		panic(err)
	}
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
	resp := preamble{
		Meta:          m,
		ListenAddress: addr,
		Type:          m.Type,
		PprofAddress:  pprofPort,
		State:         0, // Hardcode success since panics on err
	}
	preambleJSON, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	if pchan != nil {
		// The plugin is being started as a standalone service.
		// The preamble is provided to the http handler for plugin handshaking
		// over http
		pchan <- preambleJSON
	}
	fmt.Println(string(preambleJSON))
	go p.HeartbeatWatch()
	// TODO(danielscottt): exit code
	<-p.halt
	fmt.Println("startPlugin is returning")
	return 0
}
