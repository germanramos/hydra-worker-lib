package worker

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"time"

	// DEBUG
	"fmt"

	"github.com/innotech/hydra-worker-lib/vendors/github.com/BurntSushi/toml"
	zmq "github.com/innotech/hydra-worker-lib/vendors/github.com/pebbe/zmq4"
)

const (
	SIGNAL_READY      = "\001"
	SIGNAL_REQUEST    = "\002"
	SIGNAL_REPLY      = "\003"
	SIGNAL_HEARTBEAT  = "\004"
	SIGNAL_DISCONNECT = "\005"

	DEFAULT_PRIORITY_LEVEL     = 0 // Local worker
	DEFAULT_VERBOSE            = false
	DEFAULT_HEARTBEAT_INTERVAL = 2600 * time.Millisecond
	DEFAULT_HEARTBEAT_LIVENESS = 3
	DEFAULT_RECONNECT_INTERVAL = 2500 * time.Millisecond
)

// type LBWorker interface {
// 	Close()
// 	recv([][]byte) [][]byte
// 	Run(func([]interface{}, map[string][]string, map[string]interface{}) []interface{})
// }

type Worker struct {
	HydraServerAddr string `toml:"hydra_server_address"` // Hydra Load Balancer address
	context         *zmq.Context
	poller          *zmq.Poller
	PriorityLevel   int    `toml:"priority_level"`
	ServiceName     string `toml:"service_name"`
	Verbose         bool   `toml:"verbose"`
	socket          *zmq.Socket

	HeartbeatInterval time.Duration `toml:"heartbeat_interval"`
	heartbeatAt       time.Time
	Liveness          int `toml:"liveness"`
	livenessCounter   int
	ReconnectInterval time.Duration `toml:"reconnect_interval"`

	expectReply bool
	replyTo     []byte
}

func NewWorker(arguments []string) (worker *Worker, err error) {
	worker = new(Worker)

	worker.context, err = zmq.NewContext()
	if err != nil {
		err = errors.New("Creating context failed")
		return
	}
	worker.HeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL
	worker.PriorityLevel = DEFAULT_PRIORITY_LEVEL
	worker.Liveness = DEFAULT_HEARTBEAT_LIVENESS
	worker.ReconnectInterval = DEFAULT_RECONNECT_INTERVAL
	worker.Verbose = DEFAULT_VERBOSE

	if err = worker.Load(arguments); err != nil {
		err = errors.New("Loading configuration failed")
		return
	}

	// Validate worker configuration
	if !worker.isValid() {
		err = errors.New("Invalid configuration: you must set all required configuration options")
		return
	}

	err = worker.ConnectToBroker()

	runtime.SetFinalizer(worker, (*Worker).Close)

	return
}

func (w *Worker) Close() {
	if w.socket != nil {
		w.socket.Close()
		w.socket = nil
	}
	if w.context != nil {
		w.context.Term()
		w.context = nil
	}
	return
}

// Load configures hydra-worker, it can be loaded from both
// custom file or command line arguments and the values extracted from
// files they can be overriden with the command line arguments.
func (w *Worker) Load(arguments []string) error {
	var path string
	f := flag.NewFlagSet("hydra-worker", flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.StringVar(&path, "config", "", "path to config file")
	f.Parse(arguments[1:])

	if path != "" {
		// Load from config file specified in arguments.
		if err := w.loadConfigFile(path); err != nil {
			return err
		}
	}

	// Load from command line flags.
	if err := w.loadFlags(arguments); err != nil {
		return err
	}

	return nil
}

// LoadFile loads configuration from a file.
func (w *Worker) loadConfigFile(path string) error {
	_, err := toml.DecodeFile(path, &w)
	return err
}

// LoadFlags loads configuration from command line flags.
func (w *Worker) loadFlags(arguments []string) error {
	var ignoredString string

	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.StringVar(&w.HydraServerAddr, "hydra-server-addr", w.HydraServerAddr, "")
	f.DurationVar(&w.HeartbeatInterval, "heartbeat-interval", w.HeartbeatInterval, "")
	f.IntVar(&w.Liveness, "Liveness", w.Liveness, "")
	f.IntVar(&w.PriorityLevel, "priority-level", w.PriorityLevel, "")
	f.DurationVar(&w.ReconnectInterval, "reconnect-interval", w.ReconnectInterval, "")
	f.StringVar(&w.ServiceName, "service-name", w.ServiceName, "")
	f.BoolVar(&w.Verbose, "v", w.Verbose, "")
	f.BoolVar(&w.Verbose, "Verbose", w.Verbose, "")

	// BEGIN IGNORED FLAGS
	f.StringVar(&ignoredString, "config", "", "")
	// END IGNORED FLAGS

	return nil
}

func (w *Worker) isValid() bool {
	if w.HydraServerAddr == "" {
		return false
	}
	if w.ServiceName == "" {
		return false
	}
	return true
}

// reconnectToBroker connects worker to hydra load balancer server (broker)
func (w *Worker) ConnectToBroker() (err error) {
	if w.socket != nil {
		w.socket.Close()
		w.socket = nil
	}
	w.socket, err = w.context.NewSocket(zmq.DEALER)
	// TODO: Maybe  set linger
	// err = w.socket.SetLinger(0)
	err = w.socket.Connect(w.HydraServerAddr)
	if w.Verbose {
		log.Printf("Connecting to broker at %s...\n", w.HydraServerAddr)
	}
	w.poller = zmq.NewPoller()
	w.poller.Add(w.socket, zmq.POLLIN)

	//  Register worker with broker
	w.sendToBroker(SIGNAL_READY, []byte(w.ServiceName), [][]byte{[]byte(strconv.Itoa(w.PriorityLevel))})

	// If liveness hits zero, queue is considered disconnected
	w.livenessCounter = w.Liveness
	w.heartbeatAt = time.Now().Add(w.HeartbeatInterval)

	return
}

// sendToBroker dispatchs messages to hydra load balancer server (broker)
func (w *Worker) sendToBroker(command string, option []byte, msg [][]byte) (err error) {
	if len(option) > 0 {
		msg = append([][]byte{option}, msg...)
	}

	msg = append([][]byte{nil, []byte(command)}, msg...)
	if w.Verbose {
		log.Printf("Sending %X to broker\n", command)
	}
	_, err = w.socket.SendMessage(msg)
	return
}

// recv receives messages from hydra load balancer server (broker) and send the responses back
func (w *Worker) recv(reply [][]byte) (msg [][]byte) {
	//  Format and send the reply if we were provided one
	if len(reply) == 0 && w.expectReply {
		log.Fatal("No reply, expected")
	}

	if len(reply) > 0 {
		if len(w.replyTo) == 0 {
			log.Fatal("Error replyTo == \"\"")
		}
		reply = append([][]byte{w.replyTo, nil}, reply...)
		w.sendToBroker(SIGNAL_REPLY, nil, reply)
	}

	w.expectReply = true

	var err error
	for {
		var polled []zmq.Polled
		polled, err = w.poller.Poll(w.HeartbeatInterval)
		if err != nil {
			log.Fatal("Worker interrupted with error: ", err) //  Interrupted
		}

		if len(polled) > 0 {
			msg, err = w.socket.RecvMessageBytes(0)
			if err != nil {
				continue //  Interrupted
			}
			if w.Verbose {
				log.Printf("Received message from broker: %q\n", msg)
			}
			w.livenessCounter = w.Liveness

			if len(msg) < 2 {
				log.Fatal("Invalid message from broker") //  Interrupted
			}

			switch command := string(msg[1]); command {
			case SIGNAL_REQUEST:
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				w.replyTo = msg[2]
				msg = msg[4:7]
				return
			case SIGNAL_HEARTBEAT:
				// Do nothing for heartbeats
			case SIGNAL_DISCONNECT:
				w.ConnectToBroker()
			default:
				log.Println("Invalid input message %q\n", msg)
			}
		} else if w.livenessCounter--; w.livenessCounter <= 0 {
			if w.Verbose {
				log.Println("Disconnected from broker - retrying...")
			}
			time.Sleep(w.ReconnectInterval)
			w.ConnectToBroker()
		}

		//  Send HEARTBEAT if it's time
		if w.heartbeatAt.Before(time.Now()) {
			w.sendToBroker(SIGNAL_HEARTBEAT, nil, nil)
			w.heartbeatAt = time.Now().Add(w.HeartbeatInterval)
		}
	}

	return
}

// Run executes the worker permanently
func (w *Worker) Run(fn func([]interface{}, map[string][]string, map[string]interface{}) []interface{}) {
	for reply := [][]byte{}; ; {
		request := w.recv(reply)
		if len(request) < 3 {
			log.Printf("Bad request %q received from broker\n", request)
			break
		}
		log.Printf("Processing request: %q\n", request)
		var instances []interface{}
		if err := json.Unmarshal(request[0], &instances); err != nil {
			log.Fatalln("Bad message: invalid instances")
			// TODO: Set REPLY and return
		}

		var clientParams map[string][]string
		if err := json.Unmarshal(request[1], &clientParams); err != nil {
			log.Fatalln("Bad message: invalid client params")
			// TODO: Set REPLY and return
		}

		var args map[string]interface{}
		if err := json.Unmarshal(request[2], &args); err != nil {
			log.Fatalln("Bad message: invalid args")
			// TODO: Set REPLY and return
		}

		var processInstances func(levels []interface{}, ci *[]interface{}, iteration int) []interface{}
		processInstances = func(levels []interface{}, ci *[]interface{}, iteration int) []interface{} {
			levelIteration := 0
			for _, level := range levels {
				if level != nil {
					kind := reflect.TypeOf(level).Kind()
					if kind == reflect.Slice || kind == reflect.Array {
						o := make([]interface{}, 0)
						*ci = append(*ci, processInstances(level.([]interface{}), &o, levelIteration))
					} else {
						args["iteration"] = iteration
						t := fn(levels, clientParams, args)
						return t
					}
					levelIteration = levelIteration + 1
				}
			}
			return *ci
		}
		var tmpInstances []interface{}
		computedInstances := processInstances(instances, &tmpInstances, 0)
		log.Printf("Computed instances: %q\n", computedInstances)

		instancesResult, _ := json.Marshal(computedInstances)
		reply = [][]byte{instancesResult}
	}
}

// DEBUG: prints the message legibly
func Dump(msg [][]byte) {
	for _, part := range msg {
		isText := true
		fmt.Printf("[%03d] ", len(part))
		for _, char := range part {
			if char < 32 || char > 127 {
				isText = false
				break
			}
		}
		if isText {
			fmt.Printf("%s\n", part)
		} else {
			fmt.Printf("%X\n", part)
		}
	}
}
