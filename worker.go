package worker

import (
	"encoding/json"
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

type lbWorker interface {
	close()
	recv([][]byte) [][]byte
	Run(func([]interface{}, map[string][]string, map[string]interface{}) []interface{})
}

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

func NewWorker(arguments []string) Worker {
	worker := new(Worker)

	var err error
	worker.context, err = zmq.NewContext()
	if err != nil {
		log.Fatal("Creating context failed")
	}
	worker.HeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL
	worker.PriorityLevel = DEFAULT_PRIORITY_LEVEL
	worker.Liveness = DEFAULT_HEARTBEAT_LIVENESS
	worker.ReconnectInterval = DEFAULT_RECONNECT_INTERVAL
	worker.Verbose = DEFAULT_VERBOSE

	if err := worker.Load(arguments); err != nil {
		log.Fatal("Loading configuration failed")
	}

	// Validate worker configuration
	if !worker.isValid() {
		log.Fatal("Invalid configuration: you must set all required configuration options")
	}

	worker.livenessCounter = worker.Liveness

	// worker.reconnectToBroker()
	worker.ConnectToBroker()

	runtime.SetFinalizer(worker, (*Worker).Close)

	return worker
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
	err = w.socket.Connect(w.broker)
	if w.verbose {
		log.Printf("I: connecting to broker at %s...\n", w.broker)
	}
	w.poller = zmq.NewPoller()
	w.poller.Add(w.socket, zmq.POLLIN)

	//  Register worker with broker
	w.sendToBroker(SIGNAL_READY, []byte(w.ServiceName), [][]byte{[]byte(strconv.Itoa(w.PriorityLevel))})

	// If liveness hits zero, queue is considered disconnected
	// TODO: Maybe
	// w.liveness = heartbeat_liveness
	w.heartbeatAt = time.Now().Add(w.HeartbeatInterval)
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

//  Send reply, if any, to broker and wait for next request.
func (mdwrk *Mdwrk) Recv(reply []string) (msg []string, err error) {
	//  Format and send the reply if we were provided one
	if len(reply) == 0 && mdwrk.expect_reply {
		panic("No reply, expected")
	}
	if len(reply) > 0 {
		if mdwrk.reply_to == "" {
			panic("mdwrk.reply_to == \"\"")
		}
		m := make([]string, 2, 2+len(reply))
		m = append(m, reply...)
		m[0] = mdwrk.reply_to
		m[1] = ""
		err = mdwrk.SendToBroker(MDPW_REPLY, "", m)
	}
	mdwrk.expect_reply = true

	for {
		var polled []zmq.Polled
		polled, err = mdwrk.poller.Poll(mdwrk.heartbeat)
		if err != nil {
			break //  Interrupted
		}

		if len(polled) > 0 {
			msg, err = mdwrk.worker.RecvMessage(0)
			if err != nil {
				break //  Interrupted
			}
			if mdwrk.verbose {
				log.Printf("I: received message from broker: %q\n", msg)
			}
			mdwrk.liveness = heartbeat_liveness

			//  Don't try to handle errors, just assert noisily
			if len(msg) < 3 {
				panic("len(msg) < 3")
			}

			if msg[0] != "" {
				panic("msg[0] != \"\"")
			}

			if msg[1] != MDPW_WORKER {
				panic("msg[1] != MDPW_WORKER")
			}

			command := msg[2]
			msg = msg[3:]
			switch command {
			case MDPW_REQUEST:
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				mdwrk.reply_to, msg = unwrap(msg)
				//  Here is where we actually have a message to process; we
				//  return it to the caller application:
				return //  We have a request to process
			case MDPW_HEARTBEAT:
				//  Do nothing for heartbeats
			case MDPW_DISCONNECT:
				mdwrk.ConnectToBroker()
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		} else {
			mdwrk.liveness--
			if mdwrk.liveness == 0 {
				if mdwrk.verbose {
					log.Println("W: disconnected from broker - retrying...")
				}
				time.Sleep(mdwrk.reconnect)
				mdwrk.ConnectToBroker()
			}
		}
		//  Send HEARTBEAT if it's time
		if time.Now().After(mdwrk.heartbeat_at) {
			mdwrk.SendToBroker(MDPW_HEARTBEAT, "", []string{})
			mdwrk.heartbeat_at = time.Now().Add(mdwrk.heartbeat)
		}
	}
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

	self.expectReply = true

	for {
		var polled []zmq.Polled
		polled, err = w.poller.Poll(w.HeartbeatInterval)
		if err != nil {
			log.Fatal("Worker interrupted with error: ", err) //  Interrupted
		}

		if len(polled) > 0 {
			msg, err = w.socket.RecvMessage(0)
			if err != nil {
				continue //  Interrupted
			}
			if mdwrk.verbose {
				log.Printf("Received message from broker: %q\n", msg)
			}
			// TODO: review
			w.livenessCounter = w.Liveness

			if len(msg) < 2 {
				log.Fatal("Invalid message from broker") //  Interrupted
			}

			command := msg[2]
			msg = msg[3:]
			switch command {
			case MDPW_REQUEST:
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				mdwrk.reply_to, msg = unwrap(msg)
				//  Here is where we actually have a message to process; we
				//  return it to the caller application:
				return //  We have a request to process
			case MDPW_HEARTBEAT:
				//  Do nothing for heartbeats
			case MDPW_DISCONNECT:
				mdwrk.ConnectToBroker()
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		}

		/////////////////////////////////////////

		items := zmq.PollItems{
			zmq.PollItem{Socket: self.socket, Events: zmq.POLLIN},
		}

		_, err := zmq.Poll(items, self.HeartbeatInterval)
		if err != nil {
			panic(err) //  Interrupted
		}

		if item := items[0]; item.REvents&zmq.POLLIN != 0 {
			msg, _ = self.socket.RecvMultipart(0)
			if self.Verbose {
				log.Println("Received message from broker")
			}
			self.livenessCounter = self.Liveness
			if len(msg) < 2 {
				panic("Invalid msg") //  Interrupted
			}

			switch command := string(msg[1]); command {
			case SIGNAL_REQUEST:
				// log.Println("SIGNAL_REQUEST")
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				self.replyTo = msg[2]
				msg = msg[4:6]
				return
			case SIGNAL_HEARTBEAT:
				// log.Println("SIGNAL_HEARTBEAT")
				// do nothin
			case SIGNAL_DISCONNECT:
				// log.Println("SIGNAL_DISCONNECT")
				self.reconnectToBroker()
			default:
				// TODO: catch error
				log.Println("Invalid input message")
			}
		} else if self.livenessCounter--; self.livenessCounter <= 0 {
			if self.Verbose {
				log.Println("Disconnected from broker - retrying...")
			}
			time.Sleep(self.ReconnectInterval)
			self.reconnectToBroker()
		}

		//  Send HEARTBEAT if it's time
		if self.heartbeatAt.Before(time.Now()) {
			self.sendToBroker(SIGNAL_HEARTBEAT, nil, nil)
			self.heartbeatAt = time.Now().Add(self.HeartbeatInterval)
		}
	}

	return
}

// Run executes the worker permanently
func (self *lbWorker) Run(fn func([]interface{}, map[string][]string, map[string]interface{}) []interface{}) {
	for reply := [][]byte{}; ; {
		request := self.recv(reply)
		if len(request) == 0 {
			break
		}
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
