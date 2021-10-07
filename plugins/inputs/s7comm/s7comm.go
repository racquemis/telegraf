package s7comm

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/robinson/gos7"
	"time"
	"strconv"
	"strings"
)

const (
	//S7 Datatypes
	s7bool   = 0
	s7byte	 = 1
	s7word	 = 2
	s7dword  = 3
	s7int 	 = 4
	s7dint   = 5
	s7uint   = 6
	s7udint  = 7
	s7real   = 8
	
	//Areas
	s7areape = 0x81 //Input
	s7areapa = 0x82 //Output
	s7areamk = 0x83 //Merker
	s7areadb = 0x84 //Datablok
	
	MaxItemsPerReq = 18
)


// S7Comm
type S7Comm struct {
	MetricName string `toml:"name"`
	Endpoint   string `toml:"plc_ip"`
	Rack       int    `toml:"plc_rack"`
	Slot       int    `toml:"plc_slot"`

	Timeout     config.Duration `toml:"connect_timeout"`
	IdleTimeout config.Duration `toml:"request_timeout"`
	
	PollInterval config.Duration `toml:"poll_interval"`
	PollingDisabled bool `toml:"poll_interval_disabled"`
        TicksPerInterval int64
	TickCount int64
	
	Nodes []NodeSettings  `toml:"nodes"`
	Log   telegraf.Logger `toml:"-"`

	// internal values
	handler *gos7.TCPClientHandler
	client  gos7.Client
	helper  gos7.Helper
}

type NodeSettings struct {
	MetricName	string `toml:"metric"`
	Name    string `toml:"name"`
	Address string `toml:"address"`
	Type    string `toml:"type"`
}
type Metrics struct {
	Name string
	Count int
	Nodes []NodeSettings
}

func (s *S7Comm) Connect() error {
	s.handler = gos7.NewTCPClientHandler(s.Endpoint, s.Rack, s.Slot)
	s.handler.Timeout = time.Duration(s.Timeout)
	s.handler.IdleTimeout = time.Duration(s.IdleTimeout) //Must be equal or higher than PollingInterval set in main.go

	err := s.handler.Connect()
	if err != nil {
		return err
	}

	// defer s.handler.Close()

	s.client = gos7.NewClient(s.handler)

	//s.Log.Info("Connection successfull")

	return nil
}

func (s *S7Comm) Stop() error {
	err := s.handler.Close()
	return err
}

func (s *S7Comm) Init() error {
	if s.PollingDisabled {
		s.TicksPerInterval = 1
	
	} else{
		s.TicksPerInterval = int64(s.PollInterval) / int64(10 * time.Millisecond)
	}
	
	err := s.Connect()
	return err
}

func (s *S7Comm) SampleConfig() string {
	return `
  	## Generates random numbers
		[[inputs.s7comm]]
		# name = "S7300"
		# plc_ip = "192.168.10.57"
		# plc_rack = 1
		# plc_slot = 2
		# connect_timeout = 10s
		# request_timeout = 2s
		# nodes = [{name= "DB1.DBW0", type = "int"}, 
        # {name= "DB1.DBD2", type = "real"},
        # {name= "DB1.DBD6", type = "real"}, 
        # {name= "DB1.DBX10.0", type = "bool"}, 
        # {name= "DB1.DBD12", type = "dint"}, 
        # {name= "DB1.DBW16", type = "uint"}, 
        # {name= "DB1.DBD18", type = "udint"}, 
        # {name= "DB1.DBD22", type = "time"}]
`
}

func (s *S7Comm) Gather(a telegraf.Accumulator) error {
	if s.TickCount == s.TicksPerInterval{
		s.TickCount = 0
	} else{
		s.TickCount++
		return nil
	}
	
	var metrics []Metrics
	//Determine number of unique metrics
	for _, node := range s.Nodes {
		found := false
		out:
		for j,  Metric := range metrics {
			if node.MetricName == Metric.Name {
				found = true
				metrics[j].Count++
				metrics[j].Nodes = append(metrics[j].Nodes,node)
				
				break out
			}
		}
		if found == false {
			var Node []NodeSettings
			Node= append(Node,node)
			metrics = append(metrics,Metrics{Name: node.MetricName,Count: 1, Nodes: Node})
		}
	}
	
	for _,  metric := range metrics {
		var pos, offset, s7_size, s7_dbNo,s7_dbIndex, s7_area, s7_bit int
		buf2 := make([]byte, 72)
		var items []gos7.S7DataItem 
		var dtype []int
		var errmsg [18]string
		var fields = make(map[string]interface{})
		for _, node := range metric.Nodes {
			if node.MetricName == metric.Name {
				switch Area := node.Address[0:2]; Area {
					case "EB","IB","AB","QB","MB","EW","IW","AW","QW","MW","ED","ID","AD","QD","MD": //input byte
						ioIndex, _ := strconv.ParseInt(string(string(node.Address)[2:]), 10, 16)
						switch ioType := node.Address[0:1]; ioType {
							case "E","I":
								s7_area = s7areape
							case "A","Q":
								s7_area = s7areapa
						}
						switch ioSize := Area[1:2]; ioSize {
							case "B":
								s7_size = 1
							case "W":
								s7_size = 2
							case "D":
								s7_size = 4
						}
						s7_dbIndex, s7_dbNo = int(ioIndex), 0
					case "DB": //Data Block
						dbArray := strings.Split(node.Address, ".")
						if len(dbArray) < 2 {
							s.Log.Error("Db Area read variable should not be empty")
							continue
						}
						dbNo, _:= strconv.ParseInt(string(string(dbArray[0])[2:]), 10, 16)
						dbIndex, _ := strconv.ParseInt(string(string(dbArray[1])[3:]), 10, 16)
						dbType := string(dbArray[1])[0:3]

						s7_dbIndex, s7_dbNo, s7_area = int(dbIndex), int(dbNo), s7areadb
						switch dbType {
							case "DBB": //byte
								s7_size = 1
							case "DBX": //bit
								s7_size = 1
								dbBit, _ := strconv.ParseInt(string(string(dbArray[2])[0:]),10,16)
								if dbBit > 7 || dbBit < 0{
									s.Log.Error("Db Read Bit is invalid for address ",node.Address)
									continue
								}
								s7_bit = int(dbBit) << 4
							case "DBW": //word
								s7_size = 2
							case "DBD": //dword
								s7_size = 4
							default:
								s.Log.Error("Error when parsing dbtype for address ",node.Address)
								continue
						}
						
					default:
						switch otherArea := node.Address[0:1]; otherArea {
							case "I","E": //input
								s7_area = s7areape
							case "0","A": //output
								s7_area = s7areapa
							case "M": //memory
								s7_area = s7areamk 
							default:
								s.Log.Error("Error when parsing dbtype for address ",node.Address)
								continue
						}
						ioArray := strings.Split(node.Address, ".")
						ioIndex, _ := strconv.ParseInt(string(string(ioArray[0])[1:]), 10, 16)
						ioBit, _ := strconv.ParseInt(string(string(ioArray[1])[0:]),10,16)
						if ioBit > 7 || ioBit < 0{
							s.Log.Error("Db Read Bit is invalid for address ",node.Address)
							continue
						}
						s7_dbIndex, s7_dbNo, s7_size, s7_bit = int(ioIndex), 0, 1, int(ioBit) << 4
				}
				switch dataType := node.Type; dataType {
					case "bool":
						dtype = append(dtype,s7_bit)
					case "byte":
						dtype = append(dtype,s7byte)
					case "word":
						dtype = append(dtype,s7word)
					case "dword":
						dtype = append(dtype,s7dword)
					case "int":
						dtype = append(dtype,s7int)
					case "dint", "time":
						dtype = append(dtype,s7dint)
					case "uint":
						dtype = append(dtype,s7uint)
					case "udint":
						dtype = append(dtype,s7udint)
					case "real":
						dtype = append(dtype,s7real)
					default:
						s.Log.Error("Unknown datatype '", dataType, "' for ", node.Address)
						continue
				}
				items = append(items,gos7.S7DataItem{
					Area:     s7_area,
					WordLen:  0x02,
					DBNumber: s7_dbNo,
					Start:    s7_dbIndex,
					Amount:   s7_size,
					Data:     buf2[pos*4:],
					Error:    errmsg[pos]})
				pos++
			}
			if len(items) == MaxItemsPerReq || len(items)+offset == metric.Count{
				err := s.client.AGReadMulti(items, pos)
				if err != nil {
					s.Log.Error(err)
				} else {
					for i := 0; i < len(items); i++ {
						switch dataType := dtype[i] & 0x0F; dataType {
						case s7bool:
							var res byte
							mask:= []byte{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80}
							bit:= dtype[i] >> 4
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							var val bool = (res & mask[bit]) != 0
							fields[metric.Nodes[offset+i].Name] = val
						case s7byte:
							var res byte
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7word:
							var res uint16
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7dword:
							var res uint32
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7int:
							var res int16
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7dint:
							var res int32
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7uint:
							var res uint16
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7udint:
							var res uint32
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						case s7real:
							var res float32
							s.helper.GetValueAt(buf2[i*4:], 0, &res)
							fields[metric.Nodes[offset+i].Name] = res
						}
					}
				}
				offset, dtype, items = pos, nil, nil
				pos = 0
			}
		}
		if fields != nil {
			a.AddFields(metric.Name, fields, nil)
			dtype, items = nil, nil
			pos = 0
		}
	}
	return nil
}

func (s *S7Comm) Description() string {
	return "Read data from Siemens PLC using S7 protocol with S7Go"
}

// Add this plugin to telegraf
func init() {
	inputs.Add("s7comm", func() telegraf.Input {
		return &S7Comm{
			MetricName:  "s7comm",
			Endpoint:    "192.168.10.57",
			Rack:        0,
			Slot:        1,
			Timeout:     config.Duration(5 * time.Second),
			IdleTimeout: config.Duration(10 * time.Second),
			Nodes:       nil,
		}
	})
}
