package plugins

import (
	"fmt"
	"time"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

// Filter that counts the number of messages flowing through and provides
// primitive aggregation counts.
type LogCounterFilter struct {
	conf    *LogCounterFilterConfig
	count   uint
	msgPfx  string
	fr      FilterRunner
	pConfig *PipelineConfig
}

// LogCounterFilter config struct, used for specifying default ticker
// interval, message matcher values and influx related values.
type LogCounterFilterConfig struct {
	// Defaults to counting everything except the counter's own output
	// messages.
	MessageMatcher string `toml:"message_matcher"`
	// Defaults to 5 second intervals.
	TickerInterval uint `toml:"ticker_interval"`

	InfluxMSMT  string `toml:"influx_measurement"`
	InfluxVIP   string `toml:"influx_vip"`
	InfluxIP    string `toml:"influx_ip"`
	InfluxTopic string `toml:"influx_topic"`
	PayloadName string `toml:"payload_name"`
}

func (this *LogCounterFilter) ConfigStruct() interface{} {
	return &LogCounterFilterConfig{
		TickerInterval: uint(5),
		InfluxMSMT:     "",
		InfluxVIP:      "",
		InfluxIP:       "",
		InfluxTopic:    "",
		PayloadName:    "local_report",
	}
}

func (this *LogCounterFilter) Init(config interface{}) error {
	conf := config.(*LogCounterFilterConfig)
	if conf.InfluxMSMT == "" || conf.InfluxIP == "" || conf.InfluxTopic == "" {
		err := fmt.Errorf("influx_measurement influx_ip influx_topic can't be empty")
		return err
	}

	if conf.InfluxVIP != "" {
		this.msgPfx = fmt.Sprintf("%s,vip=%s,ip=%s,topic=%s", conf.InfluxMSMT,
			conf.InfluxVIP, conf.InfluxIP, conf.InfluxTopic)
	} else {
		this.msgPfx = fmt.Sprintf("%s,ip=%s,topic=%s", conf.InfluxMSMT,
			conf.InfluxIP, conf.InfluxTopic)
	}

	this.conf = conf
	return nil
}

func (this *LogCounterFilter) ProcessMessage(pack *PipelinePack) (err error) {
	this.count++
	return nil
}

func (this *LogCounterFilter) Prepare(fr FilterRunner, h PluginHelper) (err error) {
	this.pConfig = h.PipelineConfig()
	this.fr = fr

	return nil
}

func (this *LogCounterFilter) CleanupForRestart() {
	this.count = 0
	this.conf = nil
    this.msgPfx = ""
}

func (this *LogCounterFilter) CleanUp() {
	this.CleanupForRestart()
}

func (this *LogCounterFilter) TimerEvent() error {
	tStamp := time.Now().UnixNano()

	pack, err := this.pConfig.PipelinePack(0)
	if err != nil {
		this.fr.LogError(err)
		return err
	}
	pack.Message.SetType("txt")
	pack.Message.SetPayload(fmt.Sprintf("%s size=%d,value=1 %d\n", this.msgPfx, this.count, tStamp))
    field, err := message.NewField("payload_name", this.conf.PayloadName, "Payload name for match")
    if err != nil {
        return err
    }
    pack.Message.AddField(field)
	this.fr.Inject(pack)
	this.count = 0
	return nil
}

func init() {
	RegisterPlugin("LogCounterFilter", func() interface{} {
		return new(LogCounterFilter)
	})
}
