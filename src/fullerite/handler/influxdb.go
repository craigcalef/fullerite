// Straight up ripping the InfluxDB handler into an InfluxDB Line Format
package handler

import (
	"fmt"
	"fullerite/metric"
	"fullerite/util"
	"net"
	"sort"
	"time"

	l "github.com/Sirupsen/logrus"
)

func init() {
	RegisterHandler("InfluxDB", newInfluxDB)
}

// InfluxDB type
type InfluxDB struct {
	BaseHandler
	server string
	port   string
}

// allowedPunctation: taken here https://github.com/dropwizard/metrics/issues/637
//var allowedPunctuation = []rune{'!', '#', '$', '%', '&', '"', '*', '+', '-', ';', '<', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '|', '~'}

// newInfluxDB returns a new InfluxDB handler.
func newInfluxDB(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {

	inst := new(InfluxDB)
	inst.name = "InfluxDB"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Server returns the InfluxDB server's name or IP
func (g InfluxDB) Server() string {
	return g.server
}

// Port returns the InfluxDB server's port number
func (g InfluxDB) Port() string {
	return g.port
}

// Configure accepts the different configuration options for the InfluxDB handler
func (g *InfluxDB) Configure(configMap map[string]interface{}) {
	if server, exists := configMap["server"]; exists {
		g.server = server.(string)
	} else {
		g.log.Error("There was no server specified for the InfluxDB Handler, there won't be any emissions")
	}

	if port, exists := configMap["port"]; exists {
		g.port = fmt.Sprint(port)
	} else {
		g.log.Error("There was no port specified for the InfluxDB Handler, there won't be any emissions")
	}
	g.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (g *InfluxDB) Run() {
	g.run(g.emitMetrics)
}

func (g InfluxDB) convertToInfluxDB(incomingMetric metric.Metric) (datapoint string) {
	//orders dimensions so datapoint keeps consistent name
	var keys []string
	dimensions := g.getSanitizedDimensions(incomingMetric)
	for k := range dimensions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	datapoint = g.Prefix() + graphiteSanitize(incomingMetric.Name)
	for _, key := range keys {
		datapoint = fmt.Sprintf("%s,%s=%s", datapoint, key, dimensions[key])
	}
	datapoint = fmt.Sprintf("%s value=%f %d\n", datapoint, incomingMetric.Value, time.Now().Unix())
	return datapoint
}

func (g InfluxDB) getSanitizedDimensions(incomingMetric metric.Metric) map[string]string {
	dimSanitized := make(map[string]string)
	dimensions := incomingMetric.GetDimensions(g.DefaultDimensions())
	for key, value := range dimensions {
		dimSanitized[graphiteSanitize(key)] = graphiteSanitize(value)
	}
	return dimSanitized
}

func (g *InfluxDB) emitMetrics(metrics []metric.Metric) bool {
	g.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		g.log.Warn("Skipping send because of an empty payload")
		return false
	}

	addr := fmt.Sprintf("%s:%s", g.server, g.port)
	conn, err := net.DialTimeout("udp", addr, g.timeout)
	if err != nil {
		g.log.Error("Failed to connect ", addr)
		return false
	}

	for _, m := range metrics {
		fmt.Fprintf(conn, g.convertToInfluxDB(m))
	}
	return true
}

func influxSanitize(value string) string {
	return util.StrSanitize(value, false, allowedPunctuation)
}
