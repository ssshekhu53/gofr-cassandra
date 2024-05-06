package cassandra

import (
	"errors"
	"strings"

	"github.com/gocql/gocql"
)

type Client struct {
	*gocql.Session

	logger  Logger
	metrics Metrics
}

func New(conf Config, logger Logger, metrics Metrics) *Client {
	hosts := strings.Split(conf.Get("CASSANDRA_HOSTS"), ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = conf.Get("CASSANDRA_KEYSPACE")
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Errorf("Error connecting to Cassandra:", err)

		return nil
	}

	return &Client{Session: session, logger: logger, metrics: metrics}
}

func (c *Client) Query(iter interface{}, stmt string, values ...interface{}) error {
	query := c.Session.Query(stmt, values...)

	switch val := iter.(type) {
	case *gocql.Iter:
		val = query.Iter()
		iter = val

	default:
		errors.New("invalid type")
	}

	return nil
}

func (c *Client) Exec(stmt string, values ...interface{}) error {
	return c.Session.Query(stmt, values...).Exec()
}

func (c *Client) Close() {
	c.Session.Close()
}
