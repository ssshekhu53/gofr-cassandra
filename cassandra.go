package cassandra

import (
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
		logger.Errorf("error connecting to cassandra: ", err)

		return nil
	}

	return &Client{Session: session, logger: logger, metrics: metrics}
}

func (c *Client) Query(stmt string, values ...interface{}) *gocql.Query {
	return c.Session.Query(stmt, values...)
}

func (c *Client) Iter(stmt string, values ...interface{}) *gocql.Iter {
	return c.Session.Query(stmt, values...).Iter()
}

func (c *Client) Exec(stmt string, values ...interface{}) error {
	return c.Session.Query(stmt, values...).Exec()
}
