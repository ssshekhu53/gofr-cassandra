package cassandra

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type Client struct {
	*gocql.Session

	logger  Logger
	metrics Metrics
}

func New(conf Config, logger Logger, metrics Metrics) *Client {
	hosts := strings.Split(conf.Get("CASS_DB_HOST"), ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = conf.Get("CASS_DB_KEYSPACE")
	cluster.Port, _ = strconv.Atoi(conf.Get("CASS_DB_PORT"))
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: conf.Get("CASS_DB_USER"), Password: conf.Get("CASS_DB_PASS")}

	session, err := cluster.CreateSession()
	if err != nil {
		logger.Errorf("error connecting to cassandra: ", err)

		return nil
	}

	return &Client{Session: session, logger: logger, metrics: metrics}
}

func (c *Client) Query(result interface{}, stmt string, values ...interface{}) error {
	var err error

	resultMap, err := c.Session.Query(stmt, values...).Iter().SliceMap()
	if err != nil {
		return err
	}

	err = c.parseData(resultMap, result)
	if err != nil {
		return err
	}

	return err
}

func (c *Client) QueryRow(result interface{}, stmt string, values ...interface{}) error {
	var err error

	resultMap := make(map[string]interface{})

	ok := c.Session.Query(stmt, values...).Iter().MapScan(resultMap)
	if !ok {
		return errors.New("unable to fetch result")
	}

	err = c.parseData(resultMap, result)
	if err != nil {
		return err
	}

	return err
}

func (c *Client) Exec(stmt string, values ...interface{}) error {
	return c.Session.Query(stmt, values...).Exec()
}

func (c *Client) parseData(resultMap interface{}, result interface{}) error {
	data, err := json.Marshal(resultMap)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &result)
	if err != nil {
		return err
	}

	return nil
}
