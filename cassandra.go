package cassandra

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type Client struct {
	session *gocql.Session

	clusterConfig *gocql.ClusterConfig

	logger  Logger
	metrics Metrics
}

func New(conf Config) *Client {
	hosts := strings.Split(conf.Get("CASS_DB_HOST"), ",")
	clusterConfig := gocql.NewCluster(hosts...)
	clusterConfig.Keyspace = conf.Get("CASS_DB_KEYSPACE")
	clusterConfig.Port, _ = strconv.Atoi(conf.Get("CASS_DB_PORT"))
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: conf.Get("CASS_DB_USER"), Password: conf.Get("CASS_DB_PASS")}

	return &Client{clusterConfig: clusterConfig}
}

func (c *Client) Connect() {
	session, err := c.clusterConfig.CreateSession()
	if err != nil {
		c.logger.Errorf("error connecting to cassandra: ", err)

		return
	}

	hosts := strings.TrimSuffix(strings.Join(c.clusterConfig.Hosts, ", "), ", ")

	c.logger.Info("connected to '%s' keyspace at host '%s' and port '%s'", c.clusterConfig.Keyspace, hosts, c.clusterConfig.Port)

	c.session = session
}

func (c *Client) UseLogger(logger interface{}) {
	if l, ok := logger.(Logger); ok {
		c.logger = l
	}
}

func (c *Client) UseMetrics(metrics interface{}) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

func (c *Client) Query(dest interface{}, stmt string, values ...interface{}) error {
	rvo := reflect.ValueOf(dest)
	if rvo.Kind() != reflect.Ptr {
		c.logger.Error("we did not get a pointer. data is not settable.")

		return DestinationIsNotPointer{}
	}

	rv := rvo.Elem()
	iter := c.session.Query(stmt, values...).Iter()

	switch rv.Kind() {
	case reflect.Slice:
		numRows := iter.NumRows()

		for numRows > 0 {
			val := reflect.New(rv.Type().Elem())

			if rv.Type().Elem().Kind() == reflect.Struct {
				c.rowsToStruct(iter, val)
			} else {
				_ = iter.Scan(val.Interface())
			}

			rv = reflect.Append(rv, val.Elem())

			numRows--
		}

		if rvo.Elem().CanSet() {
			rvo.Elem().Set(rv)
		}

	case reflect.Struct:
		c.rowsToStruct(iter, rv)

	default:
		c.logger.Debugf("a pointer to %v was not expected.", rv.Kind().String())

		return UnexpectedPointer{target: rv.Kind().String()}
	}

	return nil
}

func (c *Client) Exec(stmt string, values ...interface{}) error {
	return c.session.Query(stmt, values...).Exec()
}

func (c *Client) QueryCAS(dest interface{}, stmt string, values ...interface{}) (bool, error) {
	var (
		applied bool
		err     error
	)

	rvo := reflect.ValueOf(dest)
	if rvo.Kind() != reflect.Ptr {
		c.logger.Error("we did not get a pointer. data is not settable.")

		return false, DestinationIsNotPointer{}
	}

	rv := rvo.Elem()
	query := c.session.Query(stmt, values...)

	switch rv.Kind() {
	case reflect.Struct:
		applied, err = c.rowsToStructCAS(query, rv)

	default:
		c.logger.Debugf("a pointer to %v was not expected.", rv.Kind().String())

		return false, UnexpectedPointer{target: rv.Kind().String()}
	}

	return applied, err
}

func (c *Client) rowsToStruct(iter *gocql.Iter, vo reflect.Value) {
	v := vo
	if vo.Kind() == reflect.Ptr {
		v = vo.Elem()
	}

	columns := c.getColumnsFromColumnsInfo(iter.Columns())
	fieldNameIndex := c.getFieldNameIndex(v)
	fields := c.getFields(columns, fieldNameIndex, v)

	_ = iter.Scan(fields...)

	if vo.CanSet() {
		vo.Set(v)
	}
}

func (c *Client) rowsToStructCAS(query *gocql.Query, vo reflect.Value) (bool, error) {
	v := vo
	if vo.Kind() == reflect.Ptr {
		v = vo.Elem()
	}

	row := make(map[string]interface{})

	applied, err := query.MapScanCAS(row)
	if err != nil {
		return false, err
	}

	fieldNameIndex := c.getFieldNameIndex(v)

	for col, value := range row {
		if i, ok := fieldNameIndex[col]; ok {
			field := v.Field(i)
			if reflect.TypeOf(value) == field.Type() {
				field.Set(reflect.ValueOf(value))
			}
		}
	}

	if vo.CanSet() {
		vo.Set(v)
	}

	return applied, nil
}

func (c *Client) getFields(columns []string, fieldNameIndex map[string]int, v reflect.Value) []interface{} {
	fields := make([]interface{}, 0)

	for _, column := range columns {
		if i, ok := fieldNameIndex[column]; ok {
			fields = append(fields, v.Field(i).Addr().Interface())
		} else {
			var i interface{}
			fields = append(fields, &i)
		}
	}

	return fields
}

func (c *Client) getFieldNameIndex(v reflect.Value) map[string]int {
	fieldNameIndex := map[string]int{}

	for i := 0; i < v.Type().NumField(); i++ {
		var name string

		f := v.Type().Field(i)
		tag := f.Tag.Get("db")

		if tag != "" {
			name = tag
		} else {
			name = toSnakeCase(f.Name)
		}

		fieldNameIndex[name] = i
	}

	return fieldNameIndex
}

func (c *Client) getColumnsFromColumnsInfo(columns []gocql.ColumnInfo) []string {
	cols := make([]string, 0)

	for _, column := range columns {
		cols = append(cols, column.Name)
	}

	return cols
}

func (c *Client) getColumnsFromMap(columns map[string]interface{}) []string {
	cols := make([]string, 0)

	for column := range columns {
		cols = append(cols, column)
	}

	return cols
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}
