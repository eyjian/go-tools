// Writed by yijian on 2018/9/4
// 通用的根据自增ID增量导MySQL数据工具
// 使用前提：表有自增ID字段，通过参数“-incrfield”指定自增ID字段名，如：-incrfield=f_id
//
// 正常的数据以标准输出方式，异常信息以标准出错方式，所以可以通过“2>/dev/null”过滤掉异常的数据，只保留正常被导出的数据。
//
// 关键参数说明：
// 1) 当表比较大时，指定参数“-incr”的值接近需要导的数据，否则第一次查询可能很慢，“-incr”的值即为自增ID字段的值。
// 2) “-fields”为需要导出的字段列表
// 3) “-incr”为自增ID字段的初始值，从指示从这个位置开始导出数据
// 4) “-cond”可通过这个参数指定导出条件
// 5) “-delmiter”这个参数值决定值之间的分隔符，默认为“\t”
// 6) “-batch”决定每次从DB导出多少条记录，一般“-batch”可取值10000，这样十万数据只需要查询10次即可导完。
//
// 就MySQL而言，如果引擎是MyISAM，则SELECT操作会锁表，而如果是INNODB则不会锁表。
package main

// 编译方法：
// go build -o mysql_dump mysql_dump.go
// 上述编译会依赖glibc，如果不想依赖，这样编译：
// go build -o mysql_dump -ldflags '-linkmode "external" -extldflags "-static"' mysql_dump.go

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strconv"
)

// SELECT fields FROM table_name WHERE condition
// SELECT fields FROM table_name WHERE condition LIMIT batch
// SELECT incrfield,fields FROM table_name WHERE incrfield>0 AND (condition) LIMIT batch
var (
	help = flag.Bool("H", false, "Display a help message and exit")

	dbip       = flag.String("h", "", "Connect to the MySQL server on the given host")
	dbport     = flag.Int("P", 3306, "The TCP/IP port number to use for the connection")
	dbuser     = flag.String("u", "", "The MySQL user name to use when connecting to the server")
	dbpassword = flag.String("p", "", "The password to use when connecting to the server")
	dbname     = flag.String("n", "", "The database to use")

	// 表名
	tablename = flag.String("t", "", "The table to dump")
	fields    = flag.String("fields", "", "All fields to dump separated by comma")
	condition = flag.String("cond", "", "The condition to dump")
	incrfield = flag.String("incrfield", "", "The AUTO_INCREMENT field")

	// 当表比较大时，请设置好incr值为第一笔符合条件的值，否则第一次查询可能很慢
	// 要求必须指定大于0的batch值，以防止长时间操作SELECT
	incr     = flag.Int64("incr", 0, "The inital value of AUTO_INCREMENT field")
	charset  = flag.String("charset", "latin1", "Use charset as the default character set for the client and connection")
	delmiter = flag.String("delmiter", "\t", "The delmiter of values")
	batch    = flag.Int("batch", 1, "The number of rows every query")
)

func getStartId(db *sql.DB, incrfield, tablename, condition string) int64 {
	sql_statement := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1", incrfield, tablename, condition, incrfield)
	rows, err := db.Query(sql_statement)
	if err != nil {
	}
	return 0
}

func main() {
	flag.Parse()

	// help
	if *help {
		flag.Usage()
		os.Exit(1)
	}

	// dbip
	if len(*dbip) == 0 {
		fmt.Fprintf(os.Stderr, "Parameter[-dbip] not set\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// dbuser
	if len(*dbuser) == 0 {
		fmt.Fprintf(os.Stderr, "Parameter[-dbuser] not set\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// dbname
	if len(*dbname) == 0 {
		fmt.Fprintf(os.Stderr, "Parameter[-dbname] not set\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// tablename
	if len(*tablename) == 0 {
		fmt.Fprintf(os.Stderr, "Parameter[-tablename] not set\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// fields
	if len(*fields) == 0 {
		fmt.Fprintf(os.Stderr, "Parameter[-fields] not set\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// batch
	if *batch < 1 {
		fmt.Fprintf(os.Stderr, "Parameter[-batch] <= 0: %d\n\n", *batch)
		os.Exit(1)
	}

	// MySQL连接字符串
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", *dbuser, *dbpassword, *dbip, *dbport, *dbname, *charset)
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	defer db.Close() // 作用相当于C++中的自动析构
	total := 0
	start := *incr

	if start == 0 {
		start = getStartId(db, *incrfield, *tablename, *condition)
	}

	// 设置不锁表
	_, err = db.Query("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	if err != nil {
		panic(err.Error())
		os.Exit(1)
	}

	// 和SET TRANSACTION对应
	defer func() {
		db.Query("COMMIT")
	}()

	for {
		var sql_statement string

		if len(*incrfield) == 0 {
			if len(*condition) == 0 {
				sql_statement = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", *fields, *tablename, *batch)
			} else {
				sql_statement = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT %d", *fields, *tablename, *condition, *batch)
			}
		} else {
			if len(*condition) == 0 {
				sql_statement = fmt.Sprintf("SELECT %s,%s FROM %s WHERE (%s>%d) ORDER BY %s LIMIT %d", *incrfield, *fields, *tablename, *incrfield, start, *incrfield, *batch)
			} else {
				sql_statement = fmt.Sprintf("SELECT %s,%s FROM %s WHERE (%s>%d) AND (%s) ORDER BY %s LIMIT %d", *incrfield, *fields, *tablename, *incrfield, start, *condition, *incrfield, *batch)
			}
		}

		fmt.Fprintf(os.Stderr, "%s\n", sql_statement)
		rows, err := db.Query(sql_statement)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			panic(err.Error())
			os.Exit(1)
		}

		// 所有列名
		//for i := range columns {
		//	fmt.Println(columns[i])
		//}

		// 创建用来存储各字段值的slice（变长数组）
		// Make a slice for the values
		values := make([]sql.RawBytes, len(columns))

		// rows.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		num_rows := 0
		for rows.Next() {
			// get RawBytes from data
			err = rows.Scan(scanArgs...)
			if err != nil {
				panic(err.Error())
				os.Exit(1)
			}

			var line string
			var value string
			for i, col := range values {
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}

				// 拼接成一行
				if 0 == i {
					line = value

					if len(*incrfield) > 0 {
						start, _ = strconv.ParseInt(value, 10, 64)
					}
				} else {
					line = line + *delmiter + value
				}
			}

			// 输出一行
			fmt.Println(line)
			num_rows++
			total++
		}

		if len(*incrfield) == 0 {
			break
		} else {
			if num_rows < *batch {
				break
			}
		}
	}

	fmt.Fprintf(os.Stderr, "%d\n", total)
}
