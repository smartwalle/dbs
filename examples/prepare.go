package main

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"log"
)

type Customer struct {
	Id       int    `sql:"id"`
	Name     string `sql:"name"`
	Birthday string `sql:"birthday"`
	Mobile   string `sql:"mobile"`
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	db, err := dbs.Open("postgres", "host=192.168.1.99 port=5432 user=postgres password=postgres dbname=mydb sslmode=disable", 10, 1)
	if err != nil {
		log.Fatalln("连接数据库出错：", err)
		return
	}

	dbs.UsePlaceholder(dbs.DollarPlaceholder)

	var ndb = dbs.New(db)
	defer ndb.Close()

	// 创建表
	_, err = ndb.Exec(`CREATE TABLE IF NOT EXISTS customers (
	id SERIAL,
	name VARCHAR(24),
	birthday DATE,
	mobile VARCHAR(11),
	CONSTRAINT customers_id PRIMARY KEY (id)
)`)
	if err != nil {
		log.Fatalln("创建表发生错误：", err)
		return
	}

	// 添加预处理语句
	if err = ndb.PrepareStatement(context.Background(), "insert_one_customer", `INSERT INTO customers (name, birthday, mobile) VALUES($1, $2, $3) RETURNING id`); err != nil {
		log.Fatalln("创建预处理语句发生错误：", err)
		return
	}
	if err = ndb.PrepareStatement(context.Background(), "get_one_customer_with_id", dbs.GetPlaceholder().Replace(`SELECT id, name, birthday, mobile FROM customers WHERE id = ? LIMIT 1`)); err != nil {
		log.Fatalln("创建预处理语句发生错误：", err)
		return
	}

	// 添加数据一
	if _, err = ndb.ExecContext(context.Background(), "insert_one_customer", "c1", "2001-01-01", "12345678901"); err != nil {
		log.Fatalln("添加数据发生错误：", err)
		return
	}

	// 添加数据二：有返回 id 字段
	var nId int
	if err = ndb.QueryRowContext(context.Background(), "insert_one_customer", "c2", "2001-01-02", "12345678902").Scan(&nId); err != nil {
		log.Fatalln("添加数据发生错误：", err)
		return
	}

	// 添加数据三：有返回 id 字段
	customer1, err := dbs.Query[Customer](context.Background(), ndb, "insert_one_customer", "c3", "2001-01-03", "12345678903")
	if err != nil {
		log.Fatalln("添加数据发生错误：", err)
		return
	}

	// 查询数据一
	rows, err := ndb.QueryContext(context.Background(), "get_one_customer_with_id", nId)
	if err != nil {
		log.Fatalln("查询数据发生错误：", err)
		return
	}
	customer2, err := dbs.Scan[Customer](rows)
	rows.Close()

	if err != nil {
		log.Fatalln("映射数据发生错误：", err)
		return
	}
	if customer2.Id != nId {
		log.Fatalln("获取的数据不匹配：", customer2.Id, nId)
		return
	}

	// 查询数据二
	customer3, err := dbs.Query[Customer](context.Background(), ndb, "get_one_customer_with_id", customer1.Id)
	if err != nil {
		log.Fatalln("映射数据发生错误：", err)
		return
	}
	if customer3.Id != customer1.Id {
		log.Fatalln("获取的数据不匹配：", customer3.Id, customer1.Id)
		return
	}
}
