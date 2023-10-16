package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"log"
)

type Product struct {
	Id    int    `sql:"id"`
	Name  string `sql:"name"`
	Price int    `sql:"price"`
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	db, err := dbs.Open("postgres", "host=192.168.1.99 port=5432 user=postgres password=postgres dbname=mydb sslmode=disable", 10, 1)
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}

	dbs.UsePlaceholder(dbs.DollarPlaceholder)

	var ndb = dbs.New(db)
	defer ndb.Close()

	var rc = NewReadCommitted(ndb)
	rc.DirtyRead()
	rc.NonRepeatableRead()
	rc.PhantomRead()

	var rr = NewRepeatableRead(ndb)
	rr.DirtyRead()
	rr.NonRepeatableRead()
	rr.PhantomRead()

	var s = NewSerializable(ndb)
	s.DirtyRead()
	s.NonRepeatableRead()
	s.PhantomRead()
}

const (
	kTblProducts = "products"
)

// ReadCommitted 读已提交隔离级别
type ReadCommitted struct {
	db *dbs.DB
}

func NewReadCommitted(db *dbs.DB) *ReadCommitted {
	var rc = &ReadCommitted{}
	rc.db = db

	// 清理所有的数据
	var rb = dbs.NewDeleteBuilder()
	rb.Table(kTblProducts)
	rb.Where("1 = 1")
	rb.Exec(db)
	return rc
}

func getProduct(tx *dbs.Tx, name string) (*Product, error) {
	var sb = dbs.NewSelectBuilder()
	sb.From(kTblProducts)
	sb.Selects("id", "name", "price")
	sb.Where("name = ?", name)
	sb.Limit(1)
	var product *Product
	if err := sb.Scan(tx, &product); err != nil {
		return nil, err
	}
	return product, nil
}

func getProducts(tx *dbs.Tx) ([]*Product, error) {
	var sb = dbs.NewSelectBuilder()
	sb.From(kTblProducts)
	sb.Selects("id", "name", "price")
	var products []*Product
	if err := sb.Scan(tx, &products); err != nil {
		return nil, err
	}
	return products, nil
}

// DirtyRead 脏读
func (this *ReadCommitted) DirtyRead() {
	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.SET("name", "read_committed_dirty_read")
	ib.SET("price", 1)
	if _, err := ib.Exec(tx1); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		return
	}

	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	var product, err = getProduct(tx2, "read_committed_dirty_read")
	if err != nil && !errors.Is(err, dbs.ErrNoRows) {
		log.Println("读取数据发生错误:", err)
		tx2.Rollback()
		return
	}

	if product != nil {
		log.Println("隔离级别【读已提交(ReadCommitted)】不应该出现【脏读(DirtyRead)】，即事务 tx2 中不应该读取到未提交事务 tx1 写入的数据。")
	}

	tx1.Rollback()
	tx2.Commit()
}

// NonRepeatableRead 不可重复读取
func (this *ReadCommitted) NonRepeatableRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("read_committed_non_repeatable_read", 1)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})

	// 在事务 tx1 中读取数据
	product1, err := getProduct(tx1, "read_committed_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中修改数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	var ub = dbs.NewUpdateBuilder()
	ub.Table(kTblProducts)
	ub.SET("price", dbs.SQL("price + 1"))
	ub.Where("name = ?", "read_committed_non_repeatable_read")
	if _, err = ub.Exec(tx2); err != nil {
		log.Println("更新数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	product2, err := getProduct(tx1, "read_committed_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	if product1 == nil || product2 == nil {
		log.Println("读取数据异常")
		return
	}
	if product1.Price == product2.Price {
		log.Println("隔离级别【读已提交(ReadCommitted)】应该出现【不可重复读(NonRepeatableRead)】，即事务 tx1 两次读取同一个数据应该返回不同的结果。")
	}
}

// PhantomRead 幻读
func (this *ReadCommitted) PhantomRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("read_committed_phantom_read_1", 1)
	ib.Values("read_committed_phantom_read_2", 2)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})

	// 在事务 tx1 中读取数据
	products1, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中写入数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	var ib2 = dbs.NewInsertBuilder()
	ib2.Table(kTblProducts)
	ib2.Columns("name", "price")
	ib2.Values("read_committed_phantom_read_3", 3)
	if _, err := ib2.Exec(tx2); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	products2, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	var bs1, _ = json.Marshal(products1)
	var bs2, _ = json.Marshal(products2)
	if bytes.Compare(bs1, bs2) == 0 {
		log.Println("隔离级别【读已提交(ReadCommitted)】应该出现【幻读(PhantomRead)】，即事务 tx1 两次用同一条件读取数据应该返回不同结果。")
	}
}

// RepeatableRead 可重复读隔离级别
type RepeatableRead struct {
	db *dbs.DB
}

func NewRepeatableRead(db *dbs.DB) *RepeatableRead {
	var rc = &RepeatableRead{}
	rc.db = db

	// 清理所有的数据
	var rb = dbs.NewDeleteBuilder()
	rb.Table(kTblProducts)
	rb.Where("1 = 1")
	rb.Exec(db)
	return rc
}

func (this *RepeatableRead) DirtyRead() {
	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.SET("name", "repeatable_read_dirty_read")
	ib.SET("price", 1)
	if _, err := ib.Exec(tx1); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		return
	}

	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	var product, err = getProduct(tx2, "repeatable_read_dirty_read")
	if err != nil && !errors.Is(err, dbs.ErrNoRows) {
		log.Println("读取数据发生错误:", err)
		tx2.Rollback()
		return
	}

	if product != nil {
		log.Println("隔离级别【可重复读(RepeatableRead)】不应该出现【脏读(DirtyRead)】，即事务 tx2 中不应该读取到未提交事务 tx1 写入的数据。")
	}

	tx1.Rollback()
	tx2.Commit()
}

func (this *RepeatableRead) NonRepeatableRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("repeatable_read_non_repeatable_read", 1)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})

	// 在事务 tx1 中读取数据
	product1, err := getProduct(tx1, "repeatable_read_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中修改数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	var ub = dbs.NewUpdateBuilder()
	ub.Table(kTblProducts)
	ub.SET("price", dbs.SQL("price + 1"))
	ub.Where("name = ?", "repeatable_read_non_repeatable_read")
	if _, err = ub.Exec(tx2); err != nil {
		log.Println("更新数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	product2, err := getProduct(tx1, "repeatable_read_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	if product1 == nil || product2 == nil {
		log.Println("读取数据异常")
		return
	}
	if product1.Price != product2.Price {
		log.Println("隔离级别【可重复读(RepeatableRead)】不应该出现【不可重复读(NonRepeatableRead)】，即事务 tx1 两次读取同一个数据应该返回相同结果。")
	}
}

func (this *RepeatableRead) PhantomRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("repeatable_read_phantom_read_1", 1)
	ib.Values("repeatable_read_phantom_read_2", 2)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})

	// 在事务 tx1 中读取数据
	products1, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中写入数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	var ib2 = dbs.NewInsertBuilder()
	ib2.Table(kTblProducts)
	ib2.Columns("name", "price")
	ib2.Values("repeatable_read_phantom_read_3", 3)
	if _, err := ib2.Exec(tx2); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	products2, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	var bs1, _ = json.Marshal(products1)
	var bs2, _ = json.Marshal(products2)
	if bytes.Compare(bs1, bs2) != 0 {
		log.Println("隔离级别【可重复读(RepeatableRead)】不应该出现【幻读(PhantomRead)】，即事务 tx1 两次用同一条件读取数据应该返回相同结果。")
	}
}

// Serializable 可序列化隔离级别
type Serializable struct {
	db *dbs.DB
}

func NewSerializable(db *dbs.DB) *Serializable {
	var rc = &Serializable{}
	rc.db = db

	// 清理所有的数据
	var rb = dbs.NewDeleteBuilder()
	rb.Table(kTblProducts)
	rb.Where("1 = 1")
	rb.Exec(db)
	return rc
}

func (this *Serializable) DirtyRead() {
	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.SET("name", "serializable_read_dirty_read")
	ib.SET("price", 1)
	if _, err := ib.Exec(tx1); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		return
	}

	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	var product, err = getProduct(tx2, "serializable_read_dirty_read")
	if err != nil && !errors.Is(err, dbs.ErrNoRows) {
		log.Println("读取数据发生错误:", err)
		tx2.Rollback()
		return
	}

	if product != nil {
		log.Println("隔离级别【可序列化(Serializable)】不应该出现【脏读(DirtyRead)】，即事务 tx2 中不应该读取到未提交事务 tx1 写入的数据。")
	}

	tx1.Rollback()
	tx2.Commit()
}

func (this *Serializable) NonRepeatableRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("serializable_non_repeatable_read", 1)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})

	// 在事务 tx1 中读取数据
	product1, err := getProduct(tx1, "serializable_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中修改数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	var ub = dbs.NewUpdateBuilder()
	ub.Table(kTblProducts)
	ub.SET("price", dbs.SQL("price + 1"))
	ub.Where("name = ?", "serializable_non_repeatable_read")
	if _, err = ub.Exec(tx2); err != nil {
		log.Println("更新数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	product2, err := getProduct(tx1, "serializable_non_repeatable_read")
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	if product1 == nil || product2 == nil {
		log.Println("读取数据异常")
		return
	}
	if product1.Price != product2.Price {
		log.Println("隔离级别【可序列化(Serializable)】不应该出现【不可重复读(NonRepeatableRead)】，即事务 tx1 两次读取同一个数据应该返回相同结果。")
	}
}

func (this *Serializable) PhantomRead() {
	// 初始化数据
	var ib = dbs.NewInsertBuilder()
	ib.Table(kTblProducts)
	ib.Columns("name", "price")
	ib.Values("serializable_phantom_read_1", 1)
	ib.Values("serializable_phantom_read_2", 2)
	if _, err := ib.Exec(this.db); err != nil {
		log.Println("初始化写入数据发生错误:", err)
		return
	}

	var tx1, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})

	// 在事务 tx1 中读取数据
	products1, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}

	// 在事务 tx2 中写入数据
	var tx2, _ = this.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	var ib2 = dbs.NewInsertBuilder()
	ib2.Table(kTblProducts)
	ib2.Columns("name", "price")
	ib2.Values("serializable_phantom_read_3", 3)
	if _, err := ib2.Exec(tx2); err != nil {
		log.Println("写入数据发生错误:", err)
		tx1.Rollback()
		tx2.Rollback()
		return
	}
	tx2.Commit()

	// 在事务 tx1 中再次读取数据
	products2, err := getProducts(tx1)
	if err != nil {
		log.Println("读取数据发生错误:", err)
		tx1.Rollback()
		return
	}
	tx1.Commit()

	var bs1, _ = json.Marshal(products1)
	var bs2, _ = json.Marshal(products2)
	if bytes.Compare(bs1, bs2) != 0 {
		log.Println("隔离级别【可序列化(Serializable)】不应该出现【幻读(PhantomRead)】，即事务 tx1 两次用同一条件读取数据应该返回相同结果。")
	}
}
