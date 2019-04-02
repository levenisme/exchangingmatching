package main

import(
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	//"log"
	"time"
	_ "github.com/lib/pq"
)

var lock sync.Mutex

const(
	host 	 = "localhost"
	port 	 = 5432
	user 	 = "postgres"
	password = "12345"
	dbname	 = "exchanging_matching"
)

const(
	ERROR  		= 0
	UPDATE 		= 1
	INSERT 		= 2
	DECIMAL_LEN = 32
	INSERT_TRANS_LEVEL = "REPEATABLE READ"
	READ_TRANS_LEVEL = "SERIALIZABLE"
	CANCELLED = 1
	EXECUTING = 2
	NOT_EXIST = -1
)

//get transaction_id(order_id), num, price from order_info
func get_compare_info(db *sql.DB, sym string, amount string, limit string, is_buy bool){
	query_buy := fmt.Sprintf("select order_id, open, limit_price from order_info where ((type == 2 ) and (limit_price <= %s) and (open < 0 )) order by limit ASC, order_id ASC;", limit)

	query_sell := fmt.Sprintf("select order_id, open, limit_price from order_info where ((type == 2 ) and (limit_price >= %s) and (open > 0 )) order by limit DESC, order_id ASC;",  limit)

	if is_buy {
		fmt.Println(query_buy)
		db.Exec(query_buy)
	} else {
		fmt.Println(query_sell)
		db.Exec(query_sell)
	}
	
}

//update number in account_to_sym
func update_num_in_account_sym(db *sql.DB, num string, account_id string, symbol_id string) {
	update := "update account_to_symbol set number = "
	update = update + num + " where (account_id = '"
	update = update + account_id + "') and (symbol_id ='"
	update = update + symbol_id + "');"
	fmt.Println(update)
	db.Exec(update)
}


//update balance in account_info
func update_balance(db *sql.DB, balance string, account_id string) {
	update := "update account_info set balance = "
	update = update + balance + " where (account_id = '"
	update = update + account_id + "');"
	fmt.Println(update)
	db.Exec(update)
}

//update open in order_info
func update_open(db *sql.DB, open string, order_id string) {
	update := "update order_info set open = "
	update = update + open + " where (order_id = "
	update = update + order_id + ");"
	fmt.Println(update)
	db.Exec(update)
}

//when cancel, update type and time in order_info
func update_type_and_time(db *sql.DB, order_id string) {
	time_now := time.Now().Unix()
	update := "update order_info set type = "
	update = update + strconv.FormatInt(CANCELLED, 10) + ", time = " + strconv.FormatInt(time_now, 10) + " where order_id = " + order_id +"; "
	fmt.Println(update)
	db.Exec(update)
}


//get the num of the sym that an acount have
//if there is no num, it will return "" 
func get_position(db *sql.DB, account_id string, sym string) (string) {
	pos := "select number from account_to_symbol where (account_id = '"
	pos += account_id
	pos += "') and (symbol_id = '"
	pos += sym
	pos += "') ;"
	fmt.Println(pos)
	row:= db.QueryRow(pos)
	var num string
	row.Scan(&num)
	fmt.Println(num)
	return num
}

//get balance of an account 
func get_balance(db *sql.DB, account_id string)(string){
	query := "select balance from account_info where account_id = '"
	query += account_id
	query += "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var balance string
	row.Scan(&balance)
	fmt.Println(balance)
	return balance
}

//get open shares or cancel time from order_info table, differentiated by input
func get_open_or_caceltime(db *sql.DB, order_id string, check string) int64 {
	query := "select " + check + " from order_info where order_id = '"
	query += order_id 
	query += "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var req int64
	row.Scan(&req)
	fmt.Println(req)
	return req
}

//get type of the order, to check if it is cancelled
func get_type(db *sql.DB, order_id string) int {
	query := "select type from order_info where order_id = '"
	query = query + order_id + "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var order_type int
	row.Scan(&order_type)
	fmt.Println("type")
	fmt.Println(order_type)
	if(order_type == CANCELLED){
		return CANCELLED
	}
	if(order_type == EXECUTING){
		return EXECUTING
	}
	return NOT_EXIST
}


//if query, return xml of status
func get_status_xml(db *sql.DB, order_id string) string{
	status := "<status id = "
	status = status + order_id + ">\n"

	order_type := get_type(db, order_id)
	if(order_type == NOT_EXIST){
		status = status + "<error trans_id = " + order_id + " not exists>\n "
		return status
	}
	open_shares := get_open_or_caceltime(db, order_id, "open")
	//check the type of the order to get correct response
	if (order_type == CANCELLED) {
		status = status + "<canceled shares = "	+ strconv.FormatInt(open_shares, 10)
		canceltime := get_open_or_caceltime(db, order_id, "time")
		status = status + " time = " + strconv.FormatInt(canceltime,10) + ">\n"
	}
	if(order_type == EXECUTING){
		status = status + "<open shares = " + strconv.FormatInt(open_shares,10) + ">\n"
	}

	//response with the executed shares 
	query := "select shares, price, activity_info.time from activity_info,order_info where (order_info.order_id = activity_info.order_id) and  (order_info.order_id = '"
	query += order_id
	query += "');"
	fmt.Println(query)
	rows,err := db.Query(query)
	CheckErr(err)
	for rows.Next(){
		status += "<executed shares = "
		var exe_shares int64
		var exe_price float64
		var exe_time int64 
		rows.Columns()
		err = rows.Scan(&exe_shares, &exe_price, &exe_time)
		CheckErr(err)
		status = status + strconv.FormatInt(exe_shares,10) + " price = " + fmt.Sprintf("%f", exe_price) + " time = " + strconv.FormatInt(exe_time,10) + ">\n"
	}
	defer rows.Close()
	return status
}

func Create_table(db *sql.DB) {
	

	//drop table if exists
	_,err := db.Exec("DROP TABLE IF EXISTS order_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS activity_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS account_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS symbol_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS account_to_symbol cascade;")
	if err != nil{
		panic(err)
	}

	//create table activity_info
	activity := ""
	activity += "CREATE TABLE activity_info("
	activity += "activity_id serial primary key,"
	activity += "shares int,"
	activity += "order_id int,"
	activity += "price DECIMAL(32,2),"
	activity += "time bigint);"
	fmt.Println(activity)
	_,err = db.Exec(activity)

	//create table order_info 
	order := ""
	order += "CREATE TABLE order_info("
	order += "order_id serial primary key,"
	order += "account_id int,"
	order += "open DECIMAL(32,2),"
	order += "type int,"
	order += "amount DECIMAL(32,2),"
	order += "limit_price DECIMAL(32,2),"
	order += "time bigint);"
	fmt.Println(order)
	_,err = db.Exec(order)
	
	//create table account_info 
	account := ""
	account += "CREATE TABLE account_info("
	account += "account_id text primary key,"
	account += "balance DECIMAL(32,2));"
	_,err = db.Exec(account)

	//create table symbol_info 
	symbol := ""
	symbol += "CREATE TABLE symbol_info("
	symbol += "symbol_id text primary key);"
	_,err = db.Exec(symbol)

	//create foreign key between order and account
	order_account := ""
	order_account += "ALTER TABLE order_info ADD CONSTRAINT constraint_fk FOREIGN KEY(account_id) REFERENCES account_info(accoUnt_id) ON DELETE CASCADE;"
	_,err = db.Exec(order_account);

	//create foreign key between activity and order
	activity_order := ""
	activity_order += "ALTER TABLE activity_info ADD CONSTRAINT constraint_fk FOREIGN KEY(order_id) REFERENCES order_info(order_id) ON DELETE CASCADE;"
	_,err = db.Exec(activity_order);

	//build many-to-many relationship between account and symbol
	account_symbol := ""
	account_symbol += "CREATE TABLE account_to_symbol("
	//account_symbol += "account_symbol_id int primary key,"
	account_symbol += "account_symbol_id serial primary key,"
	account_symbol += "account_id text references account_info(account_id), "
	account_symbol += "symbol_id text references symbol_info(symbol_id),"
	account_symbol += "number DECIMAL(32,2));"
	_,err = db.Exec(account_symbol);
	if err != nil{
		panic(err)
	}
}

func CheckCount(rows *sql.Rows) (count int) {
 	for rows.Next() {
    	err:= rows.Scan(&count)
    	CheckErr(err)
    }   
    return count
}

func CheckErr(err error) {
    if err != nil {
        panic(err)
    }
}

//check if symbol is in symbol table, if not, could insert
func Verify_symbol(db *sql.DB, symbol_id string) (int,error){
	check_s := ""
	check_s +="select count(symbol_id) from symbol_info where symbol_id='"
	check_s += symbol_id
	check_s += "'"
	fmt.Println(check_s)

	rows,err := db.Query(check_s)
	count := CheckCount(rows)
	if(count!=0){
		return ERROR,err
	}

	defer rows.Close()
	return INSERT,err
}

//check if account is in account table, if not, could insert
func Verify_account(db *sql.DB, account_id string) (int, error) {
	check_a := ""
	check_a +="select count(account_id) from account_info where account_id='"
	check_a +=account_id
	check_a +="'; "
	fmt.Println(check_a)

	rows,err := db.Query(check_a)
	count := CheckCount(rows)
	if(count!=0){
		return ERROR,err
	}
	defer rows.Close()
	return INSERT,err

}

//check id account have a specific symbol, id account not exist, return err, if exist but have
//no sym, return insert, it both exists, return update
func Verify_symbol_account(db *sql.DB, symbol_id string, account_id string, num string) (int, error) {
	//check if account_id exists,if not, return 0, error
	check_a := ""
	check_a +="select count(account_id) from account_info where account_id='"
	check_a +=account_id
	check_a +="'; "
	fmt.Println(check_a)

	rows,err := db.Query(check_a)

	count := CheckCount(rows)
	if(count == 0){
		return ERROR,err
	}

	//account_id exists, check if it is in symbol_account, if exists, update and return 1, if not, return 2
	check_sym := ""
	check_sym +="select count(account_id) from account_to_symbol where account_id='"
	check_sym +=account_id
	check_sym +="'; "

	rows_sym, err := db.Query(check_sym)

	count = CheckCount(rows_sym)
	if(count!=0){
		return UPDATE,err
	}
	defer rows.Close()
	defer rows_sym.Close()
	return INSERT,err
}


func Insert_accout_info(db *sql.DB, account_id string, balance string) error{
	insert := ""
	insert += "insert into account_info values ('"
	insert += account_id
	insert += "', '"
	insert += balance
	insert += "'); " 
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err

}

func Insert_account_to_symbol(db *sql.DB, sym string, account_id string, num string) error{
	insert := ""
	insert += "insert into account_to_symbol(symbol_id,account_id,number) values ('"
	insert += sym
	insert += "', '"
	insert += account_id
	insert += "', '"
	insert += num
	insert += "');"
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err
}

func Insert_activity_info(db *sql.DB, order_id string, price string, shares string) {
	insert := "insert into activity_info(order_id,shares,price,time) values ("
	time_now := time.Now().Unix()
	insert = insert + order_id + ", " + shares + ", " + price + ", " + strconv.FormatInt(time_now,10) + "); "
	fmt.Println(insert)
	db.Exec(insert)
}

func Insert_order_info(db *sql.DB, account_id string, sym string, amount string, limit string) int64 {
	query := "insert into order_info(account_id, open, type, amount) values ("
	query = query + account_id + ", " + amount + ", " + limit + ", " +amount +"); "
	rs,err := db.Exec(query)
	CheckErr(err)
	fmt.Println(query)
	id, err := rs.LastInsertId()
	return id+1
}

func Insert_symbol_info(db *sql.DB,sym string) error{
	insert := ""
	insert += "insert into symbol_info values ('"
	insert += sym
	insert +="');"
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err
}

func Connect_database() (db *sql.DB, errstr error){
	psqlInfo :=  fmt.Sprintf("host=%s port=%d user=%s "+
		"password = %s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)

	if err != nil{
		fmt.Println("\nconnection mysql error")
		return db, err
		panic(err)
	}
	//defer db.Close()
	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	err = db.Ping()

	if err != nil{
		fmt.Println("\nopen mysql error ", err)
		return db, err
	}	
	

	return db,nil
}

func main() {
	//connect to the database
	
	db,_ := Connect_database()
	fmt.Println("successfully connected!")
	Create_table(db)

	p,_ := Verify_account(db,"12345")
	fmt.Println(p)
	p,_ = Verify_account(db,"123")
	fmt.Println(p)
	p,_ = Verify_symbol(db,"abcd")
	fmt.Println(p)
	p,_ = Verify_symbol_account(db,"abcd","123","1234.0")
	fmt.Println(p)
	Insert_accout_info(db,"12345","1000")
	Insert_accout_info(db,"123","321")
	Insert_symbol_info(db,"abcd")
	Insert_account_to_symbol(db, "abcd", "123", "1234.0")
	p,_ = Verify_account(db,"12345")
	fmt.Println(p)
	p,_ = Verify_account(db,"123")
	fmt.Println(p)
	p,_ = Verify_symbol(db,"abcd")
	fmt.Println(p)
	p,_ = Verify_symbol_account(db,"abcd","123","1234.0")
	fmt.Println(p)
	num:= get_position(db, "123","abcd" )
	fmt.Println(num)
	num= get_position(db, "12345","abcd" )
	fmt.Println(num)
	fmt.Println("return id")
	fmt.Println(Insert_order_info(db, "12345", "abcd", "100000", "2"))
	fmt.Println(get_status_xml(db, "2"))
	fmt.Println(get_status_xml(db, "1"))
	Insert_activity_info(db, "1", "100", "100")
	update_num_in_account_sym(db, "99999", "123", "abcd")
	update_balance(db, "9999999", "12345")
	update_open(db, "99999", "1")
	update_type_and_time(db, "1")
	get_compare_info(db, "abcd", "100", "11111", true)
}
